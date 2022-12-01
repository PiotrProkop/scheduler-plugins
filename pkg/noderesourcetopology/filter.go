/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package noderesourcetopology

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	bm "k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	topologyv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha1"

	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/stringify"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

// The maximum number of NUMA nodes that Topology Manager allows is 8
// https://kubernetes.io/docs/tasks/administer-cluster/topology-manager/#known-limitations
const highestNUMAID = 8

type PolicyHandler func(pod *v1.Pod, zoneMap topologyv1alpha1.ZoneList) *framework.Status

func singleNUMAContainerLevelHandler(pod *v1.Pod, zones topologyv1alpha1.ZoneList, nodeInfo *framework.NodeInfo) *framework.Status {
	klog.V(5).InfoS("Single NUMA node handler")

	// prepare NUMANodes list from zoneMap
	nodes := CreateNUMANodeList(zones)
	qos := v1qos.GetPodQOS(pod)

	// Node() != nil already verified in Filter(), which is the only public entry point
	logNumaNodes("container handler NUMA resources", nodeInfo.Node().Name, nodes)

	// the init containers are running SERIALLY and BEFORE the normal containers.
	// https://kubernetes.io/docs/concepts/workloads/pods/init-containers/#understanding-init-containers
	// therefore, we don't need to accumulate their resources together
	for _, initContainer := range pod.Spec.InitContainers {
		logKey := fmt.Sprintf("%s/%s/%s", pod.Namespace, pod.Name, initContainer.Name)
		klog.V(6).InfoS("target resources", stringify.ResourceListToLoggable(logKey, initContainer.Resources.Requests)...)

		_, match := resourcesAvailableInAnyNUMANodes(logKey, nodes, initContainer.Resources.Requests, qos, nodeInfo)
		if !match {
			// we can't align init container, so definitely we can't align a pod
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("cannot align init container: %s", initContainer.Name))
		}
	}

	for _, container := range pod.Spec.Containers {
		resources := fmt.Sprintf("%s/%s/%s", pod.Namespace, pod.Name, container.Name)
		klog.V(6).InfoS("target resources", stringify.ResourceListToLoggable(resources, container.Resources.Requests)...)

		numaID, match := resourcesAvailableInAnyNUMANodes(resources, nodes, container.Resources.Requests, qos, nodeInfo)
		if !match {
			// we can't align container, so definitely we can't align a pod
			return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("cannot align container: %s", container.Name))
		}

		// subtract the resources requested by the container from the given NUMA.
		// this is necessary, so we won't allocate the same resources for the upcoming containers
		subtractFromNUMA(nodes, numaID, container)
	}
	return nil
}

// resourcesAvailableInAnyNUMANodes checks for sufficient resource and return the NUMAID that would be selected by Kubelet.
// this function requires NUMANodeList with properly populated NUMANode, NUMAID should be in range 0-63
func resourcesAvailableInAnyNUMANodes(logKey string, numaNodes NUMANodeList, resources v1.ResourceList, qos v1.PodQOSClass, nodeInfo *framework.NodeInfo) (int, bool) {
	numaID := highestNUMAID
	bitmask := bm.NewEmptyBitMask()
	// set all bits, each bit is a NUMA node, if resources couldn't be aligned
	// on the NUMA node, bit should be unset
	bitmask.Fill()

	// Node() != nil already verified in Filter(), which is the only public entry point
	nodeName := nodeInfo.Node().Name
	nodeResources := util.ResourceList(nodeInfo.Allocatable)

	for resource, quantity := range resources {
		if quantity.IsZero() {
			// why bother? everything's fine from the perspective of this resource
			klog.V(4).InfoS("ignoring zero-qty resource request", "logKey", logKey, "node", nodeName, "resource", resource)
			continue
		}

		if _, ok := nodeResources[resource]; !ok {
			// some resources may not expose NUMA affinity (device plugins, extended resources), but all resources
			// must be reported at node level; thus, if they are not present at node level, we can safely assume
			// we don't have the resource at all.
			klog.V(5).InfoS("early verdict: cannot meet request", "logKey", logKey, "node", nodeName, "resource", resource, "suitable", "false")
			return numaID, false
		}

		// for each requested resource, calculate which NUMA slots are good fits, and then AND with the aggregated bitmask, IOW unset appropriate bit if we can't align resources, or set it
		// obvious, bits which are not in the NUMA id's range would be unset
		hasNUMAAffinity := false
		resourceBitmask := bm.NewEmptyBitMask()
		for _, numaNode := range numaNodes {
			numaQuantity, ok := numaNode.Resources[resource]
			if !ok {
				continue
			}

			hasNUMAAffinity = true
			if !isNUMANodeSuitable(qos, resource, quantity, numaQuantity) {
				continue
			}

			resourceBitmask.Add(numaNode.NUMAID)
			klog.V(6).InfoS("feasible", "logKey", logKey, "node", nodeName, "NUMA", numaNode.NUMAID, "resource", resource)
		}

		if !hasNUMAAffinity && !v1helper.IsNativeResource(resource) {
			klog.V(6).InfoS("resource available at node level (no NUMA affinity)", "logKey", logKey, "node", nodeName, "resource", resource)
			continue
		}

		bitmask.And(resourceBitmask)
		if bitmask.IsEmpty() {
			klog.V(5).InfoS("early verdict", "logKey", logKey, "node", nodeName, "resource", resource, "suitable", "false")
			return numaID, false
		}
	}
	// according to TopologyManager, the preferred NUMA affinity, is the narrowest one.
	// https://github.com/kubernetes/kubernetes/blob/v1.24.0-rc.1/pkg/kubelet/cm/topologymanager/policy.go#L155
	// in single-numa-node policy all resources should be allocated from a single NUMA,
	// which means that the lowest NUMA ID (with available resources) is the one to be selected by Kubelet.
	numaID = bitmask.GetBits()[0]

	// at least one NUMA node is available
	ret := !bitmask.IsEmpty()
	klog.V(5).InfoS("final verdict", "logKey", logKey, "node", nodeName, "suitable", ret)
	return numaID, ret
}

func isNUMANodeSuitable(qos v1.PodQOSClass, resource v1.ResourceName, quantity, numaQuantity resource.Quantity) bool {
	// Check for the following:
	if qos != v1.PodQOSGuaranteed {
		// 1. set numa node as possible node if resource is memory or Hugepages
		if resource == v1.ResourceMemory {
			return true
		}
		if v1helper.IsHugePageResourceName(resource) {
			return true
		}
		// 2. set numa node as possible node if resource is CPU
		if resource == v1.ResourceCPU {
			return true
		}
	}
	// 3. otherwise check amount of resources
	return numaQuantity.Cmp(quantity) >= 0
}

func singleNUMAPodLevelHandler(pod *v1.Pod, zones topologyv1alpha1.ZoneList, nodeInfo *framework.NodeInfo) *framework.Status {
	klog.V(5).InfoS("Pod Level Resource handler")

	resources := util.GetPodEffectiveRequest(pod)

	logKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	nodes := CreateNUMANodeList(zones)

	// Node() != nil already verified in Filter(), which is the only public entry point
	logNumaNodes("pod handler NUMA resources", nodeInfo.Node().Name, nodes)
	klog.V(6).InfoS("target resources", stringify.ResourceListToLoggable(logKey, resources)...)

	if _, match := resourcesAvailableInAnyNUMANodes(logKey, CreateNUMANodeList(zones), resources, v1qos.GetPodQOS(pod), nodeInfo); !match {
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("cannot align pod: %s", pod.Name))
	}
	return nil
}

// Filter Now only single-numa-node supported
func (tm *TopologyMatch) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}
	if v1qos.GetPodQOS(pod) == v1.PodQOSBestEffort && !HasNonNativeResource(pod) {
		return nil
	}

	nodeName := nodeInfo.Node().Name
	nodeTopology := tm.nrtCache.GetByNode(nodeName, pod)

	if nodeTopology == nil {
		return nil
	}

	klog.V(5).InfoS("Found NodeResourceTopology", "nodeTopology", klog.KObj(nodeTopology))
	for _, policyName := range nodeTopology.TopologyPolicies {
		if handler, ok := tm.policyHandlers[topologyv1alpha1.TopologyManagerPolicy(policyName)]; ok {
			// skip if policyHandler found but not filter plugin for this node
			if handler.filter == nil {
				klog.V(5).InfoS("Filter plugin not found", "policy", policyName)
				break
			}
			if status := handler.filter(pod, nodeTopology.Zones, nodeInfo); status != nil {
				tm.nrtCache.MarkNodeDiscarded(nodeName, pod)
				return status
			}
		} else {
			klog.V(5).InfoS("Policy handler not found", "policy", policyName)
		}
	}
	return nil
}

func HasNonNativeResource(pod *v1.Pod) bool {
	for _, initContainer := range pod.Spec.InitContainers {
		for resource := range initContainer.Resources.Requests {
			if !v1helper.IsNativeResource(resource) {
				return true
			}
		}
	}
	for _, container := range pod.Spec.Containers {
		for resource := range container.Resources.Requests {
			if !v1helper.IsNativeResource(resource) {
				return true
			}
		}
	}
	return false
}
