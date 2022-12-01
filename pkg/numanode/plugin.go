/*
Copyright 2022 The Kubernetes Authors.

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

package numanode

import (
	"context"
	"fmt"

	"gonum.org/v1/gonum/stat/combin"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology"
	nrtcache "sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/cache"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/stringify"
	"sigs.k8s.io/scheduler-plugins/pkg/util"

	topologyapi "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology"
	topologyv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha1"
)

// The maximum number of NUMA nodes that Topology Manager allows is 8
// https://kubernetes.io/docs/tasks/administer-cluster/topology-manager/#known-limitations
const highestNUMAID = 8

const (
	// Name is the name of the plugin used in the plugin registry and configurationn.
	Name = "NUMANode"
)

// NUMANode plugin which scores nodes based on how many NUMA nodes are required to run given POD/container
type NUMANode struct {
	nrtCache nrtcache.Cache
}

var _ framework.ScorePlugin = &NUMANode{}

// Name returns name of the plugin. It is used in logs, etc.
func (tm *NUMANode) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.V(5).InfoS("Creating new NUMA score plugin")

	nrtCache, err := noderesourcetopology.InitNodeTopologyInformer(handle)
	if err != nil {
		klog.ErrorS(err, "Cannot create clientset for NodeTopologyResource", "kubeConfig", handle.KubeConfig())
		return nil, err
	}

	numaScheduling := &NUMANode{
		nrtCache: nrtCache,
	}

	return numaScheduling, nil
}

// EventsToRegister returns the possible events that may make a Pod
// failed by this plugin schedulable.
// NOTE: if in-place-update (KEP 1287) gets implemented, then PodUpdate event
// should be registered for this plugin since a Pod update may free up resources
// that make other Pods schedulable.
func (nn *NUMANode) EventsToRegister() []framework.ClusterEvent {
	// To register a custom event, follow the naming convention at:
	// https://git.k8s.io/kubernetes/pkg/scheduler/eventhandlers.go#L403-L410
	nrtGVK := fmt.Sprintf("noderesourcetopologies.v1alpha1.%v", topologyapi.GroupName)
	return []framework.ClusterEvent{
		{Resource: framework.Pod, ActionType: framework.Delete},
		{Resource: framework.Node, ActionType: framework.Add | framework.UpdateNodeAllocatable},
		{Resource: framework.GVK(nrtGVK), ActionType: framework.Add | framework.Update},
	}
}

func (nn *NUMANode) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.V(6).InfoS("scoring node", "nodeName", nodeName)

	if v1qos.GetPodQOS(pod) == v1.PodQOSBestEffort && !noderesourcetopology.HasNonNativeResource(pod) {
		return framework.MaxNodeScore, nil
	}

	nodeTopology := nn.nrtCache.GetByNode(nodeName, pod)

	if nodeTopology == nil {
		klog.V(5).InfoS("noderesourcetopology was not found for node", "node", nodeName)
		return 0, nil
	}

	klog.V(5).Infof("[%s] NUMAScheduling Score start for %s", nodeName, pod.Name)

	switch nodeTopology.TopologyPolicies[0] {
	case string(topologyv1alpha1.SingleNUMANodeContainerLevel):
		return containerLevelHandler(pod, nodeTopology.Zones, nodeName)
	case string(topologyv1alpha1.SingleNUMANodePodLevel):
		return podLevelHandler(pod, nodeTopology.Zones, nodeName)
	case string(topologyv1alpha1.BestEffort):
		return containerLevelHandler(pod, nodeTopology.Zones, nodeName)
	case string(topologyv1alpha1.Restricted):
		return containerLevelHandler(pod, nodeTopology.Zones, nodeName)
	default:
		klog.V(4).InfoS("policy handler not found", "policy", nodeTopology.TopologyPolicies)
	}

	return 0, nil
}

func (nn *NUMANode) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func containerLevelHandler(pod *v1.Pod, zones topologyv1alpha1.ZoneList, nodeName string) (int64, *framework.Status) {
	nodes := noderesourcetopology.CreateNUMANodeList(zones)
	qos := v1qos.GetPodQOS(pod)

	maxNuma := 1
	// the order how TopologyManager asks for hint is important so doing it in the same order
	// https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/cm/topologymanager/scope_container.go#L52
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		logKey := fmt.Sprintf("%s/%s/%s", pod.Namespace, pod.Name, container.Name)
		klog.V(6).InfoS("target resources", stringify.ResourceListToLoggable(logKey, container.Resources.Requests)...)

		numaNodes, res := numaNodesRequired(logKey, qos, nodes, container.Resources.Requests, nodeName)
		if res < 1 {
			return 0, framework.NewStatus(framework.Error, fmt.Sprintf("cannot calculate NUMA score for container: %s", container.Name))
		}

		klog.V(5).InfoS("container verdict", "logKey", logKey, "node", nodeName, "required nodes", res, "bitmask", numaNodes.GetBits())

		// take max NUMA we want to score nodes based on the worst scenario
		if res > maxNuma {
			maxNuma = res
		}

		// subtract the resources requested by the container from the given NUMA.
		// this is necessary, so we won't allocate the same resources for the upcoming containers
		subtractFromNUMA(nodes, numaNodes.GetBits(), container.Resources.Requests)
	}

	logKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	score := normalizeScore(maxNuma)

	klog.V(5).InfoS("final verdict", "logKey", logKey, "node", nodeName, "required nodes", maxNuma, "score", score)

	return score, nil
}

func normalizeScore(numaNodes int) int64 {
	numaScore := framework.MaxNodeScore / highestNUMAID
	return framework.MaxNodeScore - int64(numaNodes)*numaScore
}

func subtractFromNUMA(numaNodes noderesourcetopology.NUMANodeList, nodes []int, resources v1.ResourceList) {
	for resName, quantity := range resources {
		for _, node := range nodes {
			// quantity is zero no need to iterate through another NUMA node, go to another resource
			if quantity.IsZero() {
				break
			}

			nRes := numaNodes[node].Resources
			if available, ok := nRes[resName]; ok {
				switch quantity.Cmp(available) {
				case 0: // the same
					// basically zero container resources
					quantity.Sub(available)
					// zero NUMA quantity
					nRes[resName] = resource.MustParse("0")
				case 1: // container wants more resources than available in this NUMA zone
					// substract NUMA resources from container request, to calculate how much is missing
					quantity.Sub(available)
					// zero NUMA quantity
					nRes[resName] = resource.MustParse("0")
				case -1: // there are more resources available in this NUMA zone than container requests
					// substract container resources from resources available in this NUMA node
					available.Sub(quantity)
					// zero container quantity
					quantity = resource.MustParse("0")
					nRes[resName] = available
				}
			}
		}
	}
}

func podLevelHandler(pod *v1.Pod, zones topologyv1alpha1.ZoneList, nodeName string) (int64, *framework.Status) {
	nodes := noderesourcetopology.CreateNUMANodeList(zones)
	qos := v1qos.GetPodQOS(pod)
	logKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

	resources := util.GetPodEffectiveRequest(pod)

	klog.V(6).InfoS("target resources", stringify.ResourceListToLoggable(logKey, resources)...)

	_, res := numaNodesRequired(logKey, qos, nodes, resources, nodeName)

	score := normalizeScore(res)
	klog.V(5).InfoS("final verdict", "logKey", logKey, "node", nodeName, "required nodes", res, "score", score)

	return score, nil
}

func numaNodesRequired(logKey string, qos v1.PodQOSClass, numaNodes noderesourcetopology.NUMANodeList, resources v1.ResourceList, nodeName string) (bitmask.BitMask, int) {
	combinationBitmask := bitmask.NewEmptyBitMask()
	if len(numaNodes) == 1 {
		combinationBitmask.Add(0)
		return combinationBitmask, 1
	}
	// we will generate combination of numa nodes from len = 1 to the number of numa nodes present on the machine
	for i := 1; i <= len(numaNodes); i++ {
		// generate combinations of len i
		numaNodesCombination := combin.Combinations(len(numaNodes), i)
		// iterate over combinations for given i
		for _, combination := range numaNodesCombination {
			// accumulate resources for given combination
			combinationResources := combineResources(numaNodes, combination)

			resourcesFit := true
			for resource, quantity := range resources {
				if quantity.IsZero() {
					// why bother? everything's fine from the perspective of this resource
					klog.V(4).InfoS("ignoring zero-qty resource request", "logKey", logKey, "node", nodeName, "resource", resource)
					continue
				}

				combinationQuantity, ok := combinationResources[resource]
				if !ok {
					// non NUMA resource continue
					continue
				}

				if !isCombinationSuitable(qos, resource, quantity, combinationQuantity) {
					resourcesFit = false
					break
				}

			}
			// if resources can be fit on given combination, just return the number of numa nodes requires to fit them
			// according to TopologyManager if both masks are the same size pick the one that has less bits set
			// https://github.com/kubernetes/kubernetes/blob/3e26e104bdf9d0dc3c4046d6350b93557c67f3f4/pkg/kubelet/cm/topologymanager/bitmask/bitmask.go#L146
			// combin.Combinations is generating combinations in an order from the smallest to highest value
			if resourcesFit {
				combinationBitmask.Add(combination...)
				return combinationBitmask, i
			}
		}
	}

	// score plugin should be running after resource filter plugin so we should always find sufficient amount of NUMA nodes
	klog.V(1).InfoS("Shouldn't be here", "logKey", logKey, "node", nodeName)
	return combinationBitmask, -1
}

func combineResources(numaNodes noderesourcetopology.NUMANodeList, combination []int) v1.ResourceList {
	resources := v1.ResourceList{}
	for _, nodeIndex := range combination {
		for resource, quantity := range numaNodes[nodeIndex].Resources {
			if value, ok := resources[resource]; ok {
				value.Add(quantity)
				resources[resource] = value
				continue
			}
			resources[resource] = quantity
		}
	}

	return resources
}

func isCombinationSuitable(qos v1.PodQOSClass, resource v1.ResourceName, quantity, numaQuantity resource.Quantity) bool {
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
