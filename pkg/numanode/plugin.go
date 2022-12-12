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

type NUMANodes struct {
	nodes   noderesourcetopology.NUMANodeList
	Mapping NUMAMapping
}

type NUMAMapping map[int]int

func (n NUMANodes) Len() int {
	return len(n.nodes)
}

func (n NUMANodes) Nodes() noderesourcetopology.NUMANodeList {
	return n.nodes
}

func (n NUMANodes) CombineResources(nodesIDxs []int) v1.ResourceList {
	resources := v1.ResourceList{}
	for _, nodeIndex := range nodesIDxs {
		for resource, quantity := range n.nodes[nodeIndex].Resources {
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

func (n NUMANodes) CombinationsAndMinDistance(k int) ([][]int, float32) {
	combinations := combin.Combinations(len(n.nodes), k)

	// max distance for NUMA node
	var minDistance float32 = 256

	for _, combination := range combinations {
		minDistance = n.minDistance(combination, minDistance)
	}

	return combinations, minDistance
}

func (n NUMANodes) distance(combination []int) float32 {
	var accu int
	for _, node := range combination {
		accu += n.nodes[node].Costs[n.Mapping[node]]
	}
	avgDistance := float32(accu) / float32(len(combination))

	return avgDistance
}

func (n NUMANodes) minDistance(combination []int, currentMin float32) float32 {
	avgDistance := n.distance(combination)

	if avgDistance < currentMin {
		return avgDistance
	}

	return currentMin
}

func (n NUMANodes) ResourcesForNodeID(id int) v1.ResourceList {
	return n.nodes[id].Resources
}

func CreateNUMANodes(zones topologyv1alpha1.ZoneList) NUMANodes {
	nodes := make(noderesourcetopology.NUMANodeList, 0)
	idxToID := make(NUMAMapping, len(zones))
	for i, zone := range zones {
		if zone.Type == "Node" {
			var numaID int
			_, err := fmt.Sscanf(zone.Name, "node-%d", &numaID)
			if err != nil {
				klog.ErrorS(nil, "Invalid zone format", "zone", zone.Name)
				continue
			}
			if numaID > 63 || numaID < 0 {
				klog.ErrorS(nil, "Invalid NUMA id range", "numaID", numaID)
				continue
			}
			idxToID[i] = numaID
			resources := extractResources(zone)
			klog.V(6).InfoS("extracted NUMA resources", stringify.ResourceListToLoggable(zone.Name, resources)...)
			costs := make(map[int]int, len(zone.Costs))
			for _, cost := range zone.Costs {
				_, err := fmt.Sscanf(cost.Name, "node-%d", &numaID)
				if err != nil {
					klog.ErrorS(nil, "Invalid zone format", "zone", zone.Name)
					continue
				}
				costs[numaID] = int(cost.Value)
			}
			nodes = append(nodes, noderesourcetopology.NUMANode{NUMAID: numaID, Resources: resources, Costs: costs})

		}
	}
	return NUMANodes{
		Mapping: idxToID,
		nodes:   nodes,
	}
}

func extractResources(zone topologyv1alpha1.Zone) v1.ResourceList {
	res := make(v1.ResourceList)
	for _, resInfo := range zone.Resources {
		res[v1.ResourceName(resInfo.Name)] = resInfo.Available
	}
	return res
}

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
	nodes := CreateNUMANodes(zones)
	qos := v1qos.GetPodQOS(pod)

	maxNuma := -1
	var (
		numaNodes       bitmask.BitMask
		res             int
		optimalDistance bool
	)
	// the order how TopologyManager asks for hint is important so doing it in the same order
	// https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/cm/topologymanager/scope_container.go#L52
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		logKey := fmt.Sprintf("%s/%s/%s/%s", nodeName, pod.Namespace, pod.Name, container.Name)
		klog.V(6).InfoS("target resources", stringify.ResourceListToLoggable(logKey, container.Resources.Requests)...)

		numaNodes, res, optimalDistance = numaNodesRequired(logKey, qos, nodes, container.Resources.Requests)
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
	score := normalizeScore(maxNuma, optimalDistance)

	klog.V(5).InfoS("final verdict", "logKey", logKey, "node", nodeName, "required nodes", maxNuma, "score", score)

	return score, nil
}

func normalizeScore(numaNodes int, optimalDistance bool) int64 {
	numaScore := framework.MaxNodeScore / highestNUMAID

	// if not optimal NUMA distance substract half of numaScore
	var modifier int64 = 0
	if !optimalDistance {
		modifier -= numaScore / 2
	}
	return framework.MaxNodeScore - int64(numaNodes)*numaScore - modifier
}

func subtractFromNUMA(numaNodes NUMANodes, nodes []int, resources v1.ResourceList) {
	for resName, quantity := range resources {
		for _, node := range nodes {
			// quantity is zero no need to iterate through another NUMA node, go to another resource
			if quantity.IsZero() {
				break
			}

			nRes := numaNodes.ResourcesForNodeID(node)
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
	nodes := CreateNUMANodes(zones)
	qos := v1qos.GetPodQOS(pod)
	logKey := fmt.Sprintf("%s/%s/%s", nodeName, pod.Namespace, pod.Name)

	resources := util.GetPodEffectiveRequest(pod)

	klog.V(6).InfoS("target resources", stringify.ResourceListToLoggable(logKey, resources)...)

	_, res, optimalDistance := numaNodesRequired(logKey, qos, nodes, resources)

	score := normalizeScore(res, optimalDistance)
	klog.V(5).InfoS("final verdict", "logKey", logKey, "required nodes", res, "score", score)

	return score, nil
}

func numaNodesRequired(logKey string, qos v1.PodQOSClass, numaNodes NUMANodes, resources v1.ResourceList) (bitmask.BitMask, int, bool) {
	combinationBitmask := bitmask.NewEmptyBitMask()
	if numaNodes.Len() == 1 {
		combinationBitmask.Add(0)
		return combinationBitmask, 1, true
	}
	// we will generate combination of numa nodes from len = 1 to the number of numa nodes present on the machine
	for i := 1; i <= numaNodes.Len(); i++ {
		// generate combinations of len i
		// iterate over combinations for given i
		numaNodesCombination, globalMinDistance := numaNodes.CombinationsAndMinDistance(i)
		for _, combination := range numaNodesCombination {
			distance := numaNodes.minDistance(combination, globalMinDistance)
			// accumulate resources for given combination
			combinationResources := numaNodes.CombineResources(combination)

			resourcesFit := true
			for resource, quantity := range resources {
				if quantity.IsZero() {
					// why bother? everything's fine from the perspective of this resource
					klog.V(4).InfoS("ignoring zero-qty resource request", "logKey", logKey, "resource", resource)
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
				return combinationBitmask, i, distance == globalMinDistance
			}
		}
	}

	// score plugin should be running after resource filter plugin so we should always find sufficient amount of NUMA nodes
	klog.V(1).InfoS("Shouldn't be here", "logKey", logKey)
	return combinationBitmask, -1, false
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
