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
	"reflect"
	"testing"

	"github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/listers/topology/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology"

	topologyv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha1"
	faketopologyv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/clientset/versioned/fake"
	topologyinformers "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/informers/externalversions"
	nrtcache "sigs.k8s.io/scheduler-plugins/pkg/noderesourcetopology/cache"
)

type nodeToScoreMap map[string]int64

const (
	cpu    = string(v1.ResourceCPU)
	memory = string(v1.ResourceMemory)
)

func TestNUMAScorePlugin(t *testing.T) {
	nodeTopologies := make([]*topologyv1alpha1.NodeResourceTopology, 3)
	nodesMap := make(map[string]*v1.Node)
	var lister v1alpha1.NodeResourceTopologyLister

	initTest := func() {
		// noderesourcetopology objects
		nodeTopologies[0] = &topologyv1alpha1.NodeResourceTopology{
			ObjectMeta:       metav1.ObjectMeta{Name: "Node1"},
			TopologyPolicies: []string{string(topologyv1alpha1.SingleNUMANodeContainerLevel)},
			Zones: topologyv1alpha1.ZoneList{
				topologyv1alpha1.Zone{
					Name: "node-0",
					Type: "Node",
					Resources: topologyv1alpha1.ResourceInfoList{
						noderesourcetopology.MakeTopologyResInfo(cpu, "4", "4"),
						noderesourcetopology.MakeTopologyResInfo(memory, "500Mi", "500Mi"),
					},
				},
				topologyv1alpha1.Zone{
					Name: "node-1",
					Type: "Node",
					Resources: topologyv1alpha1.ResourceInfoList{
						noderesourcetopology.MakeTopologyResInfo(cpu, "4", "4"),
						noderesourcetopology.MakeTopologyResInfo(memory, "500Mi", "500Mi"),
					},
				},
			},
		}

		nodeTopologies[1] = &topologyv1alpha1.NodeResourceTopology{
			ObjectMeta:       metav1.ObjectMeta{Name: "Node2"},
			TopologyPolicies: []string{string(topologyv1alpha1.SingleNUMANodeContainerLevel)},
			Zones: topologyv1alpha1.ZoneList{
				topologyv1alpha1.Zone{
					Name: "node-0",
					Type: "Node",
					Resources: topologyv1alpha1.ResourceInfoList{
						noderesourcetopology.MakeTopologyResInfo(cpu, "2", "2"),
						noderesourcetopology.MakeTopologyResInfo(memory, "50Mi", "50Mi"),
					},
				}, topologyv1alpha1.Zone{
					Name: "node-1",
					Type: "Node",
					Resources: topologyv1alpha1.ResourceInfoList{
						noderesourcetopology.MakeTopologyResInfo(cpu, "2", "2"),
						noderesourcetopology.MakeTopologyResInfo(memory, "50Mi", "50Mi"),
					},
				},
			},
		}
		nodeTopologies[2] = &topologyv1alpha1.NodeResourceTopology{
			ObjectMeta:       metav1.ObjectMeta{Name: "Node3"},
			TopologyPolicies: []string{string(topologyv1alpha1.SingleNUMANodeContainerLevel)},
			Zones: topologyv1alpha1.ZoneList{
				topologyv1alpha1.Zone{
					Name: "node-0",
					Type: "Node",
					Resources: topologyv1alpha1.ResourceInfoList{
						noderesourcetopology.MakeTopologyResInfo(cpu, "6", "6"),
						noderesourcetopology.MakeTopologyResInfo(memory, "60Mi", "60Mi"),
					},
				}, topologyv1alpha1.Zone{
					Name: "node-1",
					Type: "Node",
					Resources: topologyv1alpha1.ResourceInfoList{
						noderesourcetopology.MakeTopologyResInfo(cpu, "6", "6"),
						noderesourcetopology.MakeTopologyResInfo(memory, "60Mi", "60Mi"),
					},
				},
			},
		}
		// init node objects
		for _, nrt := range nodeTopologies {
			res := noderesourcetopology.MakeResourceListFromZones(nrt.Zones)
			nodesMap[nrt.Name] = &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: nrt.Name},
				Status: v1.NodeStatus{
					Capacity:    res,
					Allocatable: res,
				},
			}
		}

		// init topology lister
		fakeClient := faketopologyv1alpha1.NewSimpleClientset()
		fakeInformer := topologyinformers.NewSharedInformerFactory(fakeClient, 0).Topology().V1alpha1().NodeResourceTopologies()
		for _, obj := range nodeTopologies {
			fakeInformer.Informer().GetStore().Add(obj)
		}
		lister = fakeInformer.Lister()
	}

	type testScenario struct {
		name      string
		wantedRes nodeToScoreMap
		pod       *v1.Pod
	}

	tests := []testScenario{
		{
			name:      "1 container in POD",
			wantedRes: nodeToScoreMap{"Node1": 88, "Node2": 88, "Node3": 88},
			pod: makePodByResourceLists(&v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("2Mi"),
			}),
		},
		{
			name:      "2 containers in POD",
			wantedRes: nodeToScoreMap{"Node1": 88, "Node2": 76, "Node3": 88},
			pod: makePodByResourceLists(&v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("60Mi"),
			}, &v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("2Mi"),
			}),
		},
		{
			name:      "2 containers in POD",
			wantedRes: nodeToScoreMap{"Node1": 88, "Node2": 76, "Node3": 88},
			pod: makePodByResourceLists(&v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("60Mi"),
			}, &v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("2Mi"),
			}),
		},
	}

	for _, test := range tests {
		initTest()
		t.Run(test.name, func(t *testing.T) {
			tm := &NUMANode{
				nrtCache: nrtcache.NewPassthrough(lister),
			}

			nodeToScore := make(nodeToScoreMap, len(nodesMap))
			for _, node := range nodesMap {
				score, _ := tm.Score(
					context.Background(),
					framework.NewCycleState(),
					test.pod,
					node.Name,
				)

				nodeToScore[node.Name] = score

			}
			if !reflect.DeepEqual(test.wantedRes, nodeToScore) {
				t.Errorf("Wanted scores to be %v got %v", test.wantedRes, nodeToScore)
			}
		})
	}
}

func makePodByResourceLists(resources ...*v1.ResourceList) *v1.Pod {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{},
		},
	}

	for _, r := range resources {
		pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{
			Resources: v1.ResourceRequirements{
				Requests: *r,
				Limits:   *r,
			},
		})
	}

	return pod
}

func TestSubstractNUMA(t *testing.T) {
	tcases := []struct {
		description string
		numaNodes   NUMANodes
		nodes       []int
		resources   v1.ResourceList
		expected    noderesourcetopology.NUMANodeList
	}{
		{
			description: "simple",
			numaNodes: NUMANodes{
				nodes: noderesourcetopology.NUMANodeList{
					{
						NUMAID: 0,
						Resources: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse("10Gi"),
						},
					},
				},
			},
			resources: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			nodes: []int{0},
			expected: noderesourcetopology.NUMANodeList{
				{
					NUMAID: 0,
					Resources: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewQuantity(6, resource.DecimalSI),
						v1.ResourceMemory: resource.MustParse("8Gi"),
					},
				},
			},
		},
		{
			description: "substract resources from 2 NUMA nodes",
			numaNodes: NUMANodes{
				nodes: noderesourcetopology.NUMANodeList{
					{
						NUMAID: 0,
						Resources: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse("10Gi"),
						},
					},
					{
						NUMAID: 1,
						Resources: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse("10Gi"),
						},
					},
				},
			},
			resources: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(12, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			nodes: []int{0, 1},
			expected: noderesourcetopology.NUMANodeList{
				{
					NUMAID: 0,
					Resources: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewQuantity(0, resource.DecimalSI),
						v1.ResourceMemory: resource.MustParse("8Gi"),
					},
				},
				{
					NUMAID: 0,
					Resources: v1.ResourceList{
						v1.ResourceCPU:    *resource.NewQuantity(4, resource.DecimalSI),
						v1.ResourceMemory: resource.MustParse("10Gi"),
					},
				},
			},
		},
	}

	for _, tcase := range tcases {
		t.Run(tcase.description, func(t *testing.T) {
			subtractFromNUMA(tcase.numaNodes, tcase.nodes, tcase.resources)
			for i, node := range tcase.numaNodes.Nodes() {
				for resName, quantity := range node.Resources {
					if !tcase.expected[i].Resources[resName].Equal(quantity) {
						t.Errorf("Expected %s to equal %v instead of %v", resName, tcase.expected[i].Resources[resName], quantity)
					}
				}
			}
		})
	}
}

func TestNUMANodesRequired(t *testing.T) {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node",
		},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("10Gi"),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("10Gi"),
			},
		},
	}
	testCases := []struct {
		description     string
		numaNodes       NUMANodes
		podResources    v1.ResourceList
		node            *v1.Node
		expectedNuma    int
		expectedBitmask bitmask.BitMask
		optimalDistance bool
	}{
		{
			description: "simple case, fit on 1 NUMA node",
			numaNodes: NUMANodes{
				nodes: noderesourcetopology.NUMANodeList{
					{
						NUMAID: 0,
						Resources: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse("10Gi"),
						},
					},
				},
			},
			podResources: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			node:            node,
			expectedNuma:    1,
			expectedBitmask: NewTestBitmask(0),
			optimalDistance: true,
		},
		{
			description: "simple case, fit on 2 NUMA node",
			numaNodes: NUMANodes{
				nodes: noderesourcetopology.NUMANodeList{
					{
						NUMAID: 0,
						Resources: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse("10Gi"),
						},
					},
					{
						NUMAID: 1,
						Resources: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse("10Gi"),
						},
					},
				},
			},
			podResources: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(12, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			node:            node,
			expectedNuma:    2,
			expectedBitmask: NewTestBitmask(0, 1),
			optimalDistance: true,
		},
		{
			description: "no pod resources",
			numaNodes: NUMANodes{
				nodes: noderesourcetopology.NUMANodeList{
					{
						NUMAID: 0,
						Resources: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse("10Gi"),
						},
					},
					{
						NUMAID: 1,
						Resources: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse("10Gi"),
						},
					},
				},
			},
			podResources:    v1.ResourceList{},
			node:            node,
			expectedNuma:    1,
			expectedBitmask: NewTestBitmask(0),
			optimalDistance: true,
		},
		{
			description: "can't fit",
			numaNodes: NUMANodes{
				nodes: noderesourcetopology.NUMANodeList{
					{
						NUMAID: 0,
						Resources: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse("10Gi"),
						},
					},
					{
						NUMAID: 1,
						Resources: v1.ResourceList{
							v1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
							v1.ResourceMemory: resource.MustParse("10Gi"),
						},
					},
				},
			},
			podResources: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewQuantity(22, resource.DecimalSI),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
			node:            node,
			expectedNuma:    -1,
			expectedBitmask: bitmask.NewEmptyBitMask(),
			optimalDistance: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			bm, result, optimalDistance := numaNodesRequired("test", v1.PodQOSGuaranteed, tc.numaNodes, tc.podResources)
			if result != tc.expectedNuma {
				t.Errorf("wrong result expected: %d got: %d", tc.expectedNuma, result)
			}
			if !bm.IsEqual(tc.expectedBitmask) {
				t.Errorf("wrong bitmask expected: %d got: %d", tc.expectedBitmask, bm)
			}
			if optimalDistance != tc.optimalDistance {
				t.Errorf("expected optimalDistance to equal: %t got: %t", tc.optimalDistance, optimalDistance)
			}
		})
	}
}

func NewTestBitmask(bits ...int) bitmask.BitMask {
	bm, _ := bitmask.NewBitMask(bits...)
	return bm
}

func TestNormalizeScore(t *testing.T) {
	tcases := []struct {
		description     string
		score           int
		expectedScore   int64
		optimalDistance bool
	}{
		{
			description:     "1 numa node",
			score:           1,
			expectedScore:   88,
			optimalDistance: true,
		},
		{
			description:     "2 numa nodes",
			score:           2,
			expectedScore:   76,
			optimalDistance: true,
		},
		{
			description:     "8 numa nodes",
			score:           8,
			expectedScore:   4,
			optimalDistance: true,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.description, func(t *testing.T) {
			normalizedScore := normalizeScore(tc.score, tc.optimalDistance)
			if normalizedScore != tc.expectedScore {
				t.Errorf("Expected normalizedScore to be %d not %d", tc.expectedScore, normalizedScore)
			}
		})
	}
}
