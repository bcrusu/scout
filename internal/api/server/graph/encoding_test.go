package graph_test

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"sort"
	"testing"

	graphi "github.com/bcrusu/scout/internal/api/server/graph"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/internal/utils/tests"
	"github.com/bcrusu/scout/pkg/graph"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	tests.NewSuite(t, "Graph test suite")
}

var _ = Describe("Encoding tests", func() {
	vertexId := func(typ uint32, value []byte) *graph.VertexId {
		return &graph.VertexId{Type: typ, Value: []byte(value)}
	}

	edgeId := func(typ uint32, head, tail *graph.VertexId) *graph.EdgeId {
		return &graph.EdgeId{Head: head, Tail: tail, Type: typ}
	}

	alterKey := func(key []byte, index int, value byte) []byte {
		clone := slices.Clone(key)
		clone[index] = value
		return clone
	}

	makeSortedVertexIds := func(vertexTypes ...uint32) []*graph.VertexId {
		var result []*graph.VertexId
		slices.Sort(vertexTypes)

		for _, vertexType := range vertexTypes {
			result = append(result,
				vertexId(vertexType, []byte{0}),
				vertexId(vertexType, []byte{1}),
				vertexId(vertexType, []byte{2}),
				vertexId(vertexType, []byte{3}),
				vertexId(vertexType, []byte{0, 1}),
				vertexId(vertexType, []byte{0, 2}),
				vertexId(vertexType, []byte{1, 0}),
				vertexId(vertexType, []byte{1, 1}),
				vertexId(vertexType, []byte{1, 3}),
				vertexId(vertexType, []byte{2, 0}),
				vertexId(vertexType, []byte{2, 3}),
				vertexId(vertexType, []byte{1, 1, 0}),
				vertexId(vertexType, []byte{1, 1, 1}),
				vertexId(vertexType, []byte{1, 1, 2}),
				vertexId(vertexType, []byte{1, 1, 3}),
				vertexId(vertexType, []byte{1, 2, 0}),
				vertexId(vertexType, []byte{1, 2, 1}),
				vertexId(vertexType, []byte{1, 2, 3}),
				vertexId(vertexType, []byte{2, 0, 0}),
				vertexId(vertexType, []byte{2, 1, 0}),
				vertexId(vertexType, []byte{2, 1, 2}),
				vertexId(vertexType, []byte{3, 0, 1}),
				vertexId(vertexType, []byte{3, 1, 0}),
				vertexId(vertexType, []byte{3, 1, 1}),
			)
		}

		return result
	}

	makeSortedEdgeIds := func(head *graph.VertexId, edgeTypes ...uint32) []*graph.EdgeId {
		var result []*graph.EdgeId
		slices.Sort(edgeTypes)

		for _, edgeType := range edgeTypes {
			for _, tail := range makeSortedVertexIds(1, 3, 7) {
				result = append(result, &graph.EdgeId{
					Head: head,
					Tail: tail,
					Type: edgeType,
				})
			}
		}

		return result
	}

	encodeVertexIds := func(ids ...*graph.VertexId) [][]byte {
		result := make([][]byte, len(ids))
		for i, id := range ids {
			result[i] = graphi.EncodeVertexId(id)
		}
		return result
	}

	encodeEdgeIds := func(ids ...*graph.EdgeId) [][]byte {
		result := make([][]byte, len(ids))
		for i, id := range ids {
			result[i] = graphi.EncodeEdgeId(id)
		}
		return result
	}

	Context("Encode/Decode vertex key", func() {
		It("Should return the original vertex id", func() {
			ids := []*graph.VertexId{
				vertexId(0, []byte{0}),
				vertexId(0, []byte("abc")),
				vertexId(1, []byte("abc")),
				vertexId(1<<31+1, []byte("abc")),
				vertexId(math.MaxUint32, []byte("abcef")),
			}

			for i, id := range ids {
				testCase := fmt.Sprintf("test case %d", i)
				id2, err := graphi.DecodeVertexId(graphi.EncodeVertexId(id))

				Expect(err).To(BeNil(), testCase)
				Expect(id2).To(Equal(id), testCase)
			}
		})

		It("Should return error for invalid keys", func() {
			valid := graphi.EncodeVertexId(vertexId(1, []byte("0123456789")))

			invalid := [][]byte{
				valid[:7],                      // too short
				valid[:8],                      // too short
				valid[1:],                      // trimmed
				valid[:len(valid)-1],           // trimmed
				append(valid, 0),               // key contains extra padding
				alterKey(valid, 4, valid[4]+1), // alter value length encoding
				alterKey(valid, 5, valid[5]+1), // alter value length encoding
				alterKey(valid, 6, valid[6]+1), // alter value length encoding
				alterKey(valid, 7, valid[7]+1), // alter value length encoding
				alterKey(valid, 7, 0),          // set value length to zero
			}

			for i, key := range invalid {
				testCase := fmt.Sprintf("test case %d", i)
				_, err := graphi.DecodeVertexId(key)
				Expect(err).NotTo(BeNil(), testCase)
			}
		})

		It("Should produce sortable vertex keys", func() {
			sorted := makeSortedVertexIds(0, 2, 3, 7)
			sortedKeys := encodeVertexIds(sorted...)

			for range 10 {
				shuffledKeys := utils.ShuffleSlice(slices.Clone(sortedKeys))
				sort.Sort(keysSorter(shuffledKeys))
				Expect(shuffledKeys).To(Equal(sortedKeys))
			}
		})
	})

	Context("Encode/Decode edge key", func() {
		It("Should return the original edge id", func() {
			v0 := vertexId(0, []byte{0})
			v1 := vertexId(1, []byte("abc"))
			v2 := vertexId(math.MaxUint32, []byte("123xyz32100000000"))

			ids := []*graph.EdgeId{
				edgeId(0, v0, v0),
				edgeId(0, v1, v0),
				edgeId(0, v1, v1),
				edgeId(0, v1, v2),
				edgeId(1, v1, v2),
				edgeId(math.MaxUint32, v1, v2),
			}

			for i, id := range ids {
				testCase := fmt.Sprintf("test case %d", i)
				id2, err := graphi.DecodeEdgeId(graphi.EncodeEdgeId(id))

				Expect(err).To(BeNil(), testCase)
				Expect(id2).To(Equal(id), testCase)
			}
		})

		It("Should return error for invalid keys", func() {
			head := vertexId(1, []byte("0123456789"))
			tail := vertexId(2, []byte("0123456789abcdefghij0123456789"))

			valid := graphi.EncodeEdgeId(edgeId(1, head, tail))
			tailOffset := len(graphi.EncodeVertexId(head)) + 4

			invalid := [][]byte{
				valid[:7],                      // too short
				valid[:8],                      // too short
				valid[1:],                      // trimmed
				valid[:len(valid)-1],           // trimmed
				append(valid, 0),               // key contains extra padding
				alterKey(valid, 4, valid[4]+1), // alter head value length encoding
				alterKey(valid, 5, valid[5]+1), // alter head value length encoding
				alterKey(valid, 6, valid[6]+1), // alter head value length encoding
				alterKey(valid, 7, valid[7]+1), // alter head value length encoding
				alterKey(valid, 7, 0),          // set head value length to zero
				alterKey(valid, tailOffset+4, valid[tailOffset+4]+1), // alter tail value length encoding
				alterKey(valid, tailOffset+5, valid[tailOffset+5]+1), // alter tail value length encoding
				alterKey(valid, tailOffset+6, valid[tailOffset+6]+1), // alter tail value length encoding
				alterKey(valid, tailOffset+7, valid[tailOffset+7]+1), // alter tail value length encoding
				alterKey(valid, tailOffset+7, 0),                     // set tail value length to zero
			}

			for i, key := range invalid {
				testCase := fmt.Sprintf("test case %d", i)
				_, err := graphi.DecodeEdgeId(key)
				Expect(err).NotTo(BeNil(), testCase)
			}
		})

		It("Should produce sortable edge keys for the same head vertex", func() {
			head := vertexId(7, []byte{0, 1})
			sorted := makeSortedEdgeIds(head, 0, 2, 3, 7)
			sortedKeys := encodeEdgeIds(sorted...)

			for range 10 {
				shuffledKeys := utils.ShuffleSlice(slices.Clone(sortedKeys))
				sort.Sort(keysSorter(shuffledKeys))
				Expect(shuffledKeys).To(Equal(sortedKeys))
			}
		})
	})

	Context("Global sorting of vertex and edge keys", func() {
		It("Should produce the correct total order", func() {
			heads := makeSortedVertexIds(3, 5, 7)

			var sortedKeys [][]byte
			for _, head := range heads {
				sortedKeys = append(sortedKeys, graphi.EncodeVertexId(head))
				edges := makeSortedEdgeIds(head, 3, 5, 7, 9)
				sortedKeys = append(sortedKeys, encodeEdgeIds(edges...)...)
			}

			for range 10 {
				shuffledKeys := utils.ShuffleSlice(slices.Clone(sortedKeys))
				sort.Sort(keysSorter(shuffledKeys))
				Expect(shuffledKeys).To(Equal(sortedKeys))
			}
		})
	})
})

type keysSorter [][]byte

func (x keysSorter) Len() int           { return len(x) }
func (x keysSorter) Less(i, j int) bool { return bytes.Compare(x[i], x[j]) < 0 }
func (x keysSorter) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
