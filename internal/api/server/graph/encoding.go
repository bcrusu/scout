package graph

import (
	"encoding/binary"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/pkg/graph"
)

// The graph is composed of vertices and directed endges with
// each having a set o properties, thus it forms a property graph.
//
// Undirected edges (v1--v2) will be represented by using two
// directed edges v1-->v2 and v2-->v1.
//
// Each vertex and its edges are stored in a single partition, in
// a single continuous key range. This allows transactions to easily
// acquire locks for the vertex and all or a subset of its edges by
// using a single lock instruction. Similarly, checking if a vertex
// has edges during its deletion is simplified by using a single db
// iterator for the edge range.

// EncodeVertexId encodes the vertex identifier into a byte slice.
//
// Adding the len(Value) to the encoded key has a twofold role:
//   - it makes decoding of edge keys unambiguous as they are encoded
//     by the concatenation of head and tail vertex ids, and
//   - it ensures that a vertex and its edges forms a continuous key
//     range which is especially important as the Value slices can have
//     variable length and can share prefixes.
//
// +---------+-------------+--------+
// |  Type   | len(Value) |  Value  |
// | 4 Bytes |  4 Bytes   | N Bytes |
// +---------+------------+---------+
func EncodeVertexId(id *graph.VertexId) []byte {
	key := make([]byte, 0, 8+len(id.Value))
	key = appendVertexKey(key, id)
	return key
}

func appendVertexKey(bytes []byte, id *graph.VertexId) []byte {
	bytes = binary.BigEndian.AppendUint32(bytes, id.Type)
	bytes = binary.BigEndian.AppendUint32(bytes, uint32(len(id.Value)))
	bytes = append(bytes, id.Value...)
	return bytes
}

// DecodeVertexId decodes the vertex id from the byte slice.
func DecodeVertexId(key []byte) (*graph.VertexId, error) {
	id, readLen, err := decodeVertexKey(key)
	if readLen != len(key) {
		return nil, errors.Error("unexpected key padding")
	}

	return id, err
}

func decodeVertexKey(key []byte) (*graph.VertexId, int, error) {
	if len(key) <= 8 {
		return nil, 0, errors.Error("key is too short")
	}

	valueLen := int(binary.BigEndian.Uint32(key[4:8]))
	if valueLen == 0 {
		return nil, 0, errors.Error("value length is zero")
	}

	if valueLen > len(key)-8 {
		return nil, 0, errors.Error("value length too large or trimmed key")
	}

	id := &graph.VertexId{
		Type:  binary.BigEndian.Uint32(key[0:4]),
		Value: key[8 : valueLen+8],
	}

	return id, valueLen + 8, nil
}

// EncodeEdgeId encodes the edge identifier into a byte slice.
//
// +--------------------------------+---------+--------------------------------+
// |             Head Id            |  Type   |             Tail Id            |
// +---------+------------+---------+---------+---------+------------+---------+
// |  Head   |    Head    |  Head   |  Edge   |  Tail   |    Tail    |  Tail   |
// |  Type   | len(Value) |  Value  |  Type   |  Type   | len(Value) |  Value  |
// | 4 Bytes |  4 Bytes   | N Bytes | 4 Bytes | 4 Bytes |  4 Bytes   | N Bytes |
// +---------+------------+---------+---------+---------+------------+---------+
func EncodeEdgeId(id *graph.EdgeId) []byte {
	key := make([]byte, 0, 20+len(id.Head.Value)+len(id.Tail.Value))
	key = appendVertexKey(key, id.Head)
	key = binary.BigEndian.AppendUint32(key, id.Type)
	key = appendVertexKey(key, id.Tail)
	return key
}

// DecodeVertexId decodes the edge id from the byte slice.
func DecodeEdgeId(key []byte) (*graph.EdgeId, error) {
	head, headLen, err := decodeVertexKey(key)
	if err != nil {
		return nil, errors.Error("invalid head key")
	}

	tail, tailLen, err := decodeVertexKey(key[headLen+4:])
	if err != nil {
		return nil, errors.Error("invalid tail key")
	}

	if headLen+tailLen+4 != len(key) {
		return nil, errors.Error("unexpected key padding")
	}

	return &graph.EdgeId{
		Head: head,
		Tail: tail,
		Type: binary.BigEndian.Uint32(key[headLen : headLen+4]),
	}, nil
}
