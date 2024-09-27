package txn

import "github.com/cespare/xxhash/v2"

type partitioner struct {
	partitionCount uint64
}

func newPartitioner(partitionCount uint32) *partitioner {
	return &partitioner{
		partitionCount: uint64(partitionCount),
	}
}

func (p *partitioner) getPartition(key []byte) uint32 {
	h := xxhash.New()
	h.Write(key)
	hash := h.Sum64()
	result := hash % p.partitionCount
	return uint32(result)
}
