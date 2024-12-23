package jepsen

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/pkg/keyvalue"
)

type workload struct {
	config      Config
	totalWeight int
	writeWeight int
	keys        [][]byte
	lock        sync.Mutex
	index       []int
	unique      []uint32 // Elle paper Section 3 "Deducing Dependencies"
}

func newWorkload(config Config) *workload {
	totalWeight := 1000
	writeWeight := int(float64(totalWeight) / (1 + config.ReadWriteRatio))

	keys := make([][]byte, config.RequestMaxKeys)
	index := make([]int, config.RequestMaxKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("r%dk%d", config.RunId, i+1))
		index[i] = i
	}

	return &workload{
		config:      config,
		totalWeight: totalWeight,
		writeWeight: writeWeight,
		keys:        keys,
		index:       index,
		unique:      make([]uint32, config.RequestMaxKeys),
	}
}

func (r *workload) Next() any {
	r.lock.Lock()
	defer r.lock.Unlock()

	w := rand.IntN(r.totalWeight) + 1
	if w <= r.writeWeight {
		return r.newSetRequest()
	}

	return &keyvalue.GetRequest{Keys: r.randKeys()}
}

func (r *workload) randIndexSlice() []int {
	utils.ShuffleSlice(r.index)
	count := r.config.RequestMinKeys + rand.IntN(r.config.RequestMaxKeys-r.config.RequestMinKeys+1)
	return r.index[:count]
}

func (r *workload) newSetRequest() *keyvalue.SetRequest {
	index := r.randIndexSlice()
	items := make([]*keyvalue.KeyValue, len(index))

	for i, idx := range index {
		r.unique[idx]++

		value := make([]byte, 4)
		binary.BigEndian.PutUint32(value, r.unique[idx])

		items[i] = &keyvalue.KeyValue{
			Key:   r.keys[idx],
			Value: value,
		}
	}

	return &keyvalue.SetRequest{Items: items}
}

func (r *workload) randKeys() [][]byte {
	index := r.randIndexSlice()
	result := make([][]byte, len(index))

	for i, idx := range index {
		result[i] = r.keys[idx]
	}
	return result
}
