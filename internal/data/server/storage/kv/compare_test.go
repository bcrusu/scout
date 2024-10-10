package kv_test

import (
	"sort"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Compare tests", func() {
	bytes := func(v string) []byte {
		return []byte(v)
	}

	Context("Compare", func() {
		It("Should allow keys ordering", func() {
			addrs := []kv.Address{
				{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 1000},
				{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 2000},
				{Keyspace: 2, Key: bytes("abcdef"), Timestamp: 2000},
				{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 3000},
				{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 4000},
				{Keyspace: 2, Key: bytes("abcdef"), Timestamp: 4000},
				{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 5000},
			}

			expected := [][]byte{
				kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 5000}.Encode(),
				kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 4000}.Encode(),
				kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 3000}.Encode(),
				kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 2000}.Encode(),
				kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 1000}.Encode(),
				kv.Address{Keyspace: 2, Key: bytes("abcdef"), Timestamp: 4000}.Encode(),
				kv.Address{Keyspace: 2, Key: bytes("abcdef"), Timestamp: 2000}.Encode(),
			}

			actual := make([][]byte, len(addrs))
			for i, x := range addrs {
				actual[i] = x.Encode()
			}

			sort.Sort(sorter(actual))

			Expect(actual).To(Equal(expected))
		})
	})
})

type sorter [][]byte

func (x sorter) Len() int           { return len(x) }
func (x sorter) Less(i, j int) bool { return kv.CompareKeys(x[i], x[j]) < 0 }
func (x sorter) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
