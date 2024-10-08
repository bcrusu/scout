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
			items := []struct {
				addr  kv.Address
				flags kv.Flags
			}{
				{kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 1000}, 1},
				{kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 2000}, 2},
				{kv.Address{Keyspace: 2, Key: bytes("abcdef"), Timestamp: 2000}, 2},
				{kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 3000}, 3},
				{kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 4000}, 4},
				{kv.Address{Keyspace: 2, Key: bytes("abcdef"), Timestamp: 4000}, 4},
				{kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 5000}, 5},
			}

			expected := [][]byte{
				kv.EncodeKey(kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 5000}, 5),
				kv.EncodeKey(kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 4000}, 4),
				kv.EncodeKey(kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 3000}, 3),
				kv.EncodeKey(kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 2000}, 2),
				kv.EncodeKey(kv.Address{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 1000}, 1),
				kv.EncodeKey(kv.Address{Keyspace: 2, Key: bytes("abcdef"), Timestamp: 4000}, 4),
				kv.EncodeKey(kv.Address{Keyspace: 2, Key: bytes("abcdef"), Timestamp: 2000}, 2),
			}

			actual := make([][]byte, len(items))
			for i, x := range items {
				actual[i] = kv.EncodeKey(x.addr, x.flags)
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
