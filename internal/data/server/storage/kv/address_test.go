package kv_test

import (
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/utils/tests"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	tests.NewSuite(t, "KV test suite")
}

var _ = Describe("Address tests", func() {
	bytes := func(v string) []byte {
		return []byte(v)
	}

	Context("Next", func() {
		addr := kv.NewAddress

		It("Should return next address in lexicographic order", func() {
			cases := []struct {
				address  kv.Address
				expected kv.Address
			}{
				{addr(0, []byte{0, 0, 0}, 10), addr(0, []byte{0, 0, 0}, 9)},                                // timestamps are ordered desc
				{addr(0, []byte{0, 0, 0}, 0), addr(0, []byte{1, 0, 0}, math.MaxUint64)},                    // timestamp wraps and key[0] is incremented
				{addr(0, []byte{10, 0, 0}, 0), addr(0, []byte{11, 0, 0}, math.MaxUint64)},                  // timestamp wraps and key[0] is incremented
				{addr(0, []byte{255, 0, 0}, 0), addr(0, []byte{0, 1, 0}, math.MaxUint64)},                  // timestamp wraps and key[1] is incremented
				{addr(0, []byte{255, 10, 0}, 0), addr(0, []byte{0, 11, 0}, math.MaxUint64)},                // timestamp wraps and key[1] is incremented
				{addr(0, []byte{255, 255, 0}, 0), addr(0, []byte{0, 0, 1}, math.MaxUint64)},                // timestamp wraps and key[2] is incremented
				{addr(0, []byte{255, 255, 10}, 0), addr(0, []byte{0, 0, 11}, math.MaxUint64)},              // timestamp wraps and key[2] is incremented
				{addr(10, []byte{255, 255, 255}, 0), addr(11, []byte{0, 0, 0}, math.MaxUint64)},            // timestamp and key wrap and keyspace is incremeted
				{addr(math.MaxUint32, []byte{255, 255, 255}, 0), addr(0, []byte{0, 0, 0}, math.MaxUint64)}, // timestamp, key, and keyspace wrap back to first address
			}

			for i, c := range cases {
				caseLabel := fmt.Sprintf("test case %d", i)

				actual := c.address.Next()
				Expect(actual).To(Equal(c.expected), caseLabel)
			}
		})
	})

	Context("NextKey", func() {
		addr := kv.NewAddress

		It("Should return next address in lexicographic order", func() {
			cases := []struct {
				address  kv.Address
				expected kv.Address
			}{
				{addr(0, []byte{10, 0, 0}, 999), addr(0, []byte{11, 0, 0}, math.MaxUint64)},                  // key[0] is incremented
				{addr(0, []byte{255, 0, 0}, 999), addr(0, []byte{0, 1, 0}, math.MaxUint64)},                  // key[0] wraps and key[1] is incremented
				{addr(0, []byte{255, 255, 0}, 999), addr(0, []byte{0, 0, 1}, math.MaxUint64)},                // key[:2] wraps and key[2] is incremented
				{addr(10, []byte{255, 255, 255}, 999), addr(11, []byte{0, 0, 0}, math.MaxUint64)},            // key[:3] wraps and keyspace is incremeted
				{addr(math.MaxUint32, []byte{255, 255, 255}, 999), addr(0, []byte{0, 0, 0}, math.MaxUint64)}, // key[:3] and keyspace wrap back to first address
			}

			for i, c := range cases {
				caseLabel := fmt.Sprintf("test case %d", i)

				actual := c.address.NextKey()
				Expect(actual).To(Equal(c.expected), caseLabel)
			}
		})
	})

	Context("Compare", func() {
		It("Should return the expected ordering", func() {
			addrs := []kv.Address{
				{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 1000},
				{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 2000},
				{Keyspace: 2, Key: bytes("abcdef"), Timestamp: 2000},
				{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 3000},
				{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 4000},
				{Keyspace: 2, Key: bytes("abcdef"), Timestamp: 4000},
				{Keyspace: 1, Key: bytes("abcdef"), Timestamp: 5000},
			}

			expected := []kv.Address{
				addrs[6],
				addrs[4],
				addrs[3],
				addrs[1],
				addrs[0],
				addrs[5],
				addrs[2],
			}

			sort.Sort(sorter(addrs))

			Expect(addrs).To(Equal(expected))
		})
	})
})

type sorter []kv.Address

func (x sorter) Len() int           { return len(x) }
func (x sorter) Less(i, j int) bool { return x[i].Before(x[j]) }
func (x sorter) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
