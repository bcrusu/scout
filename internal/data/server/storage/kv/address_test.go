package kv_test

import (
	"fmt"
	"math"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Address tests", func() {
	bytes := func(v string) []byte {
		return []byte(v)
	}

	Context("Encode", func() {
		It("Should match Decode", func() {
			addrs := []kv.Address{
				{Keyspace: 1238301, Key: bytes("abcdef"), Timestamp: 8201230896},
				{Keyspace: 9999, Key: bytes("abcdef"), Timestamp: 1822012308960001},
			}

			for i, addr := range addrs {
				caseLabel := fmt.Sprintf("test case %d", i)

				x := addr.Encode()
				addr2 := kv.DecodeAddress(x)

				Expect(addr2).To(Equal(addr), caseLabel)
			}
		})
	})

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
})
