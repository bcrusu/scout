package kv_test

import (
	"fmt"
	"testing"

	"github.com/bcrusu/graph/internal/data/server/storage/kv"
	"github.com/bcrusu/graph/internal/utils/tests"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUtils(t *testing.T) {
	tests.NewSuite(t, "kv test suite")
}

var _ = Describe("Encoding tests", func() {
	bytes := func(v string) []byte {
		return []byte(v)
	}

	Context("Encode", func() {
		It("Should match Decode", func() {
			cases := []struct {
				addr  kv.Address
				flags kv.Flags
			}{
				{
					addr: kv.Address{
						Keyspace:  1238301,
						Key:       bytes("abcdef"),
						Timestamp: 8201230896,
					},
					flags: 1,
				},
				{
					addr: kv.Address{
						Keyspace:  9999,
						Key:       bytes("abcdef"),
						Timestamp: 1822012308960001,
					},
					flags: 123,
				},
			}

			for i, c := range cases {
				caseLabel := fmt.Sprintf("test case %d", i)

				x := kv.EncodeKey(c.addr, c.flags)
				addr, flags := kv.DecodeKey(x)

				Expect(addr).To(Equal(c.addr), caseLabel)
				Expect(flags).To(Equal(c.flags), caseLabel)
			}
		})
	})
})
