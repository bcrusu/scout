package inmem_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/bcrusu/scout/internal/data/server/storage/inmem"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/utils/tests"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUtils(t *testing.T) {
	tests.NewSuite(t, "inmem test suite")
}

var _ = Describe("inmem tests", func() {
	var db *inmem.DB

	BeforeEach(func() {
		db = inmem.New()
	})

	bytes := func(v string) []byte {
		return []byte(v)
	}

	expectEmptyIter := func(iter kv.Iterator) {
		for range iter {
			Fail("Range is not empty")
		}
	}

	expectIter := func(iter kv.Iterator, description string, expected ...kv.Entry) {
		all := []kv.Entry{}

		for kv, err := range iter {
			Expect(err).To(BeNil(), description)
			all = append(all, kv)
		}

		Expect(all).To(Equal(expected), description)
	}

	Context("When is empty", func() {
		addr := kv.Address{Keyspace: 1, Key: bytes("abc"), Timestamp: 1000}
		entry := kv.Entry{Address: addr, Value: bytes("value1")}

		It("Get should return empty iter", func() {
			iter, err := db.Get(1, addr)
			Expect(err).To(BeNil())
			expectEmptyIter(iter)
		})

		It("Put should be successful", func() {
			Expect(db.Put(1, 1, entry)).To(BeNil())
			iter, err := db.Get(1, addr)
			Expect(err).To(BeNil())
			expectIter(iter, "", entry)
		})
	})

	Context("When is not empty", func() {
		makeAddress := func(key string, timestamp uint64) kv.Address {
			return kv.Address{Keyspace: 1, Key: bytes(key), Timestamp: timestamp}
		}

		makeEntry := func(pid uint32, key string, timestamp uint64) kv.Entry {
			return kv.Entry{
				Address: makeAddress(key, timestamp),
				Value:   bytes(fmt.Sprintf("data_%d_%s_%d", pid, key, timestamp)),
			}
		}

		part := map[uint32][]kv.Entry{
			10: {
				makeEntry(10, "777", 1000),
				makeEntry(10, "777", 2000),
				makeEntry(10, "777", 3000),
				makeEntry(10, "ccc", 1000),
				makeEntry(10, "ccc", 2000),
				makeEntry(10, "ttt", 5000),
			},
			99: {
				makeEntry(99, "777", 1000),
				makeEntry(99, "777", 2000),
				makeEntry(99, "777", 3000),
				makeEntry(99, "ccc", 1000),
				makeEntry(99, "ccc", 2000),
				makeEntry(99, "ttt", 5000),
			},
		}

		initPartitions := func() {
			for pid, entries := range part {
				Expect(db.Put(1, pid, entries...)).To(BeNil())
			}
		}

		BeforeEach(func() {
			initPartitions()
		})

		It("Get should return correct values", func() {
			cases := []struct {
				pid      uint32
				start    kv.Address
				expected []kv.Entry
			}{
				{
					pid:   10,
					start: kv.FirstAddress(),
					expected: []kv.Entry{
						makeEntry(10, "777", 3000),
						makeEntry(10, "777", 2000),
						makeEntry(10, "777", 1000),
						makeEntry(10, "ccc", 2000),
						makeEntry(10, "ccc", 1000),
						makeEntry(10, "ttt", 5000),
					},
				},
				{
					pid:   10,
					start: makeAddress("777", math.MaxUint64),
					expected: []kv.Entry{
						makeEntry(10, "777", 3000),
						makeEntry(10, "777", 2000),
						makeEntry(10, "777", 1000),
						makeEntry(10, "ccc", 2000),
						makeEntry(10, "ccc", 1000),
						makeEntry(10, "ttt", 5000),
					},
				},
				{
					pid:   10,
					start: makeAddress("777", 3000),
					expected: []kv.Entry{
						makeEntry(10, "777", 3000),
						makeEntry(10, "777", 2000),
						makeEntry(10, "777", 1000),
						makeEntry(10, "ccc", 2000),
						makeEntry(10, "ccc", 1000),
						makeEntry(10, "ttt", 5000),
					},
				},
				{
					pid:   10,
					start: makeAddress("777", 1500),
					expected: []kv.Entry{
						makeEntry(10, "777", 1000),
						makeEntry(10, "ccc", 2000),
						makeEntry(10, "ccc", 1000),
						makeEntry(10, "ttt", 5000),
					},
				},
				{
					pid:   99,
					start: makeAddress("8", 0),
					expected: []kv.Entry{
						makeEntry(99, "ccc", 2000),
						makeEntry(99, "ccc", 1000),
						makeEntry(99, "ttt", 5000),
					},
				},
				{
					pid:   99,
					start: makeAddress("d", 0),
					expected: []kv.Entry{
						makeEntry(99, "ttt", 5000),
					},
				},
				{
					pid:      99,
					start:    makeAddress("x", 0),
					expected: []kv.Entry{},
				},
			}

			for i, c := range cases {
				caseLabel := fmt.Sprintf("test case %d", i)

				iter, err := db.Get(c.pid, c.start)
				Expect(iter).NotTo(BeNil(), caseLabel)
				Expect(err).To(BeNil(), caseLabel)
				expectIter(iter, caseLabel, c.expected...)
			}
		})

		It("Put should insert value at timestamp", func() {
			cases := []struct {
				pid      uint32
				put      []kv.Entry
				expected []kv.Entry
			}{
				{
					pid: 10,
					put: []kv.Entry{
						makeEntry(10, "777", 3500),
						makeEntry(10, "777", 2500),
						makeEntry(10, "777", 500),
						makeEntry(10, "aaa", 1000),
						makeEntry(10, "ccc", 1500),
						makeEntry(10, "zzz", 1000),
					},
					expected: []kv.Entry{
						makeEntry(10, "777", 3500),
						makeEntry(10, "777", 3000),
						makeEntry(10, "777", 2500),
						makeEntry(10, "777", 2000),
						makeEntry(10, "777", 1000),
						makeEntry(10, "777", 500),
						makeEntry(10, "aaa", 1000),
						makeEntry(10, "ccc", 2000),
						makeEntry(10, "ccc", 1500),
						makeEntry(10, "ccc", 1000),
						makeEntry(10, "ttt", 5000),
						makeEntry(10, "zzz", 1000),
					},
				},
			}

			for i, c := range cases {
				caseLabel := fmt.Sprintf("test case %d", i)
				Expect(db.Put(1, c.pid, c.put...)).To(BeNil())

				iter, err := db.Get(c.pid, kv.FirstAddress())
				Expect(iter).NotTo(BeNil(), caseLabel)
				Expect(err).To(BeNil(), caseLabel)
				expectIter(iter, caseLabel, c.expected...)
			}
		})
	})
})
