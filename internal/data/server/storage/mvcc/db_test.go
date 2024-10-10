package mvcc_test

import (
	"fmt"
	"testing"

	"github.com/bcrusu/scout/internal/data/server/storage/inmem"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/internal/utils/tests"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUtils(t *testing.T) {
	tests.NewSuite(t, "mvcc test suite")
}

var _ = Describe("db tests", func() {
	var db mvcc.DB

	BeforeEach(func() {
		db = mvcc.New(1, inmem.New())
	})

	bytes := func(v string) []byte {
		return []byte(v)
	}

	makeAddress := func(ks uint32, key string, ts uint64) kv.Address {
		return kv.Address{
			Keyspace:  ks,
			Key:       bytes(key),
			Timestamp: ts,
		}
	}

	makeRange := func(ks uint32, start, end string, ts uint64) mvcc.Range {
		return mvcc.Range{
			Keyspace:  ks,
			StartKey:  bytes(start),
			EndKey:    bytes(end),
			Timestamp: ts,
		}
	}

	makeValue := func(ks uint32, key string, ts uint64) []byte {
		return bytes(fmt.Sprintf("data_%d_%s_%d", ks, key, ts))
	}

	makeEntry := func(ks uint32, key string, ts uint64, flags mvcc.Flags) mvcc.Entry {
		return mvcc.Entry{
			Address: makeAddress(ks, key, ts),
			Value:   makeValue(ks, key, ts),
			Flags:   flags,
		}
	}

	makeEntryPtr := func(ks uint32, key string, ts uint64, flags mvcc.Flags) *mvcc.Entry {
		return utils.PointerOf(makeEntry(ks, key, ts, flags))
	}

	isEmpty := func(iter mvcc.Iterator) bool {
		for range iter {
			return false
		}
		return true
	}

	expectIter := func(iter mvcc.Iterator, description string, expected ...mvcc.Entry) {
		all := []mvcc.Entry{}

		for p, err := range iter {
			Expect(err).To(BeNil(), description)
			all = append(all, p)
		}

		Expect(all).To(Equal(expected), description)
	}

	Context("When is empty", func() {
		addr := makeAddress(1, "abc", 100)
		rang := makeRange(1, "abc000", "abc999", 1000)

		It("Get should return nil", func() {
			Expect(db.Get(addr)).To(BeNil())
		})

		It("GetRange should return empty range", func() {
			iter, err := db.GetRange(rang)
			Expect(err).To(BeNil())
			Expect(isEmpty(iter)).To(BeTrue())
		})

		It("Exists should return false", func() {
			Expect(db.Exists(addr)).To(BeFalse())
		})

		It("ExistsInRange should return false", func() {
			Expect(db.ExistsInRange(rang)).To(BeFalse())
		})

		It("Put should be successful", func() {
			entries := []mvcc.Entry{
				makeEntry(1, "key1", 1000, 0),
				makeEntry(1, "key1", 2000, 0),
				makeEntry(1, "key1", 3000, 0),
			}

			Expect(db.Put(1, entries...)).To(BeNil())
			Expect(db.Get(makeAddress(1, "key1", 3000))).To(Equal(&entries[2]))
			Expect(db.Get(makeAddress(1, "key1", 2000))).To(Equal(&entries[1]))
			Expect(db.Get(makeAddress(1, "key1", 1000))).To(Equal(&entries[0]))
		})
	})

	Context("When is not empty", func() {
		ks := []uint32{100, 107, 109}
		keys := []string{"777", "ccc", "kkk", "ttt"}
		lastTs := uint64(3000)
		ts := []uint64{lastTs, 2500, 2000, 1500, 1000}
		deleted := []bool{false, true, false, true, false}

		initKeyspace := func(keyspace uint32) {
			for _, key := range keys {
				for i, t := range ts {
					flags := mvcc.FlagEmpty
					if deleted[i] {
						flags = mvcc.FlagTombstone
					}

					entry := makeEntry(keyspace, key, t, flags)
					Expect(db.Put(1, entry)).To(BeNil())

					if !deleted[i] {
						Expect(db.Get(entry.Address)).To(Equal(&entry))
					} else {
						Expect(db.Get(entry.Address)).To(BeNil())
					}
				}
			}
		}

		initKeyspaces := func() {
			for _, ks := range ks {
				initKeyspace(ks)
			}
		}

		BeforeEach(func() {
			initKeyspaces()
		})

		It("Get/Exists should return correct value", func() {
			cases := []struct {
				addr     kv.Address
				expected *mvcc.Entry
			}{
				{makeAddress(999, keys[0], ts[0]), nil},                                                     // unknown keyspace
				{makeAddress(ks[0], "999", ts[0]), nil},                                                     // unknown key
				{makeAddress(ks[0], keys[0], 999), nil},                                                     // unknown timestamp
				{makeAddress(ks[0], keys[0], 0), makeEntryPtr(ks[0], keys[0], lastTs, mvcc.FlagEmpty)},      // latest
				{makeAddress(ks[1], keys[2], ts[0]+1), makeEntryPtr(ks[1], keys[2], ts[0], mvcc.FlagEmpty)}, // last
				{makeAddress(ks[1], keys[2], ts[0]), makeEntryPtr(ks[1], keys[2], ts[0], mvcc.FlagEmpty)},   // last
				{makeAddress(ks[1], keys[2], ts[0]-1), nil},                                                 // deleted
				{makeAddress(ks[1], keys[2], ts[1]+1), nil},                                                 // deleted
				{makeAddress(ks[1], keys[2], ts[1]), nil},                                                   // deleted
				{makeAddress(ks[1], keys[2], ts[1]-1), makeEntryPtr(ks[1], keys[2], ts[2], mvcc.FlagEmpty)}, // not deleted
				{makeAddress(ks[1], keys[2], ts[2]+1), makeEntryPtr(ks[1], keys[2], ts[2], mvcc.FlagEmpty)}, // not deleted
				{makeAddress(ks[1], keys[2], ts[2]), makeEntryPtr(ks[1], keys[2], ts[2], mvcc.FlagEmpty)},   // not deleted
				{makeAddress(ks[1], keys[2], ts[2]-1), nil},                                                 // deleted
			}

			for i, c := range cases {
				caseLabel := fmt.Sprintf("test case %d", i)
				Expect(db.Get(c.addr)).To(Equal(c.expected), caseLabel)
				Expect(db.Exists(c.addr)).To(Equal(c.expected != nil), caseLabel)
			}
		})

		It("GetRange/ExistsInRange should return correct values", func() {
			cases := []struct {
				rang     mvcc.Range
				expected []mvcc.Entry
			}{
				{
					rang: makeRange(ks[0], keys[0], keys[1], lastTs), // does not include the end key
					expected: []mvcc.Entry{
						makeEntry(ks[0], keys[0], ts[0], mvcc.FlagEmpty),
					},
				},
				{
					rang:     makeRange(ks[0], keys[0], keys[0], lastTs), // start==end key will not be included
					expected: []mvcc.Entry{},
				},
				{
					rang: makeRange(ks[0], "", "", 0), // latest value for all
					expected: []mvcc.Entry{
						makeEntry(ks[0], keys[0], ts[0], mvcc.FlagEmpty),
						makeEntry(ks[0], keys[1], ts[0], mvcc.FlagEmpty),
						makeEntry(ks[0], keys[2], ts[0], mvcc.FlagEmpty),
						makeEntry(ks[0], keys[3], ts[0], mvcc.FlagEmpty),
					},
				},
				{
					rang: makeRange(ks[2], keys[0], keys[3], ts[2]), // snapshot read for multiple keys
					expected: []mvcc.Entry{
						makeEntry(ks[2], keys[0], ts[2], mvcc.FlagEmpty),
						makeEntry(ks[2], keys[1], ts[2], mvcc.FlagEmpty),
						makeEntry(ks[2], keys[2], ts[2], mvcc.FlagEmpty),
					},
				},
				{
					rang: makeRange(ks[2], keys[0], keys[3], ts[2]+1), // snapshot read for multiple keys
					expected: []mvcc.Entry{
						makeEntry(ks[2], keys[0], ts[2], mvcc.FlagEmpty),
						makeEntry(ks[2], keys[1], ts[2], mvcc.FlagEmpty),
						makeEntry(ks[2], keys[2], ts[2], mvcc.FlagEmpty),
					},
				},
				{
					rang:     makeRange(ks[1], "0", "z", ts[2]-1), // snapshot read for deleted keys
					expected: []mvcc.Entry{},
				},
				{
					rang:     makeRange(ks[1], "0", "z", ts[1]), // snapshot read for deleted keys
					expected: []mvcc.Entry{},
				},
			}

			for i, c := range cases {
				caseLabel := fmt.Sprintf("test case %d", i)

				// GetRange
				iter, err := db.GetRange(c.rang)
				Expect(iter).NotTo(BeNil(), caseLabel)
				Expect(err).To(BeNil(), caseLabel)
				expectIter(iter, caseLabel, c.expected...)

				// ExistsInRange
				Expect(db.ExistsInRange(c.rang)).To(Equal(len(c.expected) > 0), caseLabel)
			}
		})

		It("Put should insert value at timestamp", func() {
			timestamp := ts[1] + 10
			entry := makeEntry(ks[1], keys[1], timestamp, mvcc.FlagEmpty)
			Expect(db.Put(1, entry)).To(BeNil())
			Expect(db.Get(entry.Address)).To(Equal(&entry))
		})
	})
})
