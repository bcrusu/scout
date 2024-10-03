package inmem_test

import (
	"fmt"
	"testing"

	"github.com/bcrusu/graph/internal/data/server/storage"
	"github.com/bcrusu/graph/internal/data/server/storage/inmem"
	"github.com/bcrusu/graph/internal/utils/tests"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUtils(t *testing.T) {
	tests.NewSuite(t, "inmemDB test suite")
}

var _ = Describe("inmemDB tests", func() {
	var db storage.DB

	BeforeEach(func() {
		db = inmem.New()
	})

	bytes := func(v string) []byte {
		return []byte(v)
	}

	makeLoc := func(pid uint32, ks uint64, key string, ts uint64) storage.Location {
		return storage.Location{
			PartitionID: pid,
			Keyspace:    ks,
			Key:         bytes(key),
			Timestamp:   ts,
		}
	}

	makeRange := func(pid uint32, ks uint64, start, end string, ts uint64) storage.Range {
		return storage.Range{
			PartitionID: pid,
			Keyspace:    ks,
			StartKey:    bytes(start),
			EndKey:      bytes(end),
			Timestamp:   ts,
		}
	}

	valueAt := func(data []byte, loc storage.Location) *storage.ValueAt {
		return &storage.ValueAt{
			Data:     data,
			Location: loc,
		}
	}

	isEmpty := func(iter storage.Iterator) bool {
		for range iter {
			return false
		}
		return true
	}

	expectRange := func(iter storage.Iterator, description string, expected ...*storage.ValueAt) {
		j := 0
		for value, err := range iter {
			Expect(j).To(BeNumerically("<", len(expected)), description)
			Expect(&value).To(Equal(expected[j]), description)
			Expect(err).To(BeNil(), description)
			j++
		}
		Expect(j).To(Equal(len(expected)), description)
	}

	Context("When is empty", func() {
		loc := makeLoc(1, 1, "abc", 100)
		rang := makeRange(1, 1, "abc000", "abc999", 1000)

		It("Get should return nil", func() {
			Expect(db.Get(loc)).To(BeNil())
		})

		It("GetRange should return empty range", func() {
			iter, err := db.GetRange(rang)
			Expect(err).To(BeNil())
			Expect(isEmpty(iter)).To(BeTrue())
		})

		It("Exists should return false", func() {
			Expect(db.Exists(loc)).To(BeFalse())
		})

		It("ExistsInRange should return false", func() {
			Expect(db.ExistsInRange(rang)).To(BeFalse())
		})

		It("Set should be successful", func() {
			data := bytes("value1")
			Expect(db.Set(loc, data)).To(BeNil())

			expected := valueAt(data, loc)
			Expect(db.Get(loc)).To(Equal(expected))
		})

		It("Delete should be successful", func() {
			Expect(db.Delete(loc)).To(BeNil())
			Expect(db.Get(loc)).To(BeNil())
		})
	})

	Context("When is not empty", func() {
		part := []uint32{10, 20, 70}
		ks := []uint64{100, 107, 109}
		keys := []string{"777", "ccc", "kkk", "ttt"}
		lastTs := uint64(3000)
		ts := []uint64{lastTs, 2500, 2000, 1500, 1000}
		deleted := []bool{false, true, false, true, false}

		makeData := func(pid uint32, ks uint64, key string, ts uint64) []byte {
			return bytes(fmt.Sprintf("data_%d_%d_%s_%d", pid, ks, key, ts))
		}

		locDataPair := func(pid uint32, ks uint64, key string, ts uint64) (storage.Location, []byte) {
			return makeLoc(pid, ks, key, ts), makeData(pid, ks, key, ts)
		}

		makeValueAt := func(pid uint32, ks uint64, key string, atTs uint64) *storage.ValueAt {
			return valueAt(makeData(pid, ks, key, atTs), makeLoc(pid, ks, key, atTs))
		}

		initKeyspace := func(pid uint32, keyspace uint64) {
			for _, key := range keys {
				for i, t := range ts {
					loc, data := locDataPair(pid, keyspace, key, t)

					if !deleted[i] {
						Expect(db.Set(loc, data)).To(BeNil())
						expected := valueAt(data, loc)
						Expect(db.Get(loc)).To(Equal(expected))
					} else {
						db.Delete(loc)
						Expect(db.Get(loc)).To(BeNil())
					}
				}
			}
		}

		initPartitions := func() {
			for _, pid := range part {
				for _, ks := range ks {
					initKeyspace(pid, ks)
				}
			}
		}

		BeforeEach(func() {
			initPartitions()
		})

		It("Get/Exists should return correct value", func() {
			cases := []struct {
				loc      storage.Location
				expected *storage.ValueAt
			}{
				{makeLoc(999, ks[0], keys[0], ts[0]), nil},                                               // unknown partition
				{makeLoc(part[0], 999, keys[0], ts[0]), nil},                                             // unknown keyspace
				{makeLoc(part[0], ks[0], "999", ts[0]), nil},                                             // unknown key
				{makeLoc(part[0], ks[0], keys[0], 999), nil},                                             // unknown timestamp
				{makeLoc(part[0], ks[0], keys[0], 0), makeValueAt(part[0], ks[0], keys[0], lastTs)},      // latest
				{makeLoc(part[0], ks[1], keys[2], ts[0]+1), makeValueAt(part[0], ks[1], keys[2], ts[0])}, // last
				{makeLoc(part[0], ks[1], keys[2], ts[0]), makeValueAt(part[0], ks[1], keys[2], ts[0])},   // last
				{makeLoc(part[0], ks[1], keys[2], ts[0]-1), nil},                                         // deleted
				{makeLoc(part[0], ks[1], keys[2], ts[1]+1), nil},                                         // deleted
				{makeLoc(part[0], ks[1], keys[2], ts[1]), nil},                                           // deleted
				{makeLoc(part[0], ks[1], keys[2], ts[1]-1), makeValueAt(part[0], ks[1], keys[2], ts[2])}, // not deleted
				{makeLoc(part[0], ks[1], keys[2], ts[2]+1), makeValueAt(part[0], ks[1], keys[2], ts[2])}, // not deleted
				{makeLoc(part[0], ks[1], keys[2], ts[2]), makeValueAt(part[0], ks[1], keys[2], ts[2])},   // not deleted
				{makeLoc(part[0], ks[1], keys[2], ts[2]-1), nil},                                         // deleted
			}

			for i, c := range cases {
				caseLabel := fmt.Sprintf("test case %d", i)
				Expect(db.Get(c.loc)).To(Equal(c.expected), caseLabel)
				Expect(db.Exists(c.loc)).To(Equal(c.expected != nil), caseLabel)
			}
		})

		It("GetRange/ExistsInRange should return correct values", func() {
			cases := []struct {
				rang     storage.Range
				expected []*storage.ValueAt
			}{
				{
					rang: makeRange(part[0], ks[0], keys[0], keys[1], lastTs), // does not include the end key
					expected: []*storage.ValueAt{
						makeValueAt(part[0], ks[0], keys[0], ts[0]),
					},
				},
				{
					rang:     makeRange(part[0], ks[0], keys[0], keys[0], lastTs), // start==end key will not be included
					expected: []*storage.ValueAt{},
				},
				{
					rang: makeRange(part[1], ks[0], "0", "z", 0), // latest value for all
					expected: []*storage.ValueAt{
						makeValueAt(part[1], ks[0], keys[0], ts[0]),
						makeValueAt(part[1], ks[0], keys[1], ts[0]),
						makeValueAt(part[1], ks[0], keys[2], ts[0]),
						makeValueAt(part[1], ks[0], keys[3], ts[0]),
					},
				},
				{
					rang: makeRange(part[0], ks[2], keys[0], keys[3], ts[2]), // snapshot read for multiple keys
					expected: []*storage.ValueAt{
						makeValueAt(part[0], ks[2], keys[0], ts[2]),
						makeValueAt(part[0], ks[2], keys[1], ts[2]),
						makeValueAt(part[0], ks[2], keys[2], ts[2]),
					},
				},
				{
					rang: makeRange(part[0], ks[2], keys[0], keys[3], ts[2]+1), // snapshot read for multiple keys
					expected: []*storage.ValueAt{
						makeValueAt(part[0], ks[2], keys[0], ts[2]),
						makeValueAt(part[0], ks[2], keys[1], ts[2]),
						makeValueAt(part[0], ks[2], keys[2], ts[2]),
					},
				},
				{
					rang:     makeRange(part[2], ks[1], "0", "z", ts[2]-1), // snapshot read for deleted keys
					expected: []*storage.ValueAt{},
				},
				{
					rang:     makeRange(part[2], ks[1], "0", "z", ts[1]), // snapshot read for deleted keys
					expected: []*storage.ValueAt{},
				},
			}

			for i, c := range cases {
				caseLabel := fmt.Sprintf("test case %d", i)

				// GetRange
				iter, err := db.GetRange(c.rang)
				Expect(iter).NotTo(BeNil(), caseLabel)
				Expect(err).To(BeNil(), caseLabel)
				expectRange(iter, caseLabel, c.expected...)

				// ExistsInRange
				Expect(db.ExistsInRange(c.rang)).To(Equal(len(c.expected) > 0), caseLabel)
			}
		})

		It("Set should insert value at timestamp", func() {
			timestamp := ts[1] + 10
			loc := makeLoc(part[1], ks[1], keys[1], timestamp)
			data := bytes("test1")
			Expect(db.Set(loc, data)).To(BeNil())
			Expect(db.Get(loc)).To(Equal(valueAt(data, loc)))
		})

		It("Delete should delete at timestamp", func() {
			timestamp := ts[1] + 10
			loc := makeLoc(part[1], ks[1], keys[1], timestamp)
			Expect(db.Delete(loc)).To(BeNil())
			Expect(db.Get(loc)).To(BeNil())
		})
	})
})
