package mvcc

import (
	"fmt"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/utils"
	. "github.com/onsi/ginkgo/v2" //lint:ignore ST1001 makes writing tests much easier
	. "github.com/onsi/gomega"    //lint:ignore ST1001 makes writing tests much easier
)

// TestScenarios contains reusable test scenarios that should be executed
// by all MVCC implementations. Used by RocksDB and Emulated implementations.
func TestScenarios(kvdb kv.DB, db DB) {
	Describe("MVCC test scenarios", func() {
		pid := uint32(42)

		BeforeEach(func() {
			Expect(kvdb.InitPartition(pid)).To(BeNil())
		})

		AfterEach(func() {
			Expect(kvdb.DropPartition(pid)).To(BeNil())
		})

		type Range struct {
			start Address
			end   Address
		}

		bytes := func(v string) []byte {
			return []byte(v)
		}

		makeAddress := func(kyespace uint32, key string) Address {
			return NewAddress(kyespace, bytes(key))
		}

		makeRange := func(ks uint32, start, end string) Range {
			return Range{makeAddress(ks, start), makeAddress(ks, end)}
		}

		makeValue := func(ks uint32, key string, ts uint64) []byte {
			return bytes(fmt.Sprintf("data_%d_%s_%d", ks, key, ts))
		}

		makeRecord := func(ks uint32, key string, ts uint64, flags Flags) Record {
			return Record{
				Address:   makeAddress(ks, key),
				Timestamp: ts,
				Value:     makeValue(ks, key, ts),
				Flags:     flags,
			}
		}

		makeRecordPtr := func(ks uint32, key string, ts uint64, flags Flags) *Record {
			return utils.PointerOf(makeRecord(ks, key, ts, flags))
		}

		isEmpty := func(iter Iterator) bool {
			for range iter {
				return false
			}
			return true
		}

		expectIter := func(iter Iterator, description string, expected ...Record) {
			all := []Record{}

			for p, err := range iter {
				Expect(err).To(BeNil(), description)
				all = append(all, p)
			}

			Expect(all).To(Equal(expected), description)
		}

		Context("When is empty", func() {
			addr := makeAddress(1, "abc")
			ts := uint64(100)
			rang := makeRange(1, "abc000", "abc999")

			It("Get should return nil", func() {
				expected := []*Record{nil}
				Expect(db.Get(pid, ts, addr)).To(Equal(expected))
			})

			It("GetRange should return empty range", func() {
				iter, err := db.GetRange(pid, ts, rang.start, rang.end)
				Expect(err).To(BeNil())
				Expect(isEmpty(iter)).To(BeTrue())
			})

			It("Exists should return false", func() {
				Expect(db.Exists(pid, ts, addr)).To(BeFalse())
			})

			It("ExistsInRange should return false", func() {
				Expect(db.ExistsInRange(pid, ts, rang.start, rang.end)).To(BeFalse())
			})

			It("Put should be successful", func() {
				records := []Record{
					makeRecord(1, "key1", 1000, 0),
					makeRecord(1, "key1", 2000, 0),
					makeRecord(1, "key1", 3000, 0),
				}

				Expect(db.Put(pid, 1, records...)).To(BeNil())

				for _, record := range records {
					expected := []*Record{&record}
					Expect(db.Get(pid, record.Timestamp, record.Address)).To(Equal(expected))
				}
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
						flags := FlagEmpty
						if deleted[i] {
							flags = FlagTombstone
						}

						record := makeRecord(keyspace, key, t, flags)
						Expect(db.Put(pid, 1, record)).To(BeNil())

						if !deleted[i] {
							expected := []*Record{&record}
							Expect(db.Get(pid, record.Timestamp, record.Address)).To(Equal(expected))
						} else {
							expected := []*Record{nil}
							Expect(db.Get(pid, record.Timestamp, record.Address)).To(Equal(expected))
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
					addr     Address
					ts       uint64
					expected *Record
				}{
					{makeAddress(999, keys[0]), ts[0], nil},                                                   // unknown keyspace
					{makeAddress(ks[0], "999"), ts[0], nil},                                                   // unknown key
					{makeAddress(ks[0], keys[0]), 999, nil},                                                   // unknown timestamp
					{makeAddress(ks[0], keys[0]), 0, makeRecordPtr(ks[0], keys[0], lastTs, FlagEmpty)},        // latest
					{makeAddress(ks[1], keys[2]), ts[0] + 1, makeRecordPtr(ks[1], keys[2], ts[0], FlagEmpty)}, // last
					{makeAddress(ks[1], keys[2]), ts[0], makeRecordPtr(ks[1], keys[2], ts[0], FlagEmpty)},     // last
					{makeAddress(ks[1], keys[2]), ts[0] - 1, nil},                                             // deleted
					{makeAddress(ks[1], keys[2]), ts[1] + 1, nil},                                             // deleted
					{makeAddress(ks[1], keys[2]), ts[1], nil},                                                 // deleted
					{makeAddress(ks[1], keys[2]), ts[1] - 1, makeRecordPtr(ks[1], keys[2], ts[2], FlagEmpty)}, // not deleted
					{makeAddress(ks[1], keys[2]), ts[2] + 1, makeRecordPtr(ks[1], keys[2], ts[2], FlagEmpty)}, // not deleted
					{makeAddress(ks[1], keys[2]), ts[2], makeRecordPtr(ks[1], keys[2], ts[2], FlagEmpty)},     // not deleted
					{makeAddress(ks[1], keys[2]), ts[2] - 1, nil},                                             // deleted
				}

				for i, c := range cases {
					caseLabel := fmt.Sprintf("test case %d", i)
					expected := []*Record{c.expected}

					Expect(db.Get(pid, c.ts, c.addr)).To(Equal(expected), caseLabel)
					Expect(db.Exists(pid, c.ts, c.addr)).To(Equal(c.expected != nil), caseLabel)
				}
			})

			It("GetRange/ExistsInRange should return correct values", func() {
				cases := []struct {
					rang     Range
					ts       uint64
					expected []Record
				}{
					{
						rang: makeRange(ks[0], keys[0], keys[1]), // does not include the end key
						ts:   lastTs,
						expected: []Record{
							makeRecord(ks[0], keys[0], ts[0], FlagEmpty),
						},
					},
					{
						rang:     makeRange(ks[0], keys[0], keys[0]), // start==end key will not be included
						ts:       lastTs,
						expected: []Record{},
					},
					{
						rang: makeRange(ks[0], "", "zzz"),
						ts:   0, // latest value for all
						expected: []Record{
							makeRecord(ks[0], keys[0], ts[0], FlagEmpty),
							makeRecord(ks[0], keys[1], ts[0], FlagEmpty),
							makeRecord(ks[0], keys[2], ts[0], FlagEmpty),
							makeRecord(ks[0], keys[3], ts[0], FlagEmpty),
						},
					},
					{
						rang: makeRange(ks[2], keys[0], keys[3]), // snapshot read for multiple keys
						ts:   ts[2],
						expected: []Record{
							makeRecord(ks[2], keys[0], ts[2], FlagEmpty),
							makeRecord(ks[2], keys[1], ts[2], FlagEmpty),
							makeRecord(ks[2], keys[2], ts[2], FlagEmpty),
						},
					},
					{
						rang: makeRange(ks[2], keys[0], keys[3]), // snapshot read for multiple keys
						ts:   ts[2] + 1,
						expected: []Record{
							makeRecord(ks[2], keys[0], ts[2], FlagEmpty),
							makeRecord(ks[2], keys[1], ts[2], FlagEmpty),
							makeRecord(ks[2], keys[2], ts[2], FlagEmpty),
						},
					},
					{
						rang:     makeRange(ks[1], "0", "z"), // snapshot read for deleted keys
						ts:       ts[2] - 1,
						expected: []Record{},
					},
					{
						rang:     makeRange(ks[1], "0", "z"), // snapshot read for deleted keys
						ts:       ts[1],
						expected: []Record{},
					},
				}

				for i, c := range cases {
					caseLabel := fmt.Sprintf("test case %d", i)

					// GetRange
					iter, err := db.GetRange(pid, c.ts, c.rang.start, c.rang.end)
					Expect(iter).NotTo(BeNil(), caseLabel)
					Expect(err).To(BeNil(), caseLabel)
					expectIter(iter, caseLabel, c.expected...)

					// ExistsInRange
					Expect(db.ExistsInRange(pid, c.ts, c.rang.start, c.rang.end)).To(Equal(len(c.expected) > 0), caseLabel)
				}
			})

			It("Put should insert value at timestamp", func() {
				timestamp := ts[1] + 10
				record := makeRecord(ks[1], keys[1], timestamp, FlagEmpty)
				expected := []*Record{&record}

				Expect(db.Put(pid, 1, record)).To(BeNil())
				Expect(db.Get(pid, timestamp, record.Address)).To(Equal(expected))
			})
		})
	})
}
