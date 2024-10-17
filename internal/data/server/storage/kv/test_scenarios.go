package kv

import (
	"fmt"
	"math"

	"github.com/bcrusu/scout/internal/utils"
	. "github.com/onsi/ginkgo/v2" //lint:ignore ST1001 makes writing tests much easier
	. "github.com/onsi/gomega"    //lint:ignore ST1001 makes writing tests much easier
)

// TestScenarios contains reusable test scenarios that should be executed
// by all KV implementations. These are used for the RocksDB and Inmem packages.
func TestScenarios(db DB) {
	Describe("KV test scenarios", func() {
		keyspace := uint32(101)

		bytes := func(v string) []byte {
			return []byte(v)
		}

		expectEmptyIter := func(iter Iterator) {
			for record, err := range iter {
				Fail(fmt.Sprintf("Range is not empty: iter returned %+v, %s", record, err))
			}
		}

		expectIter := func(iter Iterator, description string, expected ...Record) {
			all := []Record{}

			for kv, err := range iter {
				Expect(err).To(BeNil(), description)
				all = append(all, kv)
			}

			Expect(all).To(Equal(expected), description)
		}

		makeFirstAddress := func(key string) Address {
			return FirstAddressForKey(keyspace, bytes(key))
		}

		makeLastAddress := func(key string) Address {
			return LastAddressForKey(keyspace, bytes(key))
		}

		makeAddress := func(key string, timestamp uint64) Address {
			return NewAddress(keyspace, bytes(key), timestamp)
		}

		makeRecord := func(pid uint32, key string, timestamp uint64) Record {
			return Record{
				Address: makeAddress(key, timestamp),
				Data:    bytes(fmt.Sprintf("data_%d_%s_%d", pid, key, timestamp)),
			}
		}

		Context("When db is empty", func() {
			pid := uint32(1)
			ks := uint32(42)
			addr1 := NewAddress(ks, bytes("abc"), 1000)
			addr2 := NewAddress(ks, bytes("xyz"), 1000)
			record := Record{Address: addr1, Data: bytes("value1")}

			BeforeEach(func() {
				Expect(db.InitPartition(pid)).To(BeNil())
			})

			AfterEach(func() {
				Expect(db.DropPartition(pid)).To(BeNil())
			})

			It("GetRange should return empty iter", func() {
				iter, err := db.GetRange(1, addr1, &addr2)
				Expect(err).To(BeNil())
				expectEmptyIter(iter)
			})

			It("Put should be successful", func() {
				Expect(db.Put(pid, 1, record)).To(BeNil())
				Expect(db.Get(pid, addr1)).To(Equal(&record))
			})

			It("GetIndex should return max index written", func() {
				Expect(db.Put(pid, 1, record)).To(BeNil())
				Expect(db.Put(pid, 2, record)).To(BeNil())
				Expect(db.Put(pid, 7, record)).To(BeNil())
				Expect(db.GetIndex(pid, false)).To(Equal(uint64(7)))
			})

			It("SyncPartition then GetIndex should return max index written to disk", func() {
				Expect(db.Put(pid, 9, record)).To(BeNil())
				Expect(db.SyncPartition(pid)).To(BeNil())
				Expect(db.GetIndex(pid, true)).To(Equal(uint64(9)))
			})
		})

		Context("When db is not empty", func() {
			part := map[uint32][]Record{
				10: {
					makeRecord(10, "777", 1000),
					makeRecord(10, "777", 2000),
					makeRecord(10, "777", 3000),
					makeRecord(10, "ccc", 1000),
					makeRecord(10, "ccc", 2000),
					makeRecord(10, "ttt", 5000),
				},
				99: {
					makeRecord(99, "777", 1000),
					makeRecord(99, "777", 2000),
					makeRecord(99, "777", 3000),
					makeRecord(99, "ccc", 1000),
					makeRecord(99, "ccc", 2000),
					makeRecord(99, "ttt", 5000),
				},
			}

			BeforeEach(func() {
				for pid, record := range part {
					Expect(db.InitPartition(pid)).To(BeNil())
					Expect(db.Put(pid, 1, record...)).To(BeNil())
				}
			})

			AfterEach(func() {
				for pid := range part {
					Expect(db.DropPartition(pid)).To(BeNil())
				}
			})

			It("Get should return correct value", func() {
				cases := []struct {
					pid      uint32
					addr     Address
					expected *Record
				}{
					{
						pid:      10,
						addr:     makeAddress("777", 2000),
						expected: utils.PointerOf(makeRecord(10, "777", 2000)),
					},
					{
						pid:      10,
						addr:     makeAddress("777", 2001),
						expected: nil,
					},
					{
						pid:      99,
						addr:     makeAddress("zzz", 1000),
						expected: nil,
					},
				}

				for i, c := range cases {
					caseLabel := fmt.Sprintf("test case %d", i)
					Expect(db.Get(c.pid, c.addr)).To(Equal(c.expected), caseLabel)
				}
			})

			It("GetRange should return correct values", func() {
				cases := []struct {
					pid      uint32
					start    Address
					end      Address
					expected []Record
				}{
					{
						pid:   10,
						start: makeFirstAddress("777"),
						end:   makeLastAddress("777").Next(),
						expected: []Record{
							makeRecord(10, "777", 3000),
							makeRecord(10, "777", 2000),
							makeRecord(10, "777", 1000),
						},
					},
					{
						pid:      10,
						start:    makeAddress("777", 1100),
						end:      makeAddress("777", 1001),
						expected: []Record{},
					},
					{
						pid:      10,
						start:    makeAddress("777", 1001),
						end:      makeAddress("777", 1000), // exclusive
						expected: []Record{},
					},
					{
						pid:   10,
						start: makeAddress("777", 1001),
						end:   makeAddress("777", 999),
						expected: []Record{
							makeRecord(10, "777", 1000),
						},
					},
					{
						pid:   10,
						start: makeAddress("777", 2001),
						end:   makeAddress("777", 1001),
						expected: []Record{
							makeRecord(10, "777", 2000),
						},
					},
					{
						pid:   10,
						start: makeAddress("777", 3001),
						end:   makeAddress("777", 1001),
						expected: []Record{
							makeRecord(10, "777", 3000),
							makeRecord(10, "777", 2000),
						},
					},
					{
						pid:   10,
						start: makeAddress("777", 3001),
						end:   makeAddress("777", 999),
						expected: []Record{
							makeRecord(10, "777", 3000),
							makeRecord(10, "777", 2000),
							makeRecord(10, "777", 1000),
						},
					},
					{
						pid:   10,
						start: makeAddress("777", 1001),
						end:   makeAddress("ttt", 5000),
						expected: []Record{
							makeRecord(10, "777", 1000),
							makeRecord(10, "ccc", 2000),
							makeRecord(10, "ccc", 1000),
						},
					},
					{
						pid:   10,
						start: makeAddress("ccc", 2000),
						end:   makeAddress("ttt", 4999),
						expected: []Record{
							makeRecord(10, "ccc", 2000),
							makeRecord(10, "ccc", 1000),
							makeRecord(10, "ttt", 5000),
						},
					},
				}

				for i, c := range cases {
					caseLabel := fmt.Sprintf("test case %d", i)

					iter, err := db.GetRange(c.pid, c.start, &c.end)
					Expect(iter).NotTo(BeNil(), caseLabel)
					Expect(err).To(BeNil(), caseLabel)
					expectIter(iter, caseLabel, c.expected...)
				}
			})

			It("GetRange/GetStream should return correct values", func() {
				cases := []struct {
					pid      uint32
					start    Address
					expected []Record
				}{
					{
						pid:   10,
						start: FirstAddress(keyspace),
						expected: []Record{
							makeRecord(10, "777", 3000),
							makeRecord(10, "777", 2000),
							makeRecord(10, "777", 1000),
							makeRecord(10, "ccc", 2000),
							makeRecord(10, "ccc", 1000),
							makeRecord(10, "ttt", 5000),
						},
					},
					{
						pid:   10,
						start: makeAddress("777", math.MaxUint64),
						expected: []Record{
							makeRecord(10, "777", 3000),
							makeRecord(10, "777", 2000),
							makeRecord(10, "777", 1000),
							makeRecord(10, "ccc", 2000),
							makeRecord(10, "ccc", 1000),
							makeRecord(10, "ttt", 5000),
						},
					},
					{
						pid:   10,
						start: makeAddress("777", 3000),
						expected: []Record{
							makeRecord(10, "777", 3000),
							makeRecord(10, "777", 2000),
							makeRecord(10, "777", 1000),
							makeRecord(10, "ccc", 2000),
							makeRecord(10, "ccc", 1000),
							makeRecord(10, "ttt", 5000),
						},
					},
					{
						pid:   10,
						start: makeAddress("777", 1500),
						expected: []Record{
							makeRecord(10, "777", 1000),
							makeRecord(10, "ccc", 2000),
							makeRecord(10, "ccc", 1000),
							makeRecord(10, "ttt", 5000),
						},
					},
					{
						pid:   99,
						start: makeAddress("8", 0),
						expected: []Record{
							makeRecord(99, "ccc", 2000),
							makeRecord(99, "ccc", 1000),
							makeRecord(99, "ttt", 5000),
						},
					},
					{
						pid:   99,
						start: makeAddress("d", 0),
						expected: []Record{
							makeRecord(99, "ttt", 5000),
						},
					},
					{
						pid:      99,
						start:    makeAddress("x", 0),
						expected: []Record{},
					},
				}

				for i, c := range cases {
					caseLabel := fmt.Sprintf("test case %d", i)

					// GetRange with nil end
					iter, err := db.GetRange(c.pid, c.start, nil)
					Expect(iter).NotTo(BeNil(), caseLabel)
					Expect(err).To(BeNil(), caseLabel)
					expectIter(iter, caseLabel, c.expected...)

					// GetStream
					iter, err = db.GetStream(c.pid, c.start)
					Expect(iter).NotTo(BeNil(), caseLabel)
					Expect(err).To(BeNil(), caseLabel)
					expectIter(iter, caseLabel, c.expected...)
				}
			})

			It("Put should insert value at timestamp", func() {
				cases := []struct {
					pid      uint32
					put      []Record
					expected []Record
				}{
					{
						pid: 10,
						put: []Record{
							makeRecord(10, "777", 3500),
							makeRecord(10, "777", 2500),
							makeRecord(10, "777", 500),
							makeRecord(10, "aaa", 1000),
							makeRecord(10, "ccc", 1500),
							makeRecord(10, "zzz", 1000),
						},
						expected: []Record{
							makeRecord(10, "777", 3500),
							makeRecord(10, "777", 3000),
							makeRecord(10, "777", 2500),
							makeRecord(10, "777", 2000),
							makeRecord(10, "777", 1000),
							makeRecord(10, "777", 500),
							makeRecord(10, "aaa", 1000),
							makeRecord(10, "ccc", 2000),
							makeRecord(10, "ccc", 1500),
							makeRecord(10, "ccc", 1000),
							makeRecord(10, "ttt", 5000),
							makeRecord(10, "zzz", 1000),
						},
					},
				}

				for i, c := range cases {
					caseLabel := fmt.Sprintf("test case %d", i)
					Expect(db.Put(c.pid, 1, c.put...)).To(BeNil())

					iter, err := db.GetRange(c.pid, FirstAddress(keyspace), nil)
					Expect(iter).NotTo(BeNil(), caseLabel)
					Expect(err).To(BeNil(), caseLabel)
					expectIter(iter, caseLabel, c.expected...)
				}
			})
		})
	})
}
