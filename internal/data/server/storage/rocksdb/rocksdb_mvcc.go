package rocksdb

import (
	"math"
	"slices"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/data/server/storage/mvcc"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/linxGnu/grocksdb"
)

var (
	_ mvcc.DB = (*rocksMVCC)(nil)
)

type rocksMVCC struct {
	rdb *RocksDB
}

func newRocksMVCC(rdb *RocksDB) *rocksMVCC {
	return &rocksMVCC{
		rdb: rdb,
	}
}

func (r *rocksMVCC) Get(pid uint32, timestamp uint64, addrs ...mvcc.Address) ([]*mvcc.Record, error) {
	cf := r.rdb.getCF(pid)
	if cf == nil {
		return nil, errors.NotFound
	}

	if timestamp == 0 {
		timestamp = math.MaxUint64
	}

	keys := make([][]byte, len(addrs))
	for i, addr := range addrs {
		keys[i] = r.encodeKey(addr)
	}

	ro := makeReadOptionsMVCC()
	ro.SetTimestamp(encodeUint64(timestamp))
	defer ro.Destroy()

	datas, timestamps, err := r.rdb.db.MultiGetCFWithTS(ro, cf, keys...)
	if err != nil {
		return nil, err
	}
	defer datas.Destroy()
	defer timestamps.Destroy()

	result := make([]*mvcc.Record, len(addrs))

	for i, addr := range addrs {
		data := datas[i]
		if !data.Exists() {
			continue
		}

		value, flags := mvcc.DecodeData(slices.Clone(data.Data()))
		if flags.Tombstone() {
			continue
		}

		result[i] = &mvcc.Record{
			Address:   addr,
			Timestamp: errors.Assert2(decodeUint64(timestamps[i].Data())),
			Value:     value,
			Flags:     flags,
		}
	}

	return result, nil
}

func (r *rocksMVCC) GetRange(pid uint32, timestamp uint64, start, end mvcc.Address) (mvcc.Iterator, error) {
	cf := r.rdb.getCF(pid)
	if cf == nil {
		return nil, errors.NotFound
	}

	if timestamp == 0 {
		timestamp = math.MaxUint64
	}

	startKey := r.encodeKey(start)
	endKey := r.encodeKey(end)

	return func(yield func(mvcc.Record, error) bool) {
		ro := makeReadOptionsMVCC()
		ro.SetTimestamp(encodeUint64(timestamp))
		ro.SetIterateUpperBound(endKey)
		defer ro.Destroy()

		it := r.rdb.db.NewIteratorCF(ro, cf)
		defer it.Close()

		it.Seek(startKey)

		for ; it.Valid(); it.Next() {
			data := it.Value()
			ts := it.Timestamp()

			value, flags := mvcc.DecodeData(slices.Clone(data.Data()))
			if flags.Tombstone() {
				continue
			}

			record := mvcc.Record{
				Address:   r.decodeAddressFromIter(it),
				Timestamp: errors.Assert2(decodeUint64(ts.Data())),
				Value:     value,
				Flags:     flags,
			}

			data.Free()
			ts.Free()

			if !yield(record, nil) {
				return
			}
		}

		if err := it.Err(); err != nil {
			yield(mvcc.Record{}, err)
		}
	}, nil
}

func (r *rocksMVCC) Exists(pid uint32, timestamp uint64, addr mvcc.Address) (bool, error) {
	v, err := r.Get(pid, timestamp, addr)
	if err != nil {
		return false, err
	}
	return v[0] != nil, nil
}

func (r *rocksMVCC) ExistsInRange(pid uint32, timestamp uint64, start, end mvcc.Address) (bool, error) {
	iter, err := r.GetRange(pid, timestamp, start, end)
	if err != nil {
		return false, err
	}

	for range iter {
		return true, nil
	}

	return false, nil
}

func (r *rocksMVCC) Put(pid uint32, index uint64, records ...mvcc.Record) error {
	kvRecords := make([]kv.Record, len(records))
	for i, record := range records {
		kvRecords[i] = record.ToKVRecord()
	}

	return r.rdb.kv.Put(pid, index, kvRecords...)
}

func (r *rocksMVCC) encodeKey(a mvcc.Address) []byte {
	return encodeKey(a.Keyspace, a.Key)
}

func (r *rocksMVCC) decodeAddressFromIter(iter *grocksdb.Iterator) mvcc.Address {
	return r.decodeAddress(iter.Key())
}

func (r *rocksMVCC) decodeAddress(keySlice *grocksdb.Slice) mvcc.Address {
	keyspace, key := decodeKey(keySlice.Data())
	addr := mvcc.NewAddress(keyspace, key)

	keySlice.Free()
	return addr
}
