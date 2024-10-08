package rocksdb

import (
	"fmt"
	"strings"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/linxGnu/grocksdb"
)

const (
	cfNamePrefix  = "part_"
	cfDefaultName = "default"
)

var (
	// each column family is tagged with the partition identifier during init
	keyPartition = kv.EncodeKey(kv.Address{Keyspace: kv.InternalKeyspace, Key: []byte("pid"), Timestamp: 0}, 0)
	// each column family stores the last applied raft index
	keyIndex = kv.EncodeKey(kv.Address{Keyspace: kv.InternalKeyspace, Key: []byte("index"), Timestamp: 0}, 0)
)

type cfMap = map[uint32]*grocksdb.ColumnFamilyHandle
type cfSlice = []*grocksdb.ColumnFamilyHandle

func getCFName(pid uint32) string {
	return fmt.Sprintf("%s%d", cfNamePrefix, pid)
}

func initCF(db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, pid uint32) error {
	wo := makeWriteOptions()
	defer wo.Destroy()

	data := encodeUint32(pid)

	return db.PutCF(wo, cf, keyPartition, data)
}

func probeCF(db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle) (uint32, error) {
	ro := makeReadOptions()
	defer ro.Destroy()

	slice, err := db.GetCF(ro, cf, keyPartition)
	if err != nil {
		return 0, err
	}
	defer slice.Free()

	data := slice.Data()
	if len(data) == 0 {
		return 0, errors.NotFound
	}

	return decodeUint32(data)
}

func probeCFs(db *grocksdb.DB, cfHandles cfSlice) (cfMap, cfSlice, error) {
	known := cfMap{}
	unknown := cfSlice{}

	for _, cf := range cfHandles {
		name := cf.Name()
		pid, err := probeCF(db, cf)

		if err != nil {
			if !errors.Is(err, errors.NotFound) {
				return nil, nil, err
			} else if strings.HasPrefix(name, cfNamePrefix) {
				log.Warn("Probe failed for column family.", "name", name)
			}

			unknown = append(unknown, cf)
			continue
		}

		if x, ok := known[pid]; ok {
			return nil, nil, errors.Errorf("Found multiple column families for partition=%d %s %s.", pid, x.Name(), name)
		}

		known[pid] = cf
	}

	return known, unknown, nil
}

func readCFIndex(db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, tier grocksdb.ReadTier) (uint64, error) {
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetReadTier(tier)
	defer ro.Destroy()

	slice, err := db.GetCF(ro, cf, keyIndex)
	if err != nil {
		return 0, err
	}
	defer slice.Free()

	data := slice.Data()
	if len(data) == 0 {
		return 0, errors.NotFound
	}

	return decodeUint64(data)
}
