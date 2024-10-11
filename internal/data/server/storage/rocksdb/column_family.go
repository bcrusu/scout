package rocksdb

import (
	"fmt"
	"path"
	"strings"

	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/keyspace"
	"github.com/linxGnu/grocksdb"
)

const (
	cfNamePrefix  = "part_"
	cfDefaultName = "default"
)

var (
	// each column family is tagged with the partition identifier during init
	keyPartition = kv.Address{Keyspace: keyspace.ReservedDB, Key: []byte("pid"), Timestamp: 0}.Encode()
	// each column family stores the last applied raft index for its partition
	keyIndex = kv.Address{Keyspace: keyspace.ReservedDB, Key: []byte("index"), Timestamp: 0}.Encode()
)

type cfMap = map[uint32]*grocksdb.ColumnFamilyHandle
type cfSlice = []*grocksdb.ColumnFamilyHandle

func getCFName(pid uint32) string {
	return fmt.Sprintf("%s%d", cfNamePrefix, pid)
}

func getCFPath(dataDir, name string) string {
	return path.Join(dataDir, name)
}

func initCF(db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, pid uint32) error {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	if err := db.PutCF(wo, cf, keyPartition, encodeUint32(pid)); err != nil {
		return errors.Wrap(err, "failed to put pid key")
	}

	// ensures that the pid key is persisted
	return flushCF(db, cf)
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
				log.WithError(err).Warn("Probe failed for column family.", "name", name)
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

func flushCF(db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle) error {
	opts := grocksdb.NewDefaultFlushOptions()
	opts.SetWait(true)
	defer opts.Destroy()

	if err := db.FlushCF(cf, opts); err != nil {
		return errors.Wrapf(err, "failed to flush column family")
	}

	return nil
}
