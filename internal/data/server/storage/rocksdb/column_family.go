package rocksdb

import (
	"fmt"
	"path"
	"strings"

	"github.com/bcrusu/scout/internal/data/server/config"
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
	keyPartition = encodeKey(keyspace.ReservedDB, []byte("pid"))
	// each column family stores the last applied raft index for its partition
	keyIndex = encodeKey(keyspace.ReservedDB, []byte("index"))
)

type cfMap = map[uint32]*grocksdb.ColumnFamilyHandle
type cfSlice = []*grocksdb.ColumnFamilyHandle

func getCFName(pid uint32) string {
	return fmt.Sprintf("%s%d", cfNamePrefix, pid)
}

func getCFPath(config config.RocksDB, name string) string {
	return path.Join(config.DataDir, name)
}

func initCF(db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, pid uint32) error {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	if err := db.PutCFWithTS(wo, cf, keyPartition, minUint64, encodeUint32(pid)); err != nil {
		return errors.Wrap(err, "failed to put pid key")
	}

	// ensures that the pid key is persisted
	return flushCF(db, cf)
}

func probeCF(db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle) (uint32, error) {
	ro := makeReadOptionsKV()
	defer ro.Destroy()

	slice, err := db.GetCF(ro, cf, keyPartition)
	if err != nil {
		return 0, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return 0, errors.NotFound
	}

	return decodeUint32(slice.Data())
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
			return nil, nil, errors.Errorf("Found multiple column families for partition %d %s %s.", pid, x.Name(), name)
		}

		known[pid] = cf
	}

	return known, unknown, nil
}

func readCFIndex(db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, tier grocksdb.ReadTier) (uint64, error) {
	ro := makeReadOptionsKV()
	ro.SetReadTier(tier)
	defer ro.Destroy()

	slice, err := db.GetCF(ro, cf, keyIndex)
	if err != nil {
		return 0, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return 0, errors.NotFound
	}

	return decodeUint64(slice.Data())
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
