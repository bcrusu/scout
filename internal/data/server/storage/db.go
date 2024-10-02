package storage

import (
	"encoding/base64"
	"fmt"
	"iter"

	"github.com/bcrusu/graph/internal/utils"
)

type DB interface {
	Get(Location) (*ValueAt, error)
	GetRange(Range) (Iterator, error)
	Exists(Location) (bool, error)
	ExistsInRange(Range) (bool, error)
	Set(Location, []byte) error
	Delete(Location) error
}

type Location struct {
	PartitionID uint32
	Keyspace    uint64
	Key         []byte
	Timestamp   uint64 // optional; if not specified, it represents the latest value
}

type Range struct {
	PartitionID uint32
	Keyspace    uint64
	StartKey    []byte // inclusive
	EndKey      []byte // exclusive
	Timestamp   uint64 // optional; if not specified, it represents the latest value
}

type ValueAt struct {
	Data      []byte
	Timestamp uint64
}

type Iterator = iter.Seq2[ValueAt, error]

// crticalDB wraps the DB object to ensure operation success or total destruction.
type crticalDB struct {
	db          DB
	retryPolicy utils.RetryPolicy
}

func (d *crticalDB) Get(loc Location) *ValueAt {
	value, err := utils.RetryR(d.retryPolicy, func() (*ValueAt, error) {
		return d.db.Get(loc)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Get failed")
	}

	return value
}

func (d *crticalDB) GetRange(rang Range) Iterator {
	iter, err := utils.RetryR(d.retryPolicy, func() (Iterator, error) {
		return d.db.GetRange(rang)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.GetRange failed")
	}

	return iter
}

func (d *crticalDB) Exists(loc Location) bool {
	result, err := utils.RetryR(d.retryPolicy, func() (bool, error) {
		return d.db.Exists(loc)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Exists failed")
	}

	return result
}

func (d *crticalDB) ExistsInRange(rang Range) bool {
	result, err := utils.RetryR(d.retryPolicy, func() (bool, error) {
		return d.db.ExistsInRange(rang)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.ExistsInRange failed")
	}

	return result
}

func (d *crticalDB) Set(loc Location, data []byte) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.Set(loc, data)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Set failed")
	}
}

func (d *crticalDB) Delete(loc Location) {
	err := utils.RetryE(d.retryPolicy, func() error {
		return d.db.Delete(loc)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Delete failed")
	}
}

func (l Location) String() string {
	return fmt.Sprintf("partition=%d keyspace=%d key=%s timestamp=%d",
		l.PartitionID, l.Keyspace, base64.RawURLEncoding.EncodeToString(l.Key), l.Timestamp)
}
