package storage

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/bcrusu/graph/internal/utils"
)

// TODO: make configurable
var (
	retryPolicy = utils.RetryPolicy{
		MaxAttempts: 10,
		Backoff: utils.Backoff{
			MinDelay: 50 * time.Millisecond,
			MaxDelay: 100 * time.Millisecond,
		},
	}
)

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

type Iterator interface {
	//TODO
}

type DB interface {
	Get(Location) ([]byte, uint64, error)
	GetRange(Range) (Iterator, error)
	Exists(Location) (bool, error)
	ExistsInRange(Range) (bool, error)
	Set(Location, []byte) error
	Delete(Location) error
}

// crticalDB wraps the DB object to ensure operation success or total destruction.
type crticalDB struct {
	db DB
}

type crticalIterator struct {
	inner Iterator
}

func (d *crticalDB) Get(loc Location) ([]byte, uint64) {
	var value []byte
	var timestamp uint64
	var err error

	err = utils.RetryE(retryPolicy, func() error {
		value, timestamp, err = d.db.Get(loc)
		return err
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Get failed")
	}

	return value, timestamp
}

func (d *crticalDB) GetRange(rang Range) Iterator {
	iter, err := utils.RetryR(retryPolicy, func() (Iterator, error) {
		return d.db.GetRange(rang)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.GetRange failed")
	}

	return &crticalIterator{
		inner: iter,
	}
}

func (d *crticalDB) Exists(loc Location) bool {
	result, err := utils.RetryR(retryPolicy, func() (bool, error) {
		return d.db.Exists(loc)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Exists failed")
	}

	return result
}

func (d *crticalDB) ExistsInRange(rang Range) bool {
	result, err := utils.RetryR(retryPolicy, func() (bool, error) {
		return d.db.ExistsInRange(rang)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.ExistsInRange failed")
	}

	return result
}

func (d *crticalDB) Set(loc Location, value []byte) {
	err := utils.RetryE(retryPolicy, func() error {
		return d.db.Set(loc, value)
	})

	if err != nil {
		utils.ShutdownNow("crticalDB.Set failed")
	}
}

func (d *crticalDB) Delete(loc Location) {
	err := utils.RetryE(retryPolicy, func() error {
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
