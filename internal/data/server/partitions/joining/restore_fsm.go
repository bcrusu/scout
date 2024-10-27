package joining

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/keyspace"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
)

var (
	_ multiraft.FSM = (*restoreFsm)(nil)
	// Used to save progress and resume from last ingested address during restore.
	// The value is stored next to the data to ensure atomicity: iif data was persisted
	// successfully then the checkpoint is also persisted.
	addressCheckpoint = kv.NewAddress(keyspace.ReservedReplica, []byte("checkpoint"), 0)
)

// restoreFsm helps the joining replica seed its initial state by streaming
// the partition key-value db contents from a up-to-date sponsor replica.
// After the restore completes successfuly, it waits forever inside the Apply
// method so no Raft log entries are applied. Once the operation is complete,
// it notifies the replica to update its status to joining==done. This eventually
// leads the control plane to transition the replica from joining to serving state.
type restoreFsm struct {
	ctx        context.Context
	pid        uint32
	replica    string
	config     config.DB
	dataClient data.ServiceClient
	db         *kv.DBBreaker
	log        logging.Logger
	candidates atomic.Pointer[candidates]                             // updated by the joining replica
	index      atomic.Uint64                                          // updated during restore
	status     atomic.Pointer[control.DataServerStatus_JoiningStatus] // updated during restore
}

type address struct {
	Keyspace  uint32 `json:"keyspace"`
	Key       []byte `json:"key"`
	Timestamp uint64 `json:"timestamp"`
}

type checkpoint struct {
	MinIndex    uint64   `json:"minIndex"`
	LastAddress *address `json:"lastAddress"`
	Completed   bool     `json:"completed"`
}

func newRestoreFsm(pid uint32, ctx context.Context, replica string, dataClient data.ServiceClient, db kv.DB) *restoreFsm {
	f := &restoreFsm{
		ctx:        ctx,
		pid:        pid,
		replica:    replica,
		config:     config.Get().DB,
		dataClient: dataClient,
		db:         kv.NewDBBreaker(db),
		log:        logging.WithComponent("replica_joining").With("partition", pid, "replica", replica),
	}

	f.setStatus(false)
	return f
}

func (f *restoreFsm) Apply(_ uint64, _ time.Time, _ []byte) any {
	f.log.Info(f.ctx, "Restore partition completed. Waiting for transition to serving state...")

	// wait forever as noted above
	<-f.ctx.Done()
	return nil
}

func (f *restoreFsm) Snapshot() ([]byte, error) {
	panic(fmt.Sprintf("unexpected Snapshot while restoring partition %d replica %s.", f.pid, f.replica))
}

func (f *restoreFsm) Restore(snapshot []byte) error {
	snap, err := utils.UnmarshalProto[storage.Snapshot](snapshot)
	if err != nil {
		return err
	}

	f.index.Store(snap.Index)

	f.db.InitPartition(f.pid)

	lastAddr, completed := f.loadCheckpoint(snap.Index)

	if !completed {
		if err := f.streamPartition(snap.Index, lastAddr); err != nil {
			return err
		}
	}

	f.setStatus(true)
	f.log.Info(f.ctx, "Restore partition completed.")
	return nil
}

func (f *restoreFsm) loadCheckpoint(minIndex uint64) (*data.KVAddress, bool) {
	record := f.db.Get(f.pid, addressCheckpoint)
	if record == nil {
		return nil, false
	}

	var chk checkpoint
	errors.Assert(json.Unmarshal(record.Data, &chk))

	if chk.MinIndex < minIndex {
		// Past progress needs to be discarded because we received a newer Raft snapshot.
		// This signals that the previous restore was halted and before it was resumed
		// a new Raft snapshot happened. For now will handle this scenario by adjusting
		// the shapshot interval config, but later might need to have existing replicas
		// pause snapshotting while the new replica is joining.

		f.log.Warn(f.ctx, "Received newer snapshot. Dropping past progress...")
		f.db.DropPartition(f.pid)
		return nil, false
	}

	if chk.Completed {
		return nil, true
	} else if chk.LastAddress == nil {
		return nil, false
	}

	addr := kv.NewAddress(chk.LastAddress.Keyspace, chk.LastAddress.Key, chk.LastAddress.Timestamp)
	addr = addr.Next() // continue from next

	return newKVAddress(addr), false
}

func (f *restoreFsm) streamPartition(minIndex uint64, lastAddr *data.KVAddress) error {
	for {
		c := f.candidates.Load()
		if c == nil || len(c.replicas) == 0 {
			time.Sleep(time.Second)
			continue
		}

		r := c.nextReplica()
		f.log.Debug(f.ctx, "Replica selected for streaming.", "replica", r.replica.Name, "server_id", r.replica.ServerId)

		lastAddr = f.tryStreamPartition(minIndex, lastAddr, r.replica.ServerId)
		if lastAddr != nil {
			f.log.Debug(f.ctx, "Streaming halted.", "replica", r.replica.Name, "server_id", r.replica.ServerId)
		} else {
			// stream completed
			return nil
		}

		select {
		case <-f.ctx.Done():
			return errors.Error("stream partition halted")
		case <-time.After(utils.AddJitter(f.config.StreamingThrottle)):
		}
	}
}

func (f *restoreFsm) tryStreamPartition(minIndex uint64, lastAddr *data.KVAddress, serverId uint64) *data.KVAddress {
	req := &data.StreamRequest{
		PartitionId:  f.pid,
		MinIndex:     minIndex,
		StartAddress: lastAddr,
	}

	ctx := client.WithPreferredServer(f.ctx, serverId, true)
	stream, err := f.dataClient.StreamPartition(ctx, req)
	if err != nil {
		f.log.WithError(err).Error(f.ctx, "StreamPartition call failed.")
		return lastAddr
	}

	records := make([]kv.Record, 0, f.config.MaxStreamingSize+1)

	for {
		res, err := stream.Recv()
		if err != nil {
			f.log.WithError(err).Error(f.ctx, "Stream.Recv failed.")
			return lastAddr
		}

		for _, e := range res.Records {
			records = append(records, e.Record())
		}

		if len(records) > 0 {
			last := utils.SliceLast(records)
			lastAddr = newKVAddress(last.Address)
		} else {
			lastAddr = nil
		}

		// checkpoint is persisted in the same batch
		records = append(records, kv.Record{
			Address: addressCheckpoint,
			Data:    f.newCheckpoint(minIndex, lastAddr, res.Completed),
		})

		if res.Completed {
			f.db.Put(f.pid, minIndex, records...)
			return nil
		} else {
			f.db.Put(f.pid, 0, records...)
		}

		records = records[:0]
	}
}

func (f *restoreFsm) newCheckpoint(minIndex uint64, lastAddress *data.KVAddress, completed bool) []byte {
	chk := checkpoint{
		MinIndex:  minIndex,
		Completed: completed,
	}

	if lastAddress != nil {
		chk.LastAddress = &address{
			Keyspace:  lastAddress.Keyspace,
			Key:       lastAddress.Key,
			Timestamp: lastAddress.Timestamp,
		}
	}

	return errors.Assert2(json.Marshal(chk))
}

func (f *restoreFsm) setStatus(completed bool) {
	f.status.Store(&control.DataServerStatus_JoiningStatus{
		Completed: completed,
	})
}

func newKVAddress(addr kv.Address) *data.KVAddress {
	return &data.KVAddress{
		Keyspace:  addr.Keyspace,
		Key:       addr.Key,
		Timestamp: addr.Timestamp,
	}
}
