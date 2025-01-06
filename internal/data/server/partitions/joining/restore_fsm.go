package joining

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

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
	addressCheckpoint = kv.NewAddress(keyspace.ReservedReplica, []byte("checkpoint"), 42)
)

// restoreFsm helps the joining replica seed its initial state by streaming
// the partition key-value db contents from a up-to-date sponsor replica.
// After the restore completes successfuly, it waits forever inside the Apply
// method so no Raft log entries are applied. Once the operation is complete,
// it notifies the replica to update its status to joining==done. This eventually
// leads the control plane to transition the replica from joining to serving state.
//
// To note that, if the Raft log is empty (i.e. no logs were written) the FSM cannot
// make progress as the Apply and Restore methods will never be called by Raft, thus
// the FSM will be forever stuck waiting for the first write. This scenario is bound
// to happen early during cluster setup phase when new servers are added which trigger
// replica rebalance/s resulting in the newly joining replicas getting stuck.
// The fix relies on the fact that the partition leader will issue the UpdateTimestamp
// command in the absence of write transactions.
type restoreFsm struct {
	ctx        context.Context
	pid        uint32
	replica    string
	config     config.DB
	dataClient data.ServiceClient
	db         *kv.DBBreaker
	log        logging.Logger
	candidates atomic.Pointer[candidates] // updated by the joining replica
	index      atomic.Uint64              // updated during restore
	ready      atomic.Bool                // updated during restore
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
	return &restoreFsm{
		ctx:        ctx,
		pid:        pid,
		replica:    replica,
		config:     config.Get().DB,
		dataClient: dataClient,
		db:         kv.NewDBBreaker(db),
		log:        logging.New("replica_joining").With("pid", pid, "replica", replica).WithContext(ctx),
	}
}

func (f *restoreFsm) Apply(index uint64, _ time.Time, _ []byte) any {
	// ready can be already true if a snapshot was applied first
	if !f.ready.Load() {
		if err := f.restoreAt(index - 1); err != nil {
			// err != nil only if the operation was halted
			return err
		}
	}

	f.log.Info("Restore partition completed. Waiting for transition to serving state...")

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

	return f.restoreAt(snap.Index)
}

func (f *restoreFsm) restoreAt(minIndex uint64) error {
	f.log.Info("Restoring...", "min_index", minIndex)

	f.db.InitPartition(f.pid)

	lastAddr, completed := f.loadCheckpoint(minIndex)

	if !completed {
		if err := f.streamPartition(minIndex, lastAddr); err != nil {
			return err
		}
	}

	f.index.Store(minIndex)
	f.ready.Store(true)
	f.log.Info("Restore partition completed.")
	return nil
}

func (f *restoreFsm) loadCheckpoint(minIndex uint64) (*data.KVAddress, bool) {
	record := f.db.Get(f.pid, addressCheckpoint)
	if record == nil {
		return nil, false
	}

	var chk checkpoint
	if err := json.Unmarshal(record.Data, &chk); err != nil {
		f.log.WithError(err).Error("Failed to unmarshal checkpoint. Dropping past progress...")
		return nil, false
	}

	if chk.MinIndex < minIndex {
		// Past progress needs to be discarded because we received a newer Raft snapshot.
		// This signals that the previous restore was halted and before it was resumed
		// a new Raft snapshot happened. For now will handle this scenario by adjusting
		// the shapshot interval config, but later might need to have existing replicas
		// pause snapshotting while the new replica is joining.

		f.log.Warn("Received newer snapshot. Dropping past progress...")
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
		if c := f.candidates.Load(); c != nil && len(c.replicas) > 0 {
			r := c.nextReplica()
			log := f.log.With("source_replica", r.replica.Name, "source_server_id", r.replica.ServerId)
			log.Debug("Streaming...")

			var completed bool
			lastAddr, completed = f.tryStreamPartition(minIndex, lastAddr, r.replica.ServerId, log)
			if completed {
				return nil
			} else if lastAddr != nil {
				log.Debug("Streaming halted.")
			}
		} else {
			f.log.Debug("No streaming candidates.")
		}

		select {
		case <-f.ctx.Done():
			return errors.Error("stream partition halted")
		case <-time.After(utils.AddJitter(f.config.StreamingThrottle)):
		}
	}
}

func (f *restoreFsm) tryStreamPartition(minIndex uint64, lastAddr *data.KVAddress, serverId uint64, log logging.Logger) (*data.KVAddress, bool) {
	req := &data.StreamRequest{
		PartitionId:  f.pid,
		MinIndex:     minIndex,
		StartAddress: lastAddr,
	}

	ctx := client.WithPreferredServer(f.ctx, serverId, true)
	stream, err := f.dataClient.StreamPartition(ctx, req)
	if err != nil {
		log.WithError(err).Error("StreamPartition call failed.")
		return lastAddr, false
	}

	records := make([]kv.Record, 0, f.config.MaxStreamingSize+1)

	for {
		res, err := stream.Recv()
		if err != nil {
			log.WithError(err).Error("Stream.Recv failed.")
			return lastAddr, false
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
			return nil, true
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

func newKVAddress(addr kv.Address) *data.KVAddress {
	return &data.KVAddress{
		Keyspace:  addr.Keyspace,
		Key:       addr.Key,
		Timestamp: addr.Timestamp,
	}
}
