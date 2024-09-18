package storage

import (
	"sync"
	"time"

	"github.com/bcrusu/graph/internal/errors"
	"github.com/bcrusu/graph/internal/logging"
	"github.com/bcrusu/graph/internal/multiraft"
	"github.com/bcrusu/graph/internal/utils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	_    multiraft.FSM = (*FSM)(nil)
	logF               = logging.WithComponent("storage_fsm").NoContext()
)

type FSM struct {
	notifyCh           chan bool         // notify store
	lock               sync.RWMutex      // guards all below
	version            uint64            // used for optimistic concurrency control
	clusterName        string            // read-only cluster name set during bootstrap
	clusterCreatedTime time.Time         // records the time when the cluster was created (UTC)
	partitionCount     uint32            // fixed number of data partitions
	tokens             map[string]uint64 // map[token]server_id: all seen server tokens (could be pruned later)
	servers            *Servers
	partitions         *Partitions
}

func NewFSM() *FSM {
	return &FSM{
		notifyCh: make(chan bool, 1),
		tokens:   map[string]uint64{},
	}
}

func (f *FSM) Apply(index uint64, appendedAt time.Time, data []byte) any {
	log := logF.With("index", index, "appendedAt", appendedAt)

	f.lock.Lock()
	defer f.lock.Unlock()

	cmd, err := utils.UnmarshalProto[Command](data)
	if err != nil {
		log.WithError(err).Debug("UnmarshalProto failed")
		return err
	}

	if cmd.IfMatch != 0 && cmd.IfMatch != f.version {
		log.WithError(err).Debug("Command version check failed", "fsm_version", f.version, "cmd_version", cmd.IfMatch)
		return errors.FailedPrecondition
	}

	payload, err := getPayload(cmd)
	if err != nil {
		log.WithError(err).Debug("getPayload failed")
		return err
	}

	var result any

	log.Debugf("Applying command %T...", payload)

	switch x := payload.(type) {
	case *Bootstrap:
		result, err = f.applyBootstrap(appendedAt, x)
	case *Register:
		result, err = f.applyRegister(appendedAt, x)
	case *UpdateServers:
		result, err = f.applyUpdateServers(appendedAt, x)
	default:
		return errors.Errorf("apply: unhandled payload type %T", payload)
	}

	if err != nil {
		log.WithError(err).Debugf("Applying command %T failed", payload)
		return err
	} else {
		f.version++
		logF.Debugf("Applying command %T success", payload)

		f.notifyStore()
		return result
	}
}

func (f *FSM) Snapshot() ([]byte, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	snap := &Snapshot{
		Version:            f.version,
		ClusterName:        f.clusterName,
		ClusterCreatedTime: timestamppb.New(f.clusterCreatedTime),
		Tokens:             f.tokens,
		Servers:            f.servers,
		Partitions:         f.partitions,
		PartitionCount:     f.partitionCount,
	}

	return utils.MarshalProto(snap)
}

func (f *FSM) Restore(data []byte) error {
	snap, err := utils.UnmarshalProto[Snapshot](data)
	if err != nil {
		return err
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	f.version = snap.Version
	f.clusterName = snap.ClusterName
	f.clusterCreatedTime = snap.ClusterCreatedTime.AsTime()
	f.tokens = snap.Tokens
	f.servers = snap.Servers
	f.partitions = snap.Partitions
	f.partitionCount = snap.PartitionCount

	f.notifyStore()
	return nil
}

func (f *FSM) notifyStore() {
	select {
	case f.notifyCh <- true:
	default:
		logF.Warn("Failed to notify store with latest FSM versions.")
	}
}
