package storage

import (
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/control"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/multiraft"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	_    multiraft.FSM = (*FSM)(nil)
	logF               = logging.New("fsm").NoContext()
)

type FSM struct {
	notifyCh       chan bool    // notify store
	lock           sync.RWMutex // guards all below
	index          uint64       // last applied raft index
	version        uint64       // used for optimistic concurrency control
	clusterName    string       // read-only cluster name set during bootstrap
	createdTime    time.Time    // records the time when the cluster was created (UTC)
	partitionCount uint32       // fixed number of data partitions
	servers        *control.Servers
	partitions     *control.Partitions
	// TODO: store HCL timestamp
}

func NewFSM() *FSM {
	return &FSM{
		notifyCh: make(chan bool, 1),
	}
}

func (f *FSM) Apply(index uint64, appendedAt time.Time, data []byte) any {
	log := logF.With("index", index, "appendedAt", appendedAt)

	f.lock.Lock()
	defer f.lock.Unlock()

	var result any

	if cmd, err := utils.UnmarshalProto[Command](data); err != nil {
		log.WithError(err).Debug("UnmarshalProto failed")
		result = err
	} else {
		result = f.applyCommand(appendedAt, cmd, log)
	}

	f.index = index
	f.notifyStore()
	return result
}

func (f *FSM) applyCommand(appendedAt time.Time, cmd *Command, log logging.LoggerNoContext) any {
	if cmd.IfMatch != 0 && cmd.IfMatch != f.version {
		log.Debug("Command version check failed", "fsm_version", f.version, "cmd_version", cmd.IfMatch)
		return errors.FailedPrecondition
	}

	payload := getPayload(cmd)
	var result any
	var err error

	log.Tracef("Applying command %T...", payload)

	switch x := payload.(type) {
	case *Bootstrap:
		result, err = f.applyBootstrap(appendedAt, x)
	case *Register:
		result, err = f.applyRegister(appendedAt, x)
	case *UpdateStatus:
		f.applyUpdateStatus(x)
	case *InitAssignments:
		result, err = f.applyInitAssignments(appendedAt, x)
	case *UpdateAssignments:
		result, err = f.applyUpdateAssignments(appendedAt, x)
	default:
		return errors.Errorf("apply: unhandled payload type %T", payload)
	}

	if err != nil {
		log.WithError(err).Errorf("Apply command %T failed.", payload)
		return err
	}

	f.version++
	log.Debugf("Applied command %T.", payload)

	return result
}

func (f *FSM) Snapshot() ([]byte, error) {
	c := f.Cluster()
	return utils.MarshalProto(c)
}

func (f *FSM) Restore(data []byte) error {
	c, err := utils.UnmarshalProto[control.Cluster](data)
	if err != nil {
		return err
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	f.version = c.Version
	f.index = c.Index
	f.clusterName = c.Name
	f.createdTime = c.CreatedTime.AsTime()
	f.servers = c.Servers
	f.partitions = c.Partitions
	f.partitionCount = c.PartitionCount

	f.notifyStore()
	return nil
}

func (f *FSM) Cluster() *control.Cluster {
	f.lock.RLock()
	defer f.lock.RUnlock()

	return &control.Cluster{
		Index:          f.index,
		Version:        f.version,
		Name:           f.clusterName,
		CreatedTime:    timestamppb.New(f.createdTime),
		Servers:        f.servers,
		Partitions:     f.partitions,
		PartitionCount: f.partitionCount,
	}
}

func (f *FSM) notifyStore() {
	select {
	case f.notifyCh <- true:
	default:
		logF.Warn("Failed to notify store with latest FSM versions.")
	}
}
