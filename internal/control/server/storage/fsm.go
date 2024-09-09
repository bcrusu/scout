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

const (
	MaxClusterNameLen = 100
	MaxAddressLen     = 128
	MaxTokenLen       = 1024
)

var (
	_   multiraft.FSM = (*FSM)(nil)
	_   Store         = (*FSM)(nil)
	log               = logging.WithComponent("control_storage").NoContext()
)

// Store exposes read-only operations executed directly on the FSM backing storage.
type Store interface {
	IsEmpty() bool
	ClusterName() string
}

type FSM struct {
	lock            sync.Mutex
	index           uint64               // last applied raft log index
	clusterName     string               // read-only cluster name set during bootstrap
	createdTime     time.Time            // records the time when the cluster was created (UTC)
	lastServerID    uint64               // ensures unique server id generation
	tokens          map[string]uint64    // map[token]server_id: all seen server tokens (could be pruned based on first seen ts)
	serverNames     map[uint64]string    // map[server_id]name: unique server name
	serverFirstSeen map[uint64]time.Time // map[server_id]Time: time of server registration (UTC)
	serverLastSeen  map[uint64]time.Time // map[server_id]Time: time of server last activity (UTC). Writes should be throttled.
}

func NewFSM() *FSM {
	return &FSM{
		tokens:          map[string]uint64{},
		serverNames:     map[uint64]string{},
		serverFirstSeen: map[uint64]time.Time{},
		serverLastSeen:  map[uint64]time.Time{},
	}
}

func (f *FSM) Apply(index uint64, appendedAt time.Time, data []byte) any {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.index = index

	cmd, err := utils.UnmarshalProto[Command](data)
	if err != nil {
		return err
	}

	payload, err := getPayload(cmd)
	if err != nil {
		return err
	}

	log := log.With("index", index, "appendedAt", appendedAt)
	var result any

	log.Debugf("Applying command %T...", payload)

	switch x := payload.(type) {
	case *Bootstrap:
		result, err = f.applyBootstrap(appendedAt, x)
	case *Register:
		result, err = f.applyRegister(appendedAt, x)
	default:
		return errors.Errorf("apply: unhandled payload type %T", payload)
	}

	if err != nil {
		log.WithError(err).Debugf("Applying command %T failed", payload)
		return err
	} else {
		log.Debugf("Applying command %T success", payload)
		return result
	}
}

func (f *FSM) Snapshot() ([]byte, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	snap := &Snapshot{
		Index:           f.index,
		ClusterName:     f.clusterName,
		CreatedTime:     timestamppb.New(f.createdTime),
		LastServerId:    f.lastServerID,
		Tokens:          f.tokens,
		ServerNames:     f.serverNames,
		ServerFirstSeen: utils.TimeMapToProto(f.serverFirstSeen),
		ServerLastSeen:  utils.TimeMapToProto(f.serverLastSeen),
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

	f.index = snap.Index
	f.clusterName = snap.ClusterName
	f.createdTime = snap.CreatedTime.AsTime()
	f.lastServerID = snap.LastServerId
	f.tokens = snap.Tokens
	f.serverNames = snap.ServerNames
	f.serverFirstSeen = utils.TimeMapFromProto(snap.ServerFirstSeen)
	f.serverLastSeen = utils.TimeMapFromProto(snap.ServerLastSeen)

	return nil
}

func (f *FSM) ClusterName() string {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.clusterName
}

func (f *FSM) IsEmpty() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.clusterName == "" || f.createdTime.IsZero()
}
