package shared

import (
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/keyspace"
	"github.com/bcrusu/scout/internal/logging"
	"google.golang.org/grpc"
)

// PartitionStreamer is the less-performant way to replicate a partition. It simply iterates
// over all keys and sends them in batches to the new replica. A better way is to use
// the backup/restore or checkpointing approach to copy the SST files directly.
type PartitionStreamer struct {
	config config.DB
	db     kv.DB
	log    logging.Logger
}

func NewPartitionStreamer(db kv.DB) *PartitionStreamer {
	return &PartitionStreamer{
		config: config.Get().DB,
		db:     db,
		log:    logging.WithComponent("streamer"),
	}
}

func (s *PartitionStreamer) StreamPartition(req *data.StreamRequest, stream grpc.ServerStreamingServer[data.StreamResponse]) error {
	if index, err := s.db.GetIndex(req.PartitionId, false); err != nil {
		return err
	} else if index < req.MinIndex {
		return errors.FailedPrecondition
	}

	start := kv.FirstAddress(keyspace.FirstUserKeyspace)
	if req.StartAddress != nil {
		if req.StartAddress.Keyspace < keyspace.FirstUserKeyspace {
			return errors.InvalidRequest
		}

		start = req.StartAddress.Address()
	}

	iter, err := s.db.GetStream(req.PartitionId, start)
	if err != nil {
		return err
	}

	entries := make([]*data.KVEntry, 0, s.config.MaxStreamingSize)

	for entry, err := range iter {
		if err != nil {
			return err
		}

		entries = append(entries, &data.KVEntry{
			Address: &data.KVAddress{
				Keyspace:  entry.Address.Keyspace,
				Key:       entry.Address.Key,
				Timestamp: entry.Address.Timestamp,
			},
			Data: entry.Data,
		})

		if len(entries) < s.config.MaxStreamingSize {
			continue
		}

		resp := &data.StreamResponse{
			Entries:   entries,
			Completed: false,
		}

		if err := stream.Send(resp); err != nil {
			s.log.WithError(err).Error(stream.Context(), "Failed to send entries.")
			return nil // client will reconnect and request from last received address
		}

		entries = entries[:0]
	}

	resp := &data.StreamResponse{
		Entries:   entries,
		Completed: true,
	}

	if err := stream.Send(resp); err != nil {
		s.log.WithError(err).Error(stream.Context(), "Failed to send entries.")
	}

	return nil
}
