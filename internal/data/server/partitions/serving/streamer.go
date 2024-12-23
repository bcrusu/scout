package serving

import (
	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/server/config"
	"github.com/bcrusu/scout/internal/data/server/storage/kv"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/keyspace"
	"github.com/bcrusu/scout/internal/logging"
	"google.golang.org/grpc"
)

// Streamer is the less-performant way to replicate a partition. It simply iterates
// over all keys and sends them in batches to the new replica. A better way is to use
// the backup/restore or checkpointing approach to copy the SST files directly.
type streamer struct {
	config config.DB
	db     kv.DB
	log    logging.Logger
}

func newStreamer(db kv.DB) *streamer {
	return &streamer{
		config: config.Get().DB,
		db:     db,
		log:    logging.New("streamer"),
	}
}

func (s *streamer) StreamPartition(req *data.StreamRequest, stream grpc.ServerStreamingServer[data.StreamResponse]) error {
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

	log := s.log.WithContext(stream.Context())
	log.Debug("Starting partition stream...", "partition", req.PartitionId, "min_index", req.MinIndex)

	iter, err := s.db.GetStream(req.PartitionId, start)
	if err != nil {
		return err
	}

	records := make([]*data.KVRecord, 0, s.config.MaxStreamingSize)

	for record, err := range iter {
		if err != nil {
			return err
		}

		records = append(records, &data.KVRecord{
			Address: &data.KVAddress{
				Keyspace:  record.Address.Keyspace,
				Key:       record.Address.Key,
				Timestamp: record.Address.Timestamp,
			},
			Data: record.Data,
		})

		if len(records) < s.config.MaxStreamingSize {
			continue
		}

		resp := &data.StreamResponse{
			Records:   records,
			Completed: false,
		}

		if err := stream.Send(resp); err != nil {
			log.WithError(err).Error("Failed to send records.")
			return nil // client will reconnect and request from last received address
		}

		records = records[:0]
	}

	resp := &data.StreamResponse{
		Records:   records,
		Completed: true,
	}

	if err := stream.Send(resp); err != nil {
		log.WithError(err).Error("Failed to send records.")
	}

	return nil
}
