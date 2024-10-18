package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data/client"
	"github.com/bcrusu/scout/internal/data/server/txn"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

// clientRetrier is used to retry only lock failure errors while all the
// other connection-specific and response status-specific retries are
// handled by the rpc connection layer.
type clientRetrier struct {
	client.DataClient
	policy utils.RetryPolicy
}

func (c clientRetrier) Autocommit(ctx context.Context, req *txn.AutocommitRequest, opts ...grpc.CallOption) (*txn.Status, error) {
	var last *txn.Status
	var err error

	utils.RetryContextB(ctx, c.policy, func() error {
		last, err = c.DataClient.Autocommit(ctx, req, opts...)

		// only retry txn status error
		if err != nil || !c.needsRetry(last) {
			return nil
		}

		log.Warn(ctx, "Autocommit failed. Retrying...")
		return errors.Error("Autocommit failed")
	})

	return last, err
}

func (c clientRetrier) Prepare(ctx context.Context, req *txn.PrepareRequest, opts ...grpc.CallOption) (*txn.Status, error) {
	var last *txn.Status
	var err error

	utils.RetryContextB(ctx, c.policy, func() error {
		last, err = c.DataClient.Prepare(ctx, req, opts...)

		// only retry txn status error
		if err != nil || !c.needsRetry(last) {
			return nil
		}

		log.Warn(ctx, "Prepare failed. Retrying...")
		return errors.Error("Prepare failed")
	})

	return last, err
}

func (c clientRetrier) needsRetry(status *txn.Status) bool {
	if status.State != txn.Status_Failed {
		return false
	}

	for _, r := range status.ActionStatus {
		if !c.shouldRetryCode(r.Code) {
			return false
		}
	}

	return true
}

func (c clientRetrier) shouldRetryCode(code txn.ActionStatus_Code) bool {
	return code == txn.ActionStatus_LockFailed
}
