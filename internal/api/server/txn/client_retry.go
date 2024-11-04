package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/data/client"
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

func (c clientRetrier) Autocommit(ctx context.Context, req *data.AutocommitRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	var last *data.TxnStatus
	var lastErr error

	err := utils.RetryContextE(ctx, c.policy, func() error {
		last, lastErr = c.DataClient.Autocommit(ctx, req, opts...)

		// only retry txn status error
		if lastErr != nil || !c.needsRetry(last) {
			return nil
		}

		log.WithContext(ctx).Warn("Autocommit failed. Retrying...")
		return errors.Error("Autocommit failed")
	})

	if err != nil {
		return nil, err
	}

	return last, lastErr
}

func (c clientRetrier) Prepare(ctx context.Context, req *data.PrepareRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	var last *data.TxnStatus
	var lastErr error

	err := utils.RetryContextE(ctx, c.policy, func() error {
		last, lastErr = c.DataClient.Prepare(ctx, req, opts...)

		// only retry txn status error
		if lastErr != nil || !c.needsRetry(last) {
			return nil
		}

		log.WithContext(ctx).Warn("Prepare failed. Retrying...")
		return errors.Error("Prepare failed")
	})

	if err != nil {
		return nil, err
	}

	return last, lastErr
}

func (c clientRetrier) needsRetry(status *data.TxnStatus) bool {
	if status.State != data.TxnStatus_Failed {
		return false
	}

	for _, r := range status.ActionStatus {
		if !c.shouldRetryCode(r.Code) {
			return false
		}
	}

	return true
}

func (c clientRetrier) shouldRetryCode(code data.ActionStatus_Code) bool {
	return code == data.ActionStatus_LockFailed
}
