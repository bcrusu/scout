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
	policy    utils.RetryPolicy
	markerErr error
}

func newClientRetrier(client client.DataClient, policy utils.RetryPolicy) clientRetrier {
	return clientRetrier{
		DataClient: client,
		policy:     policy,
		markerErr:  errors.Error("marker retry err"),
	}
}

func (c clientRetrier) Autocommit(ctx context.Context, req *data.AutocommitRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	var status *data.TxnStatus
	var err error

	retryErr := utils.RetryContextE(ctx, c.policy, func() error {
		status, err = c.DataClient.Autocommit(ctx, req, opts...)

		if !c.isLockFailed(status) {
			return nil
		}

		log.WithContext(ctx).WithError(err).Debug("Autocommit failed. Retrying...", "id", req.Txn.Id.LogString())
		return c.markerErr
	})

	if retryErr == nil || retryErr == c.markerErr {
		return status, err
	}

	return nil, retryErr
}

func (c clientRetrier) Prepare(ctx context.Context, req *data.PrepareRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	var status *data.TxnStatus
	var err error

	retryErr := utils.RetryContextE(ctx, c.policy, func() error {
		status, err = c.DataClient.Prepare(ctx, req, opts...)

		if !c.isLockFailed(status) {
			return nil
		}

		log.WithContext(ctx).WithError(err).Debug("Prepare failed. Retrying...", "id", req.Txn.Id.LogString())
		return c.markerErr
	})

	if retryErr == nil || retryErr == c.markerErr {
		return status, err
	}

	return nil, retryErr
}

func (c clientRetrier) Commit(ctx context.Context, req *data.CommitRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	var status *data.TxnStatus
	var err error

	retryErr := utils.RetryContextE(ctx, c.policy, func() error {
		status, err = c.DataClient.Commit(ctx, req, opts...)

		if !errors.Is(err, errors.TimeOutOfRange) {
			return nil
		}

		log.WithContext(ctx).WithError(err).Debug("Commit failed. Retrying...", "id", req.Id.LogString())
		return c.markerErr
	})

	if retryErr == nil || retryErr == c.markerErr {
		return status, err
	}

	return nil, retryErr
}

func (c clientRetrier) isLockFailed(status *data.TxnStatus) bool {
	if status == nil || status.State != data.TxnStatus_Failed {
		return false
	}

	for _, r := range status.ActionStatus {
		if r.Code != data.ActionStatus_LockFailed {
			return false
		}
	}

	return true
}
