package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

var (
	_ data.ServiceClient = (*clientRetrier)(nil)
)

type clientRetrier struct {
	policy utils.RetryPolicy
	inner  data.ServiceClient
}

func (c clientRetrier) Autocommit(ctx context.Context, txn *data.Txn, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	var last *data.TxnStatus
	var err error

	utils.RetryContextB(ctx, c.policy, func() error {
		last, err = c.inner.Autocommit(ctx, txn, opts...)

		// only retry txn status error
		if err != nil || !c.needsRetry(last) {
			return nil
		}

		log.Warn(ctx, "Autocommit failed. Retrying...")
		return errors.Error("Autocommit failed")
	})

	return last, err
}

func (c clientRetrier) Prepare(ctx context.Context, req *data.PrepareRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	var last *data.TxnStatus
	var err error

	utils.RetryContextB(ctx, c.policy, func() error {
		last, err = c.inner.Prepare(ctx, req, opts...)

		// only retry txn status error
		if err != nil || !c.needsRetry(last) {
			return nil
		}

		log.Warn(ctx, "Prepare failed. Retrying...")
		return errors.Error("Prepare failed")
	})

	return last, err
}

func (c clientRetrier) Commit(ctx context.Context, req *data.CommitRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	return c.inner.Commit(ctx, req, opts...)
}

func (c clientRetrier) Abort(ctx context.Context, req *data.AbortRequest, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	return c.inner.Abort(ctx, req, opts...)
}

func (c clientRetrier) StoreDecision(ctx context.Context, dec *data.TxnDecision, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	return c.inner.StoreDecision(ctx, dec, opts...)
}

func (c clientRetrier) needsRetry(status *data.TxnStatus) bool {
	if status.State != data.TxnState_Failed {
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
