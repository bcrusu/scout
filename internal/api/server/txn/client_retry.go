package txn

import (
	"context"

	"github.com/bcrusu/scout/internal/data"
	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/utils"
	"google.golang.org/grpc"
)

type clientRetrier struct {
	data.ServiceClient
	policy utils.RetryPolicy
}

func (c clientRetrier) Autocommit(ctx context.Context, txn *data.Txn, opts ...grpc.CallOption) (*data.TxnStatus, error) {
	var last *data.TxnStatus
	var err error

	utils.RetryContextB(ctx, c.policy, func() error {
		last, err = c.ServiceClient.Autocommit(ctx, txn, opts...)

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
		last, err = c.ServiceClient.Prepare(ctx, req, opts...)

		// only retry txn status error
		if err != nil || !c.needsRetry(last) {
			return nil
		}

		log.Warn(ctx, "Prepare failed. Retrying...")
		return errors.Error("Prepare failed")
	})

	return last, err
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
