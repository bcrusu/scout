package txn

import "github.com/bcrusu/scout/internal/errors"

func (r *AutocommitRequest) IsSnapshotRead() bool {
	return r.ReadTimestamp != 0 && r.Txn.IsReadOnly()
}

func (r *AutocommitRequest) Validate() error {
	if r == nil {
		return errors.Error("AutocommitRequest is nil")
	}
	if err := r.Txn.Validate(); err != nil {
		return errors.Wrap(err, "AutocommitRequest.Txn is invalid")
	}
	if r.PartitionId != r.Txn.Id.PrincipalPid {
		return errors.Error("AutocommitRequest.PartitionId is invalid")
	}
	if r.ReadTimestamp != 0 && !r.Txn.IsReadOnly() {
		return errors.Error("AutocommitRequest.ReadTimestamp invalid for read-write txn")
	}
	return nil
}

func (r *PrepareRequest) Validate() error {
	if r == nil {
		return errors.Error("PrepareRequest is nil")
	}
	if err := r.Txn.Validate(); err != nil {
		return errors.Wrap(err, "PrepareRequest.Txn is invalid")
	}
	return nil
}

func (r *CommitRequest) Validate() error {
	if r == nil {
		return errors.Error("CommitRequest is nil")
	}
	if err := r.Id.Validate(); err != nil {
		return errors.Wrap(err, "PrepareRequest.Id is invalid")
	}
	if r.CommitTimestamp == 0 {
		return errors.Error("CommitRequest has missing fields")
	}
	return nil
}

func (r *AbortRequest) Validate() error {
	if r == nil {
		return errors.Error("AbortRequest is nil")
	}
	if err := r.Id.Validate(); err != nil {
		return errors.Wrap(err, "AbortRequest.Id is invalid")
	}
	return nil
}
