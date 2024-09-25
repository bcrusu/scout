package storage

type TxnBatchResult struct {
	Errors map[uint64]error
}
