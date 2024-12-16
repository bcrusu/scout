package jepsen

import (
	"context"
	"fmt"
	"time"

	"github.com/bcrusu/scout/pkg/client"
	"github.com/bcrusu/scout/pkg/keyvalue"
	"golang.org/x/time/rate"
)

type worker struct {
	client   client.Client
	limiter  *rate.Limiter
	workload *workload
	history  *txnWriter
}

func (w *worker) Run(stopCh chan any) error {
	for {
		if !w.limiter.Allow() {
			time.After(10 * time.Millisecond)
			continue
		}

		select {
		case <-stopCh:
			return nil
		default:
		}

		switch req := w.workload.Next().(type) {
		case nil:
			return nil
		case *keyvalue.GetRequest:
			if err := w.handleGetRequest(req); err != nil {
				return err
			}
		case *keyvalue.SetRequest:
			if err := w.handleSetRequest(req); err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("unhandled request type %T", req))
		}
	}
}

func (w *worker) handleGetRequest(req *keyvalue.GetRequest) error {
	txn := w.readTxn(req.Keys)

	if err := w.history.Invoke(txn); err != nil {
		return err
	}

	resp, err := w.client.KeyValue().Get(context.Background(), req)
	if err != nil {
		return w.history.Failure(txn, err)
	}

	return w.history.Success(w.readResultTxn(req.Keys, resp.Values))
}

func (w *worker) handleSetRequest(req *keyvalue.SetRequest) error {
	txn := w.writeTxn(req.Items)

	if err := w.history.Invoke(txn); err != nil {
		return err
	}

	_, err := w.client.KeyValue().Set(context.Background(), req)
	if err != nil {
		return w.history.Failure(txn, err)
	}

	return w.history.Success(txn)
}

func (w *worker) readTxn(keys [][]byte) txn {
	txn := make(txn, len(keys))
	for i, key := range keys {
		k := decodeKey(key)
		txn[i] = mopRead(k, nil)
	}
	return txn
}

func (w *worker) readResultTxn(keys [][]byte, values [][]byte) txn {
	txn := make(txn, len(keys))
	for i, key := range keys {
		k := decodeKey(key)

		if value := values[i]; len(value) > 0 {
			v := decodeValue(values[i])
			txn[i] = mopRead(k, v)
		} else {
			txn[i] = mopRead(k, nil)
		}
	}
	return txn
}

func (w *worker) writeTxn(kvs []*keyvalue.KeyValue) txn {
	txn := make(txn, len(kvs))
	for i, kv := range kvs {
		k := decodeKey(kv.Key)
		v := decodeValue(kv.Value)
		txn[i] = mopWrite(k, v)
	}
	return txn
}
