package jepsen

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/bcrusu/scout/internal/tracing"
	"github.com/bcrusu/scout/pkg/client"
	"github.com/bcrusu/scout/pkg/keyvalue"
	"golang.org/x/time/rate"
)

type worker struct {
	runId    int
	workerId int
	client   client.Client
	limiter  *rate.Limiter
	workload *workload
	history  *txnWriter
}

func (w *worker) Run(stopCh chan any) error {
	counter := 0
	for {
		if !w.limiter.Allow() {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		select {
		case <-stopCh:
			return nil
		default:
		}

		counter++
		trace := fmt.Sprintf("r%dw%dc%d", w.runId, w.workerId, counter)
		ctx := tracing.WithTraceID(context.Background(), trace)

		switch req := w.workload.Next().(type) {
		case *keyvalue.GetRequest:
			if err := w.handleGetRequest(ctx, req); err != nil {
				return err
			}
		case *keyvalue.SetRequest:
			if err := w.handleSetRequest(ctx, req); err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("unhandled request type %T", req))
		}
	}
}

func (w *worker) handleGetRequest(ctx context.Context, req *keyvalue.GetRequest) error {
	txn := w.readTxn(req.Keys)

	if err := w.history.Invoke(ctx, txn); err != nil {
		return err
	}

	resp, err := w.client.KeyValue().Get(ctx, req)
	if err != nil {
		herr := w.history.Failure(ctx, txn, err)
		return errors.Join(err, herr)
	}

	txn = w.readResultTxn(req.Keys, resp.Values)
	return w.history.Success(ctx, txn, resp.Timestamp)
}

func (w *worker) handleSetRequest(ctx context.Context, req *keyvalue.SetRequest) error {
	txn := w.writeTxn(req.Items)

	if err := w.history.Invoke(ctx, txn); err != nil {
		return err
	}

	resp, err := w.client.KeyValue().Set(ctx, req)
	if err != nil {
		herr := w.history.Failure(ctx, txn, err)
		return errors.Join(err, herr)
	}

	return w.history.Success(ctx, txn, resp.Timestamp)
}

func (w *worker) readTxn(keys [][]byte) txn {
	txn := make(txn, len(keys))
	for i, key := range keys {
		k := w.fmtBytes(key)
		txn[i] = mopRead(k, nil)
	}
	return txn
}

func (w *worker) readResultTxn(keys [][]byte, values [][]byte) txn {
	txn := make(txn, len(keys))
	for i, key := range keys {
		k := w.fmtBytes(key)

		if value := values[i]; len(value) > 0 {
			v := w.fmtBytes(values[i])
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
		k := w.fmtBytes(kv.Key)
		v := w.fmtBytes(kv.Value)
		txn[i] = mopWrite(k, v)
	}
	return txn
}

// formats the value using the same base64 encoding as in data service logs
// to make it easy to correlate the test run history and logs entries.
func (w *worker) fmtBytes(bytes []byte) string {
	return base64.RawURLEncoding.EncodeToString(bytes)
}
