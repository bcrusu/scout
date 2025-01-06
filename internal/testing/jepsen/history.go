package jepsen

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/hlc"
	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/tracing"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	typeInvoke = "invoke" // denotes the start of a logical operation
	typeOK     = "ok"     // denotes its successful completion
	typeFail   = "fail"   // denotes its definite failure
	typeInfo   = "info"   // denotes an indeterminate result
	funcTxn    = "txn"    // defined in https://github.com/jepsen-io/jepsen/tree/main/txn
)

type operation struct {
	Type    string `json:"type"`            // operation type, as defined above
	Func    string `json:"f"`               // is the function being applied, as defined above
	Process any    `json:"process"`         // logical identifier for the process executing the operation
	Index   int    `json:"index"`           // unique, monotonically ascending integer identifying the operation in the history
	Time    int64  `json:"time"`            // time since the start of the test, in nanoseconds
	Error   string `json:"error,omitempty"` // error information, helpful for debugging why an operation returned info or fail.
	Value   any    `json:"value,omitempty"` // stores arguments to and/or return values from the operation with structure as expected by Func
	Trace   string `json:"trace,omitempty"` // custom tracing value
	HLC     uint64 `json:"hlc,omitempty"`   // commit timestamp
}

type txn = []mop  // https://github.com/jepsen-io/jepsen/blob/main/txn/src/jepsen/txn.clj
type mop = [3]any // https://github.com/jepsen-io/jepsen/blob/main/txn/src/jepsen/txn/micro_op.clj

type txnWriter struct {
	history *history
	process any
}

type nemesisWriter struct {
	history *history
	process string
}

func mopWrite(key, value any) mop {
	return mop{"w", key, value}
}

func mopRead(key, value any) mop {
	return mop{"r", key, value}
}

// History persists the operation log as defined in https://github.com/jepsen-io/history.
// It serializes the operations as JSON and later the https://github.com/ligurio/elle-cli
// tool will perform the JSON to EDN conversion.
type history struct {
	lock      sync.Mutex
	index     int
	writer    io.WriteCloser
	startTime time.Time
}

func newHistory(writer io.WriteCloser) *history {
	return &history{
		writer:    writer,
		startTime: time.Now(),
	}
}

func (h *history) Write(ctx context.Context, op operation) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	op.Index = h.index
	op.Time = time.Since(h.startTime).Nanoseconds()
	op.Trace = tracing.GetTraceID(ctx)

	data, err := json.Marshal(op)
	if err != nil {
		return errors.Wrap(err, "json marshal failed")
	}

	if op.Index == 0 {
		if err := h.writeBytes([]byte("[")); err != nil {
			return err
		}
	} else {
		if err := h.writeBytes([]byte(",\n")); err != nil {
			return err
		}
	}

	if err := h.writeBytes(data); err != nil {
		return err
	}

	h.index++
	return nil
}

func (h *history) TxnWriter(workerId int) *txnWriter {
	return &txnWriter{
		history: h,
		process: workerId,
	}
}

func (h *history) NemesisWriter(nemesis string) *nemesisWriter {
	return &nemesisWriter{
		history: h,
		process: nemesis,
	}
}

func (h *history) Close() error {
	if err := h.writeBytes([]byte("]")); err != nil {
		return err
	}

	return h.writer.Close()
}

func (h *history) writeBytes(data []byte) error {
	n, err := h.writer.Write(data)
	if err != nil {
		return errors.Wrap(err, "write failed")
	} else if n != len(data) {
		return errors.Error("detected partial write")
	}
	return nil
}

func (w *txnWriter) Invoke(ctx context.Context, value txn) error {
	return w.history.Write(ctx, operation{
		Type:    typeInvoke,
		Func:    funcTxn,
		Process: w.process,
		Value:   value,
	})
}

func (w *txnWriter) Success(ctx context.Context, value txn, timestamp *timestamppb.Timestamp) error {
	return w.history.Write(ctx, operation{
		Type:    typeOK,
		Func:    funcTxn,
		Process: w.process,
		Value:   value,
		HLC:     hlc.FromTimestamp(timestamp),
	})
}

func (w *txnWriter) Failure(ctx context.Context, value txn, err error) error {
	typ := typeFail
	if errors.IsContextError(err) || errors.Is(err, errors.InternalError) {
		typ = typeInfo
	}

	return w.history.Write(ctx, operation{
		Type:    typ,
		Func:    funcTxn,
		Process: w.process,
		Value:   value,
		Error:   err.Error(),
	})
}

func (w *nemesisWriter) Write(node string, req *agent.NemesisRequest) error {
	type value struct {
		Node     string        `json:"node"`
		Config   agent.Nemesis `json:"config,omitempty"`
		Duration uint32        `json:"duration"`
	}

	return w.history.Write(context.Background(), operation{
		Type:    typeInfo,
		Func:    req.Name(),
		Process: w.process,
		Value: value{
			Node:     node,
			Config:   req.Nemesis(),
			Duration: uint32(req.Duration.AsDuration() / time.Millisecond),
		},
	})
}
