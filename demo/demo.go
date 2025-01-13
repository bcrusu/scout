package main

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
	"github.com/bcrusu/scout/internal/utils"
	"github.com/bcrusu/scout/pkg/client"
	"github.com/bcrusu/scout/pkg/keyvalue"
)

const (
	clusterName = "demo"
	address     = "localhost:11001" // nginx proxy address
	minKeyCount = 5
	maxKeyCount = 50
)

var (
	logLevels = "*:info"
	log       = logging.New("demo")
)

func main() {
	logging.SetLevels(logLevels)
	ctx := context.Background()
	demo := &demo{}

	if err := utils.LifecycleRun(ctx, log, demo); err != nil {
		log.WithError(err).Error("Unexpected error")
		os.Exit(1)
	}
}

type demo struct {
	client     client.Client
	cancelFunc context.CancelFunc
}

func (d *demo) Start(ctx context.Context) error {
	d.client = client.New(
		client.WithClusterName(clusterName),
		client.WithAddress(address))

	if err := d.client.Start(ctx); err != nil {
		return errors.Wrap(err, "failed to start API client")
	}

	d.cancelFunc = utils.RunAsync(ctx, d.runDemo)
	return nil
}

func (d *demo) Stop() {
	d.cancelFunc()
	d.client.Stop()
}

func (d *demo) runDemo(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			d.runOnce(ctx)
		}
	}
}

func (d *demo) runOnce(ctx context.Context) {
	now := time.Now().UnixNano()
	count := minKeyCount + rand.IntN(maxKeyCount-minKeyCount+1)
	kvs := make([]*keyvalue.KeyValue, count)

	for i := range count {
		kvs[i] = &keyvalue.KeyValue{
			Key:   []byte(fmt.Sprintf("key_%d_%d", now, i)),
			Value: []byte(fmt.Sprintf("val_%d_%d", now, i)),
		}
	}

	if d.setKeys(ctx, kvs) {
		d.getAndCheckKeys(ctx, kvs)
	}
}

func (d *demo) setKeys(ctx context.Context, kvs []*keyvalue.KeyValue) bool {
	start := time.Now()
	req := &keyvalue.SetRequest{Items: kvs}

	_, err := d.client.KeyValue().Set(ctx, req)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.WithError(err).Error("Set keys failed.")
		}
		return false
	}

	log.Info("Set request was successful.", "keys", len(kvs), "duration", time.Since(start))
	return true
}

func (d *demo) getAndCheckKeys(ctx context.Context, kvs []*keyvalue.KeyValue) {
	start := time.Now()
	req := &keyvalue.GetRequest{
		Keys: make([][]byte, len(kvs)),
	}

	for i, kv := range kvs {
		req.Keys[i] = kv.Key
	}

	resp, err := d.client.KeyValue().Get(ctx, req)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			log.WithError(err).Error("Get keys failed.")
		}
		return
	}

	if len(resp.Values) != len(kvs) {
		log.WithError(err).Error("Get returned an invalid number of values.", "expected", len(kvs), "actual", len(resp.Values))
		return
	}

	for i, kv := range kvs {
		if !bytes.Equal(kv.Value, resp.Values[i]) {
			log.WithError(err).Errorf("Get returned an invalid value for key %s.", string(kv.Key))
			return
		}
	}

	log.Info("Get request was successful.", "keys", len(kvs), "duration", time.Since(start))
}
