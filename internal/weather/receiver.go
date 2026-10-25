// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package weather

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type Publish func([]Record, time.Time) error
type Collect func(context.Context, time.Time, *Store, Publish) error

type Receiver struct {
	config    Config
	namespace string
	source    string
	settings  receiver.Settings
	next      consumer.Logs
	collect   Collect
	obs       *receiverhelper.ObsReport
	store     *Store
	cancel    context.CancelFunc
	done      chan struct{}
	mu        sync.Mutex
	stopped   bool
}

func NewReceiver(cfg Config, namespace, source string, settings receiver.Settings, next consumer.Logs, collect Collect) (*Receiver, error) {
	if next == nil {
		return nil, errors.New("nil next consumer")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	obs, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{ReceiverID: settings.ID, Transport: "http", ReceiverCreateSettings: settings})
	if err != nil {
		return nil, err
	}
	return &Receiver{config: cfg, namespace: namespace, source: source, settings: settings, next: next, collect: collect, obs: obs}, nil
}

func (r *Receiver) Start(ctx context.Context, _ component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.cancel != nil || r.stopped {
		return errors.New("weather receiver already started or stopped")
	}
	s, err := OpenStore(r.config.StateFile, r.namespace)
	if err != nil {
		return err
	}
	r.store = s
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	go func() {
		defer close(r.done)
		defer r.store.Close()
		for {
			if runCtx.Err() != nil {
				return
			}
			if err := r.poll(runCtx); err != nil && runCtx.Err() == nil {
				r.settings.Logger.Warn("Weather collection failed; pending records will be retried", zap.String("source", r.source), zap.Error(err))
			}
			timer := time.NewTimer(r.config.CollectionInterval)
			select {
			case <-runCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return nil
}

func (r *Receiver) poll(ctx context.Context) error {
	if err := r.flush(ctx); err != nil {
		return err
	}
	err := r.collect(ctx, time.Now().UTC(), r.store, func(records []Record, at time.Time) error {
		if err := r.store.Stage(records, at, r.config.StateTTL); err != nil {
			return err
		}
		return r.flush(ctx)
	})
	if err != nil {
		op := r.obs.StartLogsOp(ctx)
		r.obs.EndLogsOp(op, "weather", 0, err)
	}
	return err
}

func (r *Receiver) flush(ctx context.Context) error {
	events := r.store.Pending()
	if len(events) == 0 {
		return nil
	}
	op := r.obs.StartLogsOp(ctx)
	logs := plog.NewLogs()
	resource := logs.ResourceLogs().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", "weather-"+r.source)
	resource.Resource().Attributes().PutStr("weather.source", r.source)
	scope := resource.ScopeLogs().AppendEmpty()
	scope.Scope().SetName("github.com/ymotongpoo/opentelemetry-collector-extra/receiver/" + r.source + "receiver")
	// The log is a retrieval/change event, not a backdated observation. Refresh
	// its transport timestamp when replaying an old outbox; body IDs remain stable.
	now := time.Now()
	for i, e := range events {
		log := scope.LogRecords().AppendEmpty()
		log.SetTimestamp(pcommon.NewTimestampFromTime(now.Add(time.Duration(i))))
		log.SetObservedTimestamp(pcommon.NewTimestampFromTime(e.RetrievedAt))
		log.SetSeverityNumber(plog.SeverityNumberInfo)
		log.SetSeverityText("INFO")
		body, err := json.Marshal(e)
		if err != nil {
			r.obs.EndLogsOp(op, "weather", 0, err)
			return err
		}
		log.Body().SetStr(string(body))
		log.Attributes().PutStr("weather.location.id", e.LocationID)
		log.Attributes().PutStr("weather.variable", e.Variable)
		log.Attributes().PutStr("weather.event.id", e.EventID)
	}
	err := r.next.ConsumeLogs(op, logs)
	r.obs.EndLogsOp(op, "weather", len(events), err)
	if err != nil {
		return err
	}
	return r.store.Ack()
}

func (r *Receiver) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	r.stopped = true
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
