// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package smartheimreceiver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/smartheimreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type smartheimReceiver struct {
	cfg                     *Config
	settings                receiver.Settings
	next                    consumer.Metrics
	obs                     *receiverhelper.ObsReport
	mb                      *metadata.MetricsBuilder
	client, discoveryClient *http.Client
	// The collection goroutine owns endpoint, deviceID and discovery state.
	endpoint, deviceID string
	lastDiscovery      time.Time
	failures           int
	mu                 sync.Mutex
	cancel             context.CancelFunc
	done               chan struct{}
	stopped            bool
}

func newReceiver(cfg *Config, settings receiver.Settings, next consumer.Metrics) (*smartheimReceiver, error) {
	obs, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{ReceiverID: settings.ID, Transport: "http", ReceiverCreateSettings: settings})
	if err != nil {
		return nil, err
	}
	return &smartheimReceiver{cfg: cfg, settings: settings, next: next, obs: obs, mb: metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, settings), client: newHTTPClient(cfg.Timeout, cfg.Timeout), discoveryClient: newHTTPClient(cfg.Discovery.Timeout, 250*time.Millisecond), endpoint: cfg.Endpoint, deviceID: cfg.DeviceID}, nil
}

func (r *smartheimReceiver) Start(ctx context.Context, _ component.Host) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil || r.stopped {
		return errors.New("smartheim receiver cannot be started twice")
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	go func() {
		defer close(r.done)
		defer r.client.CloseIdleConnections()
		defer r.discoveryClient.CloseIdleConnections()
		for {
			if err := r.collect(runCtx); err != nil && runCtx.Err() == nil {
				r.settings.Logger.Warn("Smart Heim Navi collection failed", zap.Error(err))
			}
			// Wait after each collection; slow responses never overlap requests.
			timer := time.NewTimer(r.cfg.CollectionInterval)
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

func (r *smartheimReceiver) Shutdown(ctx context.Context) error {
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

func (r *smartheimReceiver) locate(ctx context.Context) error {
	if r.endpoint != "" {
		return nil
	}
	if !r.lastDiscovery.IsZero() && time.Since(r.lastDiscovery) < r.cfg.Discovery.Interval {
		return errors.New("waiting for next discovery attempt")
	}
	r.lastDiscovery = time.Now()
	networks, err := discoveryNetworks(r.cfg.Discovery.Networks)
	if err != nil {
		return err
	}
	endpoint, err := discover(ctx, r.discoveryClient, candidateEndpoints(networks), r.deviceID, r.cfg.Discovery.Concurrency)
	if err != nil {
		return err
	}
	r.endpoint = endpoint
	r.settings.Logger.Info("Discovered Smart Heim Navi device", zap.String("endpoint", endpoint))
	return nil
}

func (r *smartheimReceiver) collect(ctx context.Context) (err error) {
	ctx = r.obs.StartMetricsOp(ctx)
	points := 0
	defer func() { r.obs.EndMetricsOp(ctx, "", points, err) }()
	if err = r.locate(ctx); err != nil {
		return err
	}
	s, err := readSummary(ctx, r.client, r.endpoint)
	if err == nil && r.deviceID != "" && s.id != r.deviceID {
		err = fmt.Errorf("device number mismatch: expected %q, received %q", r.deviceID, s.id)
	}
	if err != nil {
		r.failures++
		if r.failures >= 3 && r.cfg.Endpoint == "" {
			r.endpoint = ""
			r.failures = 0
		}
		return err
	}
	r.failures = 0
	// Preserve identity across DHCP changes, even without configured device_id.
	r.deviceID = s.id
	now := pcommon.NewTimestampFromTime(time.Now())
	r.mb.RecordSmartheimSolarPowerDataPoint(now, s.generation)
	r.mb.RecordSmartheimConsumptionPowerDataPoint(now, s.consumption)
	if s.hasGrid {
		r.mb.RecordSmartheimGridImportPowerDataPoint(now, s.gridImport)
		r.mb.RecordSmartheimGridExportPowerDataPoint(now, s.gridExport)
	}
	if r.cfg.CollectBranches {
		root, e := fetchPage(ctx, r.client, r.endpoint, branchesPath)
		if e == nil {
			id, branches, parseErr := parseBranches(root)
			e = parseErr
			if e == nil && id != s.id {
				e = errors.New("summary and branch pages report different device numbers")
			}
			if e == nil {
				branchTime := pcommon.NewTimestampFromTime(time.Now())
				for _, b := range branches {
					r.mb.RecordSmartheimBranchPowerDataPoint(branchTime, b.power, b.id, b.name)
				}
			}
		}
		if e != nil {
			err = fmt.Errorf("branches: %w", e)
		}
	}
	rb := r.mb.NewResourceBuilder()
	rb.SetSmartheimDeviceID(s.id)
	r.mb.EmitForResource(metadata.WithResource(rb.Emit()))
	md := r.mb.Emit()
	points = md.DataPointCount()
	if points > 0 {
		err = errors.Join(err, r.next.ConsumeMetrics(ctx, md))
	}
	return err
}
