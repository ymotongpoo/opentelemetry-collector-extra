// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package switchbotreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/switchbotreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type observation struct {
	value float64
	at    time.Time
}
type measurements map[string]observation

type switchbotReceiver struct {
	cfg           *Config
	settings      receiver.Settings
	next          consumer.Metrics
	obs           *receiverhelper.ObsReport
	mb            *metadata.MetricsBuilder
	cloud         *cloudClient
	mu            sync.Mutex
	devices       map[string]device
	cloudValues   map[string]measurements
	matterValues  map[string]measurements
	available     map[uint64]bool
	cloudAttempts map[string]time.Time
	lastDiscovery time.Time
	cancel        context.CancelFunc
	done          chan struct{}
	stopped       bool
}

func newReceiver(cfg *Config, settings receiver.Settings, next consumer.Metrics) (*switchbotReceiver, error) {
	obs, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{ReceiverID: settings.ID, Transport: "http", ReceiverCreateSettings: settings})
	if err != nil {
		return nil, err
	}
	r := &switchbotReceiver{cfg: cfg, settings: settings, next: next, obs: obs, mb: metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, settings), cloud: newCloudClient(cfg.Cloud), devices: map[string]device{}, cloudValues: map[string]measurements{}, matterValues: map[string]measurements{}, available: map[uint64]bool{}, cloudAttempts: map[string]time.Time{}}
	for _, b := range cfg.Matter.Bindings {
		if r.includes(b.DeviceID) {
			r.devices[deviceKey(b.DeviceID)] = device{ID: b.DeviceID, Name: b.Name, Type: b.DeviceType}
		}
	}
	return r, nil
}

func (r *switchbotReceiver) Start(ctx context.Context, _ component.Host) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil || r.stopped {
		return errors.New("switchbot receiver cannot be started twice")
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	var wg sync.WaitGroup
	launch := func(fn func(context.Context)) { wg.Add(1); go func() { defer wg.Done(); fn(runCtx) }() }
	if r.cfg.Cloud.Enabled {
		launch(r.cloudLoop)
	}
	if r.cfg.Matter.Enabled {
		launch(r.matterLoop)
	}
	launch(func(ctx context.Context) {
		ticker := time.NewTicker(r.cfg.ExportInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if err := r.flush(ctx, now); err != nil && ctx.Err() == nil {
					r.settings.Logger.Warn("SwitchBot metric export failed", zap.Error(err))
				}
			}
		}
	})
	go func() { wg.Wait(); close(r.done) }()
	return nil
}

func (r *switchbotReceiver) Shutdown(ctx context.Context) error {
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

func (r *switchbotReceiver) includes(id string) bool {
	return len(r.cfg.Devices) == 0 || slices.ContainsFunc(r.cfg.Devices, func(x string) bool { return deviceKey(x) == deviceKey(id) })
}

// One worker owns cloud scheduling; requests cannot overlap or multiply after a
// Matter failure. The next scheduling pass starts after the previous pass ends.
func (r *switchbotReceiver) cloudLoop(ctx context.Context) {
	interval := min(r.cfg.Cloud.CollectionInterval, r.cfg.BatteryInterval, r.cfg.Cloud.DiscoveryInterval)
	for {
		r.pollCloud(ctx, time.Now())
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *switchbotReceiver) pollCloud(ctx context.Context, now time.Time) {
	if r.lastDiscovery.IsZero() || now.Sub(r.lastDiscovery) >= r.cfg.Cloud.DiscoveryInterval {
		devices, err := r.cloud.devices(ctx)
		if err != nil {
			r.settings.Logger.Warn("SwitchBot discovery failed", zap.Error(err))
		} else {
			r.mu.Lock()
			seen := map[string]bool{}
			for _, d := range devices {
				if d.ID != "" && r.includes(d.ID) {
					key := deviceKey(d.ID)
					r.devices[key] = d
					seen[key] = true
				}
			}
			for _, b := range r.cfg.Matter.Bindings {
				seen[deviceKey(b.DeviceID)] = true
			}
			for key := range r.devices {
				if !seen[key] {
					delete(r.devices, key)
					delete(r.cloudValues, key)
					delete(r.matterValues, key)
					delete(r.cloudAttempts, key)
				}
			}
			r.mu.Unlock()
			r.lastDiscovery = now
		}
	}
	r.mu.Lock()
	devices := make([]device, 0, len(r.devices))
	for _, d := range r.devices {
		devices = append(devices, d)
	}
	r.mu.Unlock()
	slices.SortFunc(devices, func(a, b device) int { return strings.Compare(a.ID, b.ID) })
	for _, d := range devices {
		if ctx.Err() != nil {
			return
		}
		fields, battery := capabilities(d.Type)
		if len(fields) == 0 && !battery {
			continue
		}
		key := deviceKey(d.ID)
		r.mu.Lock()
		need := false
		for _, field := range fields {
			if !r.matterFresh(key, field, now) {
				need = true
			}
		}
		interval := r.cfg.Cloud.CollectionInterval
		if !need {
			interval = r.cfg.BatteryInterval
			need = battery && !r.matterFresh(key, "battery", now)
		}
		last := r.cloudAttempts[key]
		if !need || (!last.IsZero() && now.Sub(last) < interval) {
			r.mu.Unlock()
			continue
		}
		r.cloudAttempts[key] = now
		r.mu.Unlock()
		body, err := r.cloud.status(ctx, d.ID)
		if err != nil {
			r.settings.Logger.Warn("SwitchBot status unavailable", zap.String("device_id", d.ID), zap.Error(err))
			continue
		}
		values := parseCloud(d.Type, body, time.Now())
		r.mu.Lock()
		r.cloudValues[key] = values
		r.mu.Unlock()
	}
}

func capabilities(kind string) ([]string, bool) {
	switch kind {
	case "Hub 2", "Hub 3":
		return []string{"temperature", "humidity", "light_level"}, false
	case "MeterPro(CO2)", "Meter Pro CO2":
		return []string{"temperature", "humidity", "co2"}, true
	case "Meter", "MeterPlus", "Meter Plus", "MeterPro", "Meter Pro", "WoIOSensor", "Outdoor Meter":
		return []string{"temperature", "humidity"}, true
	case "Bot":
		return nil, true
	default:
		return nil, strings.Contains(kind, "Cam") // Best-effort battery support; missing fields are never zero-filled.
	}
}

func parseCloud(kind string, body map[string]json.RawMessage, now time.Time) measurements {
	result := measurements{}
	fields, battery := capabilities(kind)
	if battery {
		fields = append(slices.Clone(fields), "battery")
	}
	keys := map[string]string{"temperature": "temperature", "humidity": "humidity", "co2": "CO2", "battery": "battery", "light_level": "lightLevel"}
	for _, field := range fields {
		if value, ok := numeric(body[keys[field]]); ok && validValue(field, value) {
			result[field] = observation{value, now}
		}
	}
	var mode, power string
	_ = json.Unmarshal(body["deviceMode"], &mode)
	_ = json.Unmarshal(body["power"], &power)
	if kind == "Bot" && mode == "switchMode" {
		switch strings.ToLower(power) {
		case "on":
			result["power"] = observation{1, now}
		case "off":
			result["power"] = observation{0, now}
		}
	}
	return result
}

func numeric(raw json.RawMessage) (float64, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false
	}
	var value float64
	err := json.Unmarshal(raw, &value)
	return value, err == nil && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validValue(field string, value float64) bool {
	switch field {
	case "temperature":
		return value >= -273.15 && value <= 327.67
	case "battery", "humidity":
		return value >= 0 && value <= 100
	case "co2":
		return value >= 0 && value <= 1000000
	case "light_level":
		return value >= 1 && value <= 20
	default:
		return false
	}
}

func (r *switchbotReceiver) matterFresh(key, field string, now time.Time) bool {
	value, ok := r.matterValues[key][field]
	if !ok || value.at.IsZero() || now.Sub(value.at) > r.maxAge(field, true) {
		return false
	}
	for _, b := range r.cfg.Matter.Bindings {
		if deviceKey(b.DeviceID) == key {
			return r.available[b.NodeID]
		}
	}
	return false
}

func (r *switchbotReceiver) maxAge(field string, matter bool) time.Duration {
	if field == "battery" {
		return 2 * r.cfg.BatteryInterval
	}
	if matter {
		return 2*r.cfg.Matter.CollectionInterval + r.cfg.Matter.Timeout
	}
	return 2*r.cfg.Cloud.CollectionInterval + r.cfg.Cloud.Timeout
}

func (r *switchbotReceiver) metrics(now time.Time) pmetric.Metrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.devices))
	for key := range r.devices {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	ts := pcommon.NewTimestampFromTime(now)
	for _, key := range keys {
		d := r.devices[key]
		available := 0.0
		for _, field := range []string{"temperature", "humidity", "co2", "battery", "light_level", "power"} {
			value, ok := r.cloudValues[key][field]
			ttl := r.maxAge(field, false)
			if r.matterFresh(key, field, now) {
				value = r.matterValues[key][field]
				ok = true
				ttl = r.maxAge(field, true)
			}
			if !ok || value.at.IsZero() {
				continue
			}
			age := max(0, now.Sub(value.at).Seconds())
			r.mb.RecordSwitchbotObservationAgeDataPoint(ts, age, key, d.Name, d.Type, field)
			if now.Sub(value.at) > ttl {
				continue
			}
			available = 1
			switch field {
			case "temperature":
				r.mb.RecordSwitchbotTemperatureDataPoint(ts, value.value, key, d.Name, d.Type)
			case "humidity":
				r.mb.RecordSwitchbotHumidityDataPoint(ts, value.value, key, d.Name, d.Type)
			case "co2":
				r.mb.RecordSwitchbotCo2DataPoint(ts, value.value, key, d.Name, d.Type)
			case "battery":
				r.mb.RecordSwitchbotBatteryDataPoint(ts, value.value, key, d.Name, d.Type)
			case "light_level":
				r.mb.RecordSwitchbotLightLevelDataPoint(ts, value.value, key, d.Name, d.Type)
			case "power":
				r.mb.RecordSwitchbotPowerDataPoint(ts, value.value, key, d.Name, d.Type)
			}
		}
		r.mb.RecordSwitchbotDeviceAvailableDataPoint(ts, available, key, d.Name, d.Type)
		resource := pcommon.NewResource()
		resource.Attributes().PutStr("switchbot.device.id", key)
		resource.Attributes().PutStr("switchbot.device.name", d.Name)
		resource.Attributes().PutStr("switchbot.device.type", d.Type)
		r.mb.EmitForResource(metadata.WithResource(resource))
	}
	return r.mb.Emit()
}

func (r *switchbotReceiver) flush(ctx context.Context, now time.Time) error {
	m := r.metrics(now)
	if m.DataPointCount() == 0 {
		return nil
	}
	ctx = r.obs.StartMetricsOp(ctx)
	count := m.DataPointCount()
	err := r.next.ConsumeMetrics(ctx, m)
	r.obs.EndMetricsOp(ctx, "switchbot", count, err)
	return err
}
