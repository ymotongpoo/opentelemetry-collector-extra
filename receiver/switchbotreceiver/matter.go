// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package switchbotreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var matterScales = map[string]float64{"temperature": 0.01, "humidity": 0.01, "battery": 0.5, "co2": 1}

type matterNode struct {
	ID         uint64                     `json:"node_id"`
	Available  bool                       `json:"available"`
	Attributes map[string]json.RawMessage `json:"attributes"`
}

type matterMessage struct {
	ID     string          `json:"message_id"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error_code"`
	Event  string          `json:"event"`
	Data   json.RawMessage `json:"data"`
}

type pendingRead struct {
	node  uint64
	paths []string
	at    time.Time
}

func (r *switchbotReceiver) matterLoop(ctx context.Context) {
	for {
		err := r.matterSession(ctx)
		r.mu.Lock()
		clear(r.available)
		clear(r.matterValues)
		r.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		r.settings.Logger.Warn("Matter Server disconnected; cloud schedule is unchanged", zap.Error(err))
		timer := time.NewTimer(min(5*time.Second, r.cfg.Matter.CollectionInterval))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *switchbotReceiver) matterSession(ctx context.Context) error {
	dialer := websocket.Dialer{HandshakeTimeout: r.cfg.Matter.Timeout}
	conn, response, err := dialer.DialContext(ctx, r.cfg.Matter.Endpoint, nil)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		return errors.New("cannot connect to Matter Server")
	}
	defer conn.Close()
	conn.SetReadLimit(4 * 1024 * 1024)
	// Close also during the initial server_info read if shutdown is requested.
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-watchDone:
		}
	}()
	_ = conn.SetReadDeadline(time.Now().Add(r.cfg.Matter.Timeout))
	var info struct {
		Schema  int `json:"schema_version"`
		Minimum int `json:"min_supported_schema_version"`
	}
	if err := conn.ReadJSON(&info); err != nil {
		return errors.New("Matter Server handshake failed")
	}
	if info.Schema < 11 || info.Minimum > 14 {
		return errors.New("unsupported Matter Server schema")
	}
	_ = conn.SetReadDeadline(time.Time{})
	write := func(v any) error {
		_ = conn.SetWriteDeadline(time.Now().Add(r.cfg.Matter.Timeout))
		return conn.WriteJSON(v)
	}
	if err := write(map[string]any{"message_id": "initial", "command": "start_listening"}); err != nil {
		return err
	}
	messages := make(chan matterMessage, 32)
	readErrors := make(chan error, 1)
	readDone := make(chan struct{})
	readerCtx, cancelReader := context.WithCancel(ctx)
	defer func() { cancelReader(); conn.Close(); <-readDone }()
	go func() {
		defer close(readDone)
		for {
			var msg matterMessage
			if err := conn.ReadJSON(&msg); err != nil {
				readErrors <- errors.New("Matter Server read failed")
				return
			}
			select {
			case messages <- msg:
			case <-readerCtx.Done():
				return
			}
		}
	}()
	nodes := map[uint64]matterNode{}
	pending := map[string]pendingRead{}
	sequence := 0
	initialized := false
	started := time.Now()
	poll := func() error {
		grouped := map[uint64][]string{}
		for _, b := range r.cfg.Matter.Bindings {
			if !r.includes(b.DeviceID) {
				continue
			}
			for metric, path := range b.Attributes {
				if metric == "battery" {
					r.mu.Lock()
					last := r.matterValues[deviceKey(b.DeviceID)][metric].at
					r.mu.Unlock()
					if !last.IsZero() && time.Since(last) < r.cfg.BatteryInterval {
						continue
					}
				}
				grouped[b.NodeID] = append(grouped[b.NodeID], path)
				if metric == "co2" {
					prefix := path[:strings.LastIndex(path, "/")+1]
					grouped[b.NodeID] = append(grouped[b.NodeID], prefix+"8", prefix+"9")
				}
			}
			if b.ReachabilityAttribute != "" {
				grouped[b.NodeID] = append(grouped[b.NodeID], b.ReachabilityAttribute)
			}
		}
		ids := make([]uint64, 0, len(grouped))
		for id := range grouped {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			already := false
			for _, request := range pending {
				if request.node == id {
					already = true
				}
			}
			if already {
				continue
			}
			paths := grouped[id]
			slices.Sort(paths)
			paths = slices.Compact(paths)
			sequence++
			messageID := strconv.Itoa(sequence)
			if err := write(map[string]any{"message_id": messageID, "command": "read_attribute", "args": map[string]any{"node_id": id, "attribute_path": paths}}); err != nil {
				return err
			}
			pending[messageID] = pendingRead{id, paths, time.Now()}
		}
		return nil
	}
	ticker := time.NewTicker(r.cfg.Matter.CollectionInterval)
	defer ticker.Stop()
	health := time.NewTicker(min(r.cfg.Matter.Timeout, r.cfg.Matter.CollectionInterval))
	defer health.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErrors:
			return err
		case <-ticker.C:
			if !initialized {
				continue
			}
			if err := poll(); err != nil {
				return err
			}
		case now := <-health.C:
			if !initialized && now.Sub(started) >= r.cfg.Matter.Timeout {
				return errors.New("Matter Server did not send its node snapshot")
			}
			for id, request := range pending {
				if now.Sub(request.at) >= r.cfg.Matter.Timeout {
					delete(pending, id)
					r.invalidateMatter(request.node)
				}
			}
		case msg := <-messages:
			if msg.ID == "initial" {
				if len(msg.Error) != 0 {
					return errors.New("Matter Server refused start_listening")
				}
				var initial []matterNode
				if json.Unmarshal(msg.Result, &initial) != nil {
					return errors.New("invalid Matter node snapshot")
				}
				for _, node := range initial {
					nodes[node.ID] = node
					r.setMatterAvailability(node.ID, node.Available)
				}
				initialized = true
				if err := poll(); err != nil {
					return err
				}
				continue
			}
			if request, ok := pending[msg.ID]; ok {
				delete(pending, msg.ID)
				var attributes map[string]json.RawMessage
				if len(msg.Error) != 0 || json.Unmarshal(msg.Result, &attributes) != nil {
					r.invalidateMatter(request.node)
					continue
				}
				node := nodes[request.node]
				if node.Attributes == nil {
					node.Attributes = map[string]json.RawMessage{}
				}
				for _, path := range request.paths {
					delete(node.Attributes, path)
				}
				for path, raw := range attributes {
					node.Attributes[path] = raw
				}
				nodes[request.node] = node
				r.applyMatter(request.node, node.Attributes, request.paths, time.Now())
				continue
			}
			switch msg.Event {
			case "node_added", "node_updated":
				var node matterNode
				if json.Unmarshal(msg.Data, &node) == nil {
					nodes[node.ID] = node
					r.setMatterAvailability(node.ID, node.Available)
					r.pruneMatter(node.ID, node.Attributes)
				}
			case "node_removed":
				var id uint64
				if json.Unmarshal(msg.Data, &id) == nil {
					delete(nodes, id)
					r.setMatterAvailability(id, false)
				}
			case "attribute_updated":
				var parts []json.RawMessage
				if json.Unmarshal(msg.Data, &parts) != nil || len(parts) != 3 {
					continue
				}
				var id uint64
				var path string
				if json.Unmarshal(parts[0], &id) != nil || json.Unmarshal(parts[1], &path) != nil {
					continue
				}
				node := nodes[id]
				if node.Attributes == nil {
					node.Attributes = map[string]json.RawMessage{}
				}
				node.Attributes[path] = parts[2]
				nodes[id] = node
				r.applyMatter(id, node.Attributes, []string{path}, time.Now())
			}
		}
	}
}

func (r *switchbotReceiver) invalidateMatter(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range r.cfg.Matter.Bindings {
		if b.NodeID == id {
			delete(r.matterValues, deviceKey(b.DeviceID))
		}
	}
}

func (r *switchbotReceiver) setMatterAvailability(id uint64, available bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.available[id] = available
	if !available {
		for _, b := range r.cfg.Matter.Bindings {
			if b.NodeID == id {
				delete(r.matterValues, deviceKey(b.DeviceID))
			}
		}
	}
}

// A node snapshot is a cache. It may invalidate old values, but never makes
// them fresh. Only a device read or a measurement notification does that.
func (r *switchbotReceiver) pruneMatter(id uint64, attributes map[string]json.RawMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range r.cfg.Matter.Bindings {
		if b.NodeID == id {
			if b.ReachabilityAttribute != "" {
				var reachable bool
				if json.Unmarshal(attributes[b.ReachabilityAttribute], &reachable) != nil || !reachable {
					delete(r.matterValues, deviceKey(b.DeviceID))
					continue
				}
			}
			for field, path := range b.Attributes {
				if raw := attributes[path]; len(raw) == 0 || string(raw) == "null" {
					delete(r.matterValues[deviceKey(b.DeviceID)], field)
				}
			}
		}
	}
}

func (r *switchbotReceiver) applyMatter(id uint64, attributes map[string]json.RawMessage, changed []string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.available[id] {
		return
	}
	for _, b := range r.cfg.Matter.Bindings {
		if b.NodeID != id || !r.includes(b.DeviceID) {
			continue
		}
		key := deviceKey(b.DeviceID)
		if b.ReachabilityAttribute != "" {
			var reachable bool
			if json.Unmarshal(attributes[b.ReachabilityAttribute], &reachable) != nil || !reachable {
				delete(r.matterValues, key)
				continue
			}
		}
		if r.matterValues[key] == nil {
			r.matterValues[key] = measurements{}
		}
		for field, path := range b.Attributes {
			if !slices.Contains(changed, path) {
				if field == "co2" {
					prefix := path[:strings.LastIndex(path, "/")+1]
					if slices.Contains(changed, prefix+"8") || slices.Contains(changed, prefix+"9") {
						delete(r.matterValues[key], field)
					}
				}
				continue
			}
			value, ok := numeric(attributes[path])
			value *= matterScales[field]
			if field == "co2" {
				prefix := path[:strings.LastIndex(path, "/")+1]
				unit, uok := numeric(attributes[prefix+"8"])
				medium, mok := numeric(attributes[prefix+"9"])
				if !uok || !mok || medium != 0 {
					ok = false
				}
				switch unit {
				case 0:
				case 1:
					value /= 1000
				case 2:
					value /= 1000000
				default:
					ok = false
				}
			}
			if !ok || !validValue(field, value) {
				delete(r.matterValues[key], field)
				continue
			}
			r.matterValues[key][field] = observation{value, now}
		}
	}
}

// Validate standard measurement clusters rather than allowing incompatible
// units (for example, mapping measured lux to a proprietary light level).
func validateMatterMetric(metric, path string) error {
	parts := strings.Split(path, "/")
	if len(parts) != 3 {
		return errors.New("invalid attribute path")
	}
	want := map[string]string{"temperature": "1026/0", "humidity": "1029/0", "battery": "47/12", "co2": "1037/0"}
	if parts[1]+"/"+parts[2] != want[metric] {
		return fmt.Errorf("%s must use standard Matter attribute %s", metric, want[metric])
	}
	return nil
}
