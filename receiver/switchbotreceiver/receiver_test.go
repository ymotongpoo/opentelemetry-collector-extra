// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package switchbotreceiver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/switchbotreceiver/internal/metadata"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func testConfig() *Config {
	c := createDefaultConfig().(*Config)
	c.Cloud.Token = "fixture-token"
	c.Cloud.Secret = "fixture-secret"
	c.Cloud.MinRequestInterval = 0
	return c
}

func testReceiver(t *testing.T, c *Config) *switchbotReceiver {
	t.Helper()
	r, err := newReceiver(c, receivertest.NewNopSettings(metadata.Type), consumertest.NewNop())
	require.NoError(t, err)
	return r
}

func points(m pmetric.Metrics, name string) []float64 {
	var result []float64
	for i := 0; i < m.ResourceMetrics().Len(); i++ {
		scopes := m.ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < scopes.Len(); j++ {
			metrics := scopes.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				if metric.Name() == name {
					for p := 0; p < metric.Gauge().DataPoints().Len(); p++ {
						result = append(result, metric.Gauge().DataPoints().At(p).DoubleValue())
					}
				}
			}
		}
	}
	return result
}

func TestCloudSigningCooldownAndRedirect(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		require.Equal(t, http.MethodGet, req.Method)
		require.Equal(t, "fixture-token", req.Header.Get("Authorization"))
		_, err := uuid.Parse(req.Header.Get("nonce"))
		require.NoError(t, err)
		ms, err := strconv.ParseInt(req.Header.Get("t"), 10, 64)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), time.UnixMilli(ms), time.Second)
		mac := hmac.New(sha256.New, []byte("fixture-secret"))
		_, _ = mac.Write([]byte("fixture-token" + req.Header.Get("t") + req.Header.Get("nonce")))
		require.Equal(t, base64.StdEncoding.EncodeToString(mac.Sum(nil)), req.Header.Get("sign"))
		w.Header().Set("Retry-After", "600")
		w.WriteHeader(429)
	}))
	defer server.Close()
	cfg := testConfig().Cloud
	cfg.Endpoint = server.URL
	client := newCloudClient(cfg)
	_, err := client.devices(context.Background())
	require.Error(t, err)
	_, err = client.devices(context.Background())
	require.ErrorIs(t, err, errQuota)
	require.Equal(t, int32(1), calls.Load())
	var leaked atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { leaked.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { http.Redirect(w, req, target.URL, http.StatusFound) }))
	defer redirect.Close()
	cfg.Endpoint = redirect.URL
	client = newCloudClient(cfg)
	_, err = client.devices(context.Background())
	require.ErrorContains(t, err, "302")
	require.Zero(t, leaked.Load())
}

func TestCloudQuotaCancellationAndBodyFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"statusCode":190,"body":{}}`)) }))
	defer server.Close()
	cfg := testConfig().Cloud
	cfg.Endpoint = server.URL
	cfg.DailyRequestLimit = 1
	client := newCloudClient(cfg)
	_, err := client.devices(context.Background())
	require.ErrorContains(t, err, "190")
	_, err = client.devices(context.Background())
	require.ErrorIs(t, err, errQuota)
	client.requests = []time.Time{time.Now().Add(-25 * time.Hour)}
	require.NoError(t, client.reserve(context.Background()), "only requests within the last 24 hours consume the budget")
	require.ErrorIs(t, client.reserve(context.Background()), errQuota)
	client = newCloudClient(testConfig().Cloud)
	client.next = time.Now().Add(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, client.reserve(ctx), context.Canceled)
}

func TestMissingNullZeroAndBotMode(t *testing.T) {
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(`{"temperature":0,"humidity":null,"battery":0,"CO2":-1}`), &raw))
	values := parseCloud("MeterPro(CO2)", raw, time.Now())
	require.Len(t, values, 2)
	require.Zero(t, values["temperature"].value)
	require.Zero(t, values["battery"].value)
	require.NoError(t, json.Unmarshal([]byte(`{"deviceMode":"pressMode","power":"on","battery":9}`), &raw))
	require.NotContains(t, parseCloud("Bot", raw, time.Now()), "power")
	raw["deviceMode"] = json.RawMessage(`"switchMode"`)
	require.Equal(t, 1.0, parseCloud("Bot", raw, time.Now())["power"].value)
	raw["deviceMode"] = json.RawMessage(`"customizeMode"`)
	require.NotContains(t, parseCloud("Bot", raw, time.Now()), "power")
}

func TestCloudPartialFailureCadenceAndBatteryReuse(t *testing.T) {
	counts := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		counts[req.URL.Path]++
		switch req.URL.Path {
		case "/devices":
			_, _ = w.Write([]byte(`{"statusCode":100,"body":{"deviceList":[{"deviceId":"meter","deviceType":"WoIOSensor"},{"deviceId":"bot","deviceType":"Bot"},{"deviceId":"bad","deviceType":"Bot"}]}}`))
		case "/devices/bad/status":
			_, _ = w.Write([]byte(`{"statusCode":161,"body":{}}`))
		default:
			_, _ = w.Write([]byte(`{"statusCode":100,"body":{"temperature":22.5,"humidity":45,"battery":60}}`))
		}
	}))
	defer server.Close()
	cfg := testConfig()
	cfg.Cloud.Endpoint = server.URL
	r := testReceiver(t, cfg)
	base := time.Now()
	r.pollCloud(context.Background(), base)
	r.pollCloud(context.Background(), base.Add(time.Minute))
	r.pollCloud(context.Background(), base.Add(5*time.Minute))
	require.Equal(t, 2, counts["/devices/meter/status"])
	require.Equal(t, 1, counts["/devices/bot/status"])
	require.Equal(t, 1, counts["/devices/bad/status"])
	r.mu.Lock()
	require.Equal(t, 60.0, r.cloudValues["BOT"]["battery"].value)
	require.Empty(t, r.cloudValues["BAD"])
	r.mu.Unlock()
	// A fully covered Matter sensor does not cause an extra cloud poll.
	cfg.Matter.Bindings = []Binding{{DeviceID: "meter", NodeID: 1}}
	r.mu.Lock()
	r.available[1] = true
	r.matterValues["METER"] = measurements{"temperature": {23, base.Add(10 * time.Minute)}, "humidity": {40, base.Add(10 * time.Minute)}, "battery": {50, base.Add(10 * time.Minute)}}
	r.mu.Unlock()
	r.pollCloud(context.Background(), base.Add(10*time.Minute))
	require.Equal(t, 2, counts["/devices/meter/status"])
	// Matter failure cannot accelerate the cloud's configured poll interval.
	r.invalidateMatter(1)
	r.pollCloud(context.Background(), base.Add(10*time.Minute+time.Second))
	require.Equal(t, 3, counts["/devices/meter/status"])
	r.pollCloud(context.Background(), base.Add(11*time.Minute))
	require.Equal(t, 3, counts["/devices/meter/status"])
}

func TestMetricsPreferMatterAndExpireWithoutZeroFilling(t *testing.T) {
	cfg := testConfig()
	cfg.Matter.Bindings = []Binding{{DeviceID: "meter", NodeID: 1}}
	r := testReceiver(t, cfg)
	now := time.Now()
	r.devices["METER"] = device{ID: "meter", Name: "Room", Type: "WoIOSensor"}
	r.available[1] = true
	r.cloudValues["METER"] = measurements{"temperature": {24, now}, "humidity": {0, now}, "battery": {60, now.Add(-2 * time.Hour)}}
	r.matterValues["METER"] = measurements{"temperature": {21, now}}
	m := r.metrics(now)
	require.Equal(t, []float64{21}, points(m, "switchbot.temperature"))
	require.Equal(t, []float64{0}, points(m, "switchbot.humidity"))
	require.Equal(t, []float64{60}, points(m, "switchbot.battery"))
	require.Equal(t, []float64{24}, points(r.metrics(now.Add(3*time.Minute)), "switchbot.temperature"))
	m = r.metrics(now.Add(7 * time.Hour))
	require.Empty(t, points(m, "switchbot.temperature"))
	require.Empty(t, points(m, "switchbot.battery"))
	require.Equal(t, []float64{0}, points(m, "switchbot.device.available"))
}

func TestShutdownCancelsBlockedCloudRequest(t *testing.T) {
	entered := make(chan struct{}, 1)
	released := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/devices" {
			_, _ = w.Write([]byte(`{"statusCode":100,"body":{"deviceList":[{"deviceId":"meter","deviceType":"Meter"}]}}`))
			return
		}
		entered <- struct{}{}
		<-req.Context().Done()
		close(released)
	}))
	defer server.Close()
	cfg := testConfig()
	cfg.Cloud.Endpoint = server.URL
	r := testReceiver(t, cfg)
	require.NoError(t, r.Start(context.Background(), nil))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("status request did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(ctx))
	select {
	case <-released:
	case <-ctx.Done():
		t.Fatal("HTTP request was not cancelled")
	}
	require.NoError(t, r.Shutdown(ctx))
	require.Error(t, r.Start(context.Background(), nil))
}

func TestMatterSnapshotReadNotificationAndCancellation(t *testing.T) {
	const nodeID uint64 = 9007199254740993
	readStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	sendNull := make(chan struct{})
	serverDone := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(serverDone)
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"schema_version": 14, "min_supported_schema_version": 11})
		var start map[string]any
		if conn.ReadJSON(&start) != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"message_id": "initial", "result": []any{map[string]any{"node_id": nodeID, "available": true, "attributes": map[string]any{"1/1026/0": 9900, "2/1029/0": 9900}}}})
		var read struct {
			ID      string `json:"message_id"`
			Command string `json:"command"`
			Args    struct {
				Node  uint64   `json:"node_id"`
				Paths []string `json:"attribute_path"`
			} `json:"args"`
		}
		if conn.ReadJSON(&read) != nil {
			return
		}
		require.Equal(t, nodeID, read.Args.Node)
		require.Equal(t, "read_attribute", read.Command)
		readStarted <- struct{}{}
		<-release
		_ = conn.WriteJSON(map[string]any{"message_id": read.ID, "result": map[string]any{"1/1026/0": 2150, "2/1029/0": 5500, "1/47/12": 100}})
		<-sendNull
		_ = conn.WriteJSON(map[string]any{"event": "attribute_updated", "data": []any{nodeID, "1/1026/0", nil}})
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	cfg := testConfig()
	cfg.Cloud.Enabled = false
	cfg.Matter.Enabled = true
	cfg.Matter.Endpoint = "ws" + server.URL[4:]
	cfg.Matter.CollectionInterval = time.Hour
	cfg.Matter.Timeout = time.Second
	cfg.Matter.Bindings = []Binding{{DeviceID: "meter", DeviceType: "Meter", NodeID: nodeID, Attributes: map[string]string{"temperature": "1/1026/0", "humidity": "2/1029/0", "battery": "1/47/12"}}}
	require.NoError(t, cfg.Validate())
	r := testReceiver(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.matterSession(ctx) }()
	select {
	case <-readStarted:
	case <-time.After(time.Second):
		t.Fatal("Matter read did not start")
	}
	require.Empty(t, points(r.metrics(time.Now()), "switchbot.temperature"), "cached snapshot must not be presented as a fresh read")
	close(release)
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.matterValues["METER"]["temperature"].value == 21.5
	}, time.Second, time.Millisecond)
	m := r.metrics(time.Now())
	require.Equal(t, []float64{21.5}, points(m, "switchbot.temperature"))
	require.Equal(t, []float64{50}, points(m, "switchbot.battery"))
	close(sendNull)
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		_, exists := r.matterValues["METER"]["temperature"]
		return !exists
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Matter read did not stop")
	}
	<-serverDone
}

func TestMatterUnitsReachabilityAndConfig(t *testing.T) {
	cfg := testConfig()
	cfg.Matter.Enabled = true
	cfg.Matter.Bindings = []Binding{{DeviceID: "co2", NodeID: 1, Attributes: map[string]string{"co2": "3/1037/0", "battery": "3/47/12"}, ReachabilityAttribute: "3/57/17"}}
	require.NoError(t, cfg.Validate())
	r := testReceiver(t, cfg)
	r.available[1] = true
	raw := map[string]json.RawMessage{"3/1037/0": json.RawMessage(`800000`), "3/1037/8": json.RawMessage(`1`), "3/1037/9": json.RawMessage(`0`), "3/47/12": json.RawMessage(`255`), "3/57/17": json.RawMessage(`true`)}
	r.applyMatter(1, raw, []string{"3/1037/0", "3/47/12"}, time.Now())
	require.Equal(t, 800.0, r.matterValues["CO2"]["co2"].value)
	require.NotContains(t, r.matterValues["CO2"], "battery")
	raw["3/1037/8"] = json.RawMessage(`3`)
	r.applyMatter(1, raw, []string{"3/1037/8"}, time.Now())
	require.NotContains(t, r.matterValues["CO2"], "co2")
	raw["3/57/17"] = json.RawMessage(`false`)
	r.applyMatter(1, raw, []string{"3/57/17"}, time.Now())
	require.Empty(t, r.matterValues["CO2"])
	cfg.Matter.Bindings[0].Attributes["temperature"] = "3/1029/0"
	require.Error(t, cfg.Validate())
	cfg = testConfig()
	cfg.Cloud.Endpoint = "http://example.com"
	require.Error(t, cfg.Validate())
}

func TestMatterReconnectDoesNotRefreshCachedSnapshot(t *testing.T) {
	var connections atomic.Int32
	cut := make(chan struct{})
	secondRead := make(chan struct{})
	releaseSecond := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		n := connections.Add(1)
		_ = conn.WriteJSON(map[string]any{"schema_version": 14, "min_supported_schema_version": 11})
		var request map[string]json.RawMessage
		if conn.ReadJSON(&request) != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"message_id": "initial", "result": []any{map[string]any{"node_id": 1, "available": true, "attributes": map[string]any{"1/1026/0": 9900}}}})
		if conn.ReadJSON(&request) != nil {
			return
		}
		var id string
		_ = json.Unmarshal(request["message_id"], &id)
		if n == 2 {
			close(secondRead)
			<-releaseSecond
		}
		_ = conn.WriteJSON(map[string]any{"message_id": id, "result": map[string]any{"1/1026/0": 2000 + int(n)*100}})
		if n == 1 {
			<-cut
			return
		}
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	cfg := testConfig()
	cfg.Cloud.Enabled = false
	cfg.Matter.Enabled = true
	cfg.Matter.Endpoint = "ws" + server.URL[4:]
	cfg.Matter.CollectionInterval = 40 * time.Millisecond
	cfg.Matter.Timeout = time.Second
	cfg.Matter.Bindings = []Binding{{DeviceID: "meter", NodeID: 1, Attributes: map[string]string{"temperature": "1/1026/0"}}}
	r := testReceiver(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { r.matterLoop(ctx); close(done) }()
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.matterValues["METER"]["temperature"].value == 21
	}, 2*time.Second, time.Millisecond)
	close(cut)
	select {
	case <-secondRead:
	case <-time.After(2 * time.Second):
		t.Fatal("Matter did not reconnect")
	}
	require.Empty(t, points(r.metrics(time.Now()), "switchbot.temperature"))
	close(releaseSecond)
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.matterValues["METER"]["temperature"].value == 22
	}, 2*time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconnection worker did not stop")
	}
}

// Replay anonymized payloads from the actual read-only API probe, including a
// usable CO2 device whose inventory cloud flag is false and an empty camera.
func TestObservedCloudPayloads(t *testing.T) {
	data, err := os.ReadFile("testdata/observed-status.json")
	require.NoError(t, err)
	var fixtures []struct {
		ID     string                     `json:"deviceId"`
		Name   string                     `json:"deviceName"`
		Type   string                     `json:"deviceType"`
		Cloud  bool                       `json:"enableCloudService"`
		Code   int                        `json:"statusCode"`
		Status map[string]json.RawMessage `json:"status"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/devices" {
			devices := make([]map[string]any, 0, len(fixtures))
			for _, f := range fixtures {
				devices = append(devices, map[string]any{"deviceId": f.ID, "deviceName": f.Name, "deviceType": f.Type, "enableCloudService": f.Cloud})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"statusCode": 100, "body": map[string]any{"deviceList": devices}})
			return
		}
		for _, f := range fixtures {
			if req.URL.Path == "/devices/"+f.ID+"/status" {
				_ = json.NewEncoder(w).Encode(map[string]any{"statusCode": f.Code, "body": f.Status})
				return
			}
		}
		w.WriteHeader(404)
	}))
	defer server.Close()
	cfg := testConfig()
	cfg.Cloud.Endpoint = server.URL
	r := testReceiver(t, cfg)
	r.pollCloud(context.Background(), time.Now())
	m := r.metrics(time.Now())
	require.Equal(t, []float64{402}, points(m, "switchbot.co2"))
	require.Len(t, points(m, "switchbot.temperature"), 4)
	batteries := points(m, "switchbot.battery")
	require.Len(t, batteries, 9)
	zeros := 0
	for _, value := range batteries {
		if value == 0 {
			zeros++
		}
	}
	require.Equal(t, 4, zeros)
	require.Empty(t, points(m, "switchbot.power"), "press-mode Bots do not observe appliance states")
	for i := 0; i < m.ResourceMetrics().Len(); i++ {
		rm := m.ResourceMetrics().At(i)
		id, _ := rm.Resource().Attributes().Get("switchbot.device.id")
		for j := 0; j < rm.ScopeMetrics().At(0).Metrics().Len(); j++ {
			metric := rm.ScopeMetrics().At(0).Metrics().At(j)
			for k := 0; k < metric.Gauge().DataPoints().Len(); k++ {
				pointID, ok := metric.Gauge().DataPoints().At(k).Attributes().Get("switchbot.device.id")
				require.True(t, ok)
				require.Equal(t, id.Str(), pointID.Str())
			}
		}
	}
}

func TestMatterPeriodicReadRefreshesUnchangedValuesAndDetectsTimeout(t *testing.T) {
	var reads atomic.Int32
	blocked := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"schema_version": 14, "min_supported_schema_version": 11})
		var request struct {
			ID   string `json:"message_id"`
			Args struct {
				Paths []string `json:"attribute_path"`
			} `json:"args"`
		}
		if conn.ReadJSON(&request) != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"message_id": "initial", "result": []any{map[string]any{"node_id": 1, "available": true}}})
		for {
			if conn.ReadJSON(&request) != nil {
				return
			}
			n := reads.Add(1)
			if n == 1 {
				require.Contains(t, request.Args.Paths, "1/47/12")
			} else {
				require.NotContains(t, request.Args.Paths, "1/47/12", "battery read cadence is independent")
			}
			if n <= 2 {
				_ = conn.WriteJSON(map[string]any{"message_id": request.ID, "result": map[string]any{"1/1026/0": 2300, "1/47/12": 100}})
			} else if n == 3 {
				close(blocked)
			}
		}
	}))
	defer server.Close()
	cfg := testConfig()
	cfg.Cloud.Enabled = false
	cfg.Matter.Enabled = true
	cfg.Matter.Endpoint = "ws" + server.URL[4:]
	cfg.Matter.CollectionInterval = 60 * time.Millisecond
	cfg.Matter.Timeout = 150 * time.Millisecond
	cfg.Matter.Bindings = []Binding{{DeviceID: "meter", NodeID: 1, Attributes: map[string]string{"temperature": "1/1026/0", "battery": "1/47/12"}}}
	r := testReceiver(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.matterSession(ctx) }()
	var first time.Time
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		first = r.matterValues["METER"]["temperature"].at
		return !first.IsZero()
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.matterValues["METER"]["temperature"].at.After(first)
	}, time.Second, time.Millisecond)
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("periodic read was not sent")
	}
	require.Equal(t, []float64{23}, points(r.metrics(time.Now()), "switchbot.temperature"), "unchanged values are healthy until a read fails")
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.matterValues["METER"]) == 0
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("read timeout worker did not stop")
	}
}

func TestMatterConfigPreservesUnsignedNodeID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "matter.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`cloud:
  enabled: false
matter:
  enabled: true
  bindings:
    - device_id: fixture
      node_id: 18446744073709551614
      attributes:
        temperature: 1/1026/0
`), 0600))
	conf, err := confmaptest.LoadConf(path)
	require.NoError(t, err)
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, conf.Unmarshal(cfg))
	require.NoError(t, cfg.Validate())
	require.Equal(t, uint64(18446744073709551614), cfg.Matter.Bindings[0].NodeID)
}
