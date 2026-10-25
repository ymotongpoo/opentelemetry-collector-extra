// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package smartheimreceiver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/smartheimreceiver/internal/metadata"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func testReceiver(t *testing.T, cfg *Config, next consumer.Metrics) *smartheimReceiver {
	t.Helper()
	require.NoError(t, cfg.Validate())
	r, err := newReceiver(cfg, receivertest.NewNopSettings(metadata.Type), next)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, r.Shutdown(context.Background()))
		r.client.CloseIdleConnections()
		r.discoveryClient.CloseIdleConnections()
	})
	return r
}

func metric(t *testing.T, md pmetric.Metrics, name string) pmetric.Metric {
	t.Helper()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		sm := md.ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < sm.Len(); j++ {
			ms := sm.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == name {
					return ms.At(k)
				}
			}
		}
	}
	t.Fatalf("missing metric %s", name)
	return pmetric.Metric{}
}

func TestCollectPartialFailureAndRecovery(t *testing.T) {
	summaryHTML, branchesHTML := fixture(t, "summary"), fixture(t, "branches")
	var branchFails, summaryFails, wrongID atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		switch req.URL.Path {
		case summaryPath:
			if summaryFails.Load() {
				http.Error(w, "offline", 503)
				return
			}
			_, _ = io.WriteString(w, summaryHTML)
		case branchesPath:
			if branchFails.Load() {
				http.Error(w, "offline", 503)
				return
			}
			page := branchesHTML
			if wrongID.Load() {
				page = strings.ReplaceAll(page, "TEST0001SC", "OTHER")
			}
			_, _ = io.WriteString(w, page)
		default:
			http.NotFound(w, req)
		}
	}))
	defer srv.Close()
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = srv.URL
	sink := &consumertest.MetricsSink{}
	r := testReceiver(t, cfg, sink)
	require.NoError(t, r.collect(context.Background()))
	md := sink.AllMetrics()[0]
	require.Equal(t, 7, md.DataPointCount())
	id, ok := md.ResourceMetrics().At(0).Resource().Attributes().Get("smartheim.device.id")
	require.True(t, ok)
	require.Equal(t, "TEST0001SC", id.Str())
	require.Equal(t, "W", metric(t, md, "smartheim.solar.power").Unit())
	require.Equal(t, float64(4000), metric(t, md, "smartheim.solar.power").Gauge().DataPoints().At(0).DoubleValue())
	bs := metric(t, md, "smartheim.branch.power").Gauge().DataPoints()
	require.Equal(t, 3, bs.Len())
	branchID, _ := bs.At(1).Attributes().Get("smartheim.branch.id")
	require.Equal(t, "3", branchID.Str())
	branchName, _ := bs.At(0).Attributes().Get("smartheim.branch.name")
	require.Equal(t, "キッチン", branchName.Str())
	branchFails.Store(true)
	require.ErrorContains(t, r.collect(context.Background()), "branches")
	require.Equal(t, 4, sink.AllMetrics()[1].DataPointCount())
	branchFails.Store(false)
	wrongID.Store(true)
	require.ErrorContains(t, r.collect(context.Background()), "different device numbers")
	require.Equal(t, 4, sink.AllMetrics()[2].DataPointCount())
	summaryFails.Store(true)
	require.Error(t, r.collect(context.Background()))
	require.Len(t, sink.AllMetrics(), 3) // No cached or zero measurements during failure.
	summaryFails.Store(false)
	wrongID.Store(false)
	require.NoError(t, r.collect(context.Background()))
	require.Equal(t, 7, sink.AllMetrics()[3].DataPointCount())
	r.deviceID = "OTHER"
	require.ErrorContains(t, r.collect(context.Background()), "device number mismatch")
	require.Len(t, sink.AllMetrics(), 4)
}

func TestConsumerErrorAndDisabledBranches(t *testing.T) {
	page := fixture(t, "summary")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != summaryPath {
			t.Error("unexpected branch request")
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, page)
	}))
	defer srv.Close()
	want := errors.New("downstream failure")
	next, err := consumer.NewMetrics(func(_ context.Context, md pmetric.Metrics) error {
		require.Equal(t, 4, md.DataPointCount())
		return want
	})
	require.NoError(t, err)
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = srv.URL
	cfg.CollectBranches = false
	r := testReceiver(t, cfg, next)
	require.ErrorIs(t, r.collect(context.Background()), want)
}

func TestDiscoveryIdentificationAmbiguityAndCancellation(t *testing.T) {
	page := fixture(t, "summary")
	newDevice := func(id string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = io.WriteString(w, strings.ReplaceAll(page, "TEST0001SC", id))
		}))
	}
	a, b := newDevice("TEST0001SC"), newDevice("OTHER")
	defer a.Close()
	defer b.Close()
	unrelated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "<html>unrelated server</html>")
	}))
	defer unrelated.Close()
	client := newHTTPClient(time.Second, time.Second)
	defer client.CloseIdleConnections()
	endpoints := []string{unrelated.URL, a.URL, b.URL}
	got, err := discover(context.Background(), client, endpoints, "TEST0001SC", 2)
	require.NoError(t, err)
	require.Equal(t, a.URL, got)
	_, err = discover(context.Background(), client, endpoints, "", 2)
	require.ErrorContains(t, err, "multiple matching")
	_, err = discover(context.Background(), client, endpoints, "MISSING", 2)
	require.ErrorContains(t, err, "no matching")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = discover(ctx, client, endpoints, "", 2)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDiscoveryRetriesAfterAddressLoss(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "gone", 503) }))
	defer srv.Close()
	cfg := createDefaultConfig().(*Config)
	r := testReceiver(t, cfg, consumertest.NewNop())
	r.endpoint = srv.URL
	r.deviceID = "PINNED"
	r.lastDiscovery = time.Now()
	for range 3 {
		require.Error(t, r.collect(context.Background()))
	}
	require.Empty(t, r.endpoint)
	require.Equal(t, "PINNED", r.deviceID)
	require.ErrorContains(t, r.collect(context.Background()), "waiting for next discovery")
	// A newly located address is usable without resetting the pinned identity.
	page := strings.ReplaceAll(fixture(t, "summary"), "TEST0001SC", "PINNED")
	replacement := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, page)
	}))
	defer replacement.Close()
	r.endpoint = replacement.URL
	r.cfg.CollectBranches = false
	require.NoError(t, r.collect(context.Background()))
}

func TestShutdownCancelsInFlightRequestAndStartContext(t *testing.T) {
	entered := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) { close(entered); <-req.Context().Done() }))
	defer srv.Close()
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = srv.URL
	r := testReceiver(t, cfg, consumertest.NewNop())
	startCtx, cancelStart := context.WithCancel(context.Background())
	require.NoError(t, r.Start(startCtx, nil))
	cancelStart()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("no initial request")
	}
	require.Error(t, r.Start(context.Background(), nil))
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(stopCtx))
	require.NoError(t, r.Shutdown(stopCtx))
	require.Error(t, r.Start(context.Background(), nil))
}

func TestHTTPBoundaries(t *testing.T) {
	client := newHTTPClient(50*time.Millisecond, time.Second)
	defer client.CloseIdleConnections()
	for _, mode := range []string{"oversized", "redirect", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch mode {
				case "oversized":
					_, _ = io.WriteString(w, strings.Repeat("x", maxPageBytes+1))
				case "redirect":
					http.Redirect(w, req, "http://127.0.0.1:1", 302)
				case "timeout":
					<-req.Context().Done()
				}
			}))
			defer srv.Close()
			_, err := fetchPage(context.Background(), client, srv.URL, summaryPath)
			require.Error(t, err)
		})
	}
}

func TestConfigAndNetworks(t *testing.T) {
	require.NoError(t, createDefaultConfig().(*Config).Validate())
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Discovery.Enabled = false },
		func(c *Config) { c.CollectionInterval = 0 },
		func(c *Config) { c.Timeout = 0 },
		func(c *Config) { c.Endpoint = "http://user:password@localhost" },
		func(c *Config) { c.Endpoint = "http://localhost/cgi-bin/inst_val_sv.cgi" },
		func(c *Config) { c.Discovery.Concurrency = 100 },
		func(c *Config) { c.Discovery.Networks = []string{"192.168.0.0/16"} },
		func(c *Config) { c.Discovery.Networks = []string{"8.8.8.0/24"} },
	} {
		c := createDefaultConfig().(*Config)
		mutate(c)
		require.Error(t, c.Validate())
	}
	ps, err := discoveryNetworks([]string{"192.168.0.3/30", "192.168.0.0/30"})
	require.NoError(t, err)
	require.Len(t, ps, 1)
	require.Equal(t, []string{"http://192.168.0.1", "http://192.168.0.2"}, candidateEndpoints(ps))
	require.Len(t, candidateEndpoints([]netip.Prefix{netip.MustParsePrefix("192.168.0.0/24")}), 254)
}

// Opt-in test for a real LAN device. Normal tests never contact the LAN.
func TestLiveDevice(t *testing.T) {
	endpoint := os.Getenv("SMARTHEIM_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set SMARTHEIM_TEST_ENDPOINT to test a real device")
	}
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = endpoint
	sink := &consumertest.MetricsSink{}
	r := testReceiver(t, cfg, sink)
	require.NoError(t, r.collect(context.Background()))
	require.NotEmpty(t, sink.AllMetrics())
	md := sink.AllMetrics()[0]
	for _, name := range []string{"smartheim.solar.power", "smartheim.consumption.power", "smartheim.branch.power"} {
		m := metric(t, md, name)
		t.Logf("%s: %d points", name, m.Gauge().DataPoints().Len())
	}
	if network := os.Getenv("SMARTHEIM_TEST_NETWORK"); network != "" {
		ps, err := discoveryNetworks([]string{network})
		require.NoError(t, err)
		got, err := discover(context.Background(), r.discoveryClient, candidateEndpoints(ps), r.deviceID, cfg.Discovery.Concurrency)
		require.NoError(t, err)
		require.Equal(t, endpoint, got)
		t.Log("LAN discovery matched the configured device")
	}
}
