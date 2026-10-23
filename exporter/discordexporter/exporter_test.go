// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package discordexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func testConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.Token = "test"
	cfg.Channel = "C1"
	return cfg
}

func testMetrics(long bool) pmetric.Metrics {
	md := pmetric.NewMetrics()
	metric := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("test.messages")
	point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	point.SetIntValue(42)
	text := "<!channel> <@U1> & <https://example.com>"
	if long {
		text += strings.Repeat("日本語😀", 1600)
	}
	point.Attributes().PutStr("message", text)
	return md
}

func TestConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.Error(t, cfg.Validate())
	require.NoError(t, confmap.NewFromStringMap(map[string]any{"token": "test", "channel": "C1", "timeout": "5s", "max_retries": 0}).Unmarshal(cfg))
	require.NoError(t, cfg.Validate())
	require.Equal(t, 5*time.Second, cfg.Timeout)
	cfg.Timeout = 0
	require.Error(t, cfg.Validate())
	cfg = testConfig()
	cfg.MaxRetries = -1
	require.Error(t, cfg.Validate())
	cfg = testConfig()
	cfg.Channel = ""
	require.Error(t, cfg.Validate())
}

func TestHTTPSerializationAndLongMessages(t *testing.T) {
	var mu sync.Mutex
	var texts []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "Bot test", r.Header.Get("Authorization"))
		require.Equal(t, "DiscordBot (https://github.com/ymotongpoo/opentelemetry-collector-extra, 0.1.0)", r.Header.Get("User-Agent"))
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		var payload map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		require.Equal(t, []any{}, payload["allowed_mentions"].(map[string]any)["parse"])
		require.Equal(t, float64(4), payload["flags"])
		text := payload["content"].(string)
		require.True(t, utf8.ValidString(text))
		require.LessOrEqual(t, len(utf16.Encode([]rune(text))), 2000)
		mu.Lock()
		texts = append(texts, text)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1"}`))
	}))
	defer server.Close()
	e := newDiscordExporter(testConfig())
	e.endpoint = server.URL
	require.NoError(t, e.pushMetrics(context.Background(), pmetric.NewMetrics()))
	md := testMetrics(true)
	original, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, e.pushMetrics(ctx, md))
	mu.Lock()
	defer mu.Unlock()
	require.Greater(t, len(texts), 1)
	var body strings.Builder
	for i, text := range texts {
		prefix := fmt.Sprintf("[OTLP metrics %d/%d]\n", i+1, len(texts))
		require.True(t, strings.HasPrefix(text, prefix))
		body.WriteString(strings.TrimPrefix(text, prefix))
	}
	require.JSONEq(t, string(original), body.String())
	unchanged, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
	require.NoError(t, err)
	require.Equal(t, string(original), string(unchanged))
	require.NoError(t, e.Shutdown(context.Background()))
}

func TestRetriesOnlyFailedPart(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		mu.Lock()
		requests = append(requests, payload["content"].(string))
		n := len(requests)
		mu.Unlock()
		if n == 2 {
			w.WriteHeader(503)
			return
		}
		if n == 3 {
			w.Header().Set("Retry-After", "0.001")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"retry_after":0.001}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"1"}`))
	}))
	defer server.Close()
	e := newDiscordExporter(testConfig())
	e.endpoint = server.URL
	e.retryDelay = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, e.pushMetrics(ctx, testMetrics(true)))
	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(requests), 4)
	require.NotEqual(t, requests[0], requests[1])
	require.Equal(t, requests[1], requests[2])
	require.Equal(t, requests[2], requests[3])
}

func TestPermanentAndPartialFailures(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if partial && calls == 1 {
					_, _ = w.Write([]byte(`{"id":"1"}`))
					return
				}
				w.WriteHeader(403)
				_, _ = w.Write([]byte(`{"ok":false,"error":"missing_scope"}`))
			}))
			defer server.Close()
			e := newDiscordExporter(testConfig())
			e.endpoint = server.URL
			e.retryDelay = time.Millisecond
			err := e.pushMetrics(context.Background(), testMetrics(partial))
			require.Error(t, err)
			require.True(t, consumererror.IsPermanent(err))
			if partial {
				require.Equal(t, 2, calls)
				require.ErrorContains(t, err, "delivered 1 of")
			} else {
				require.Equal(t, 1, calls)
			}
		})
	}
}

func TestRateLimitAndGateCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"retry_after":3600}`))
	}))
	defer server.Close()
	e := newDiscordExporter(testConfig())
	e.endpoint = server.URL
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.ErrorIs(t, e.pushMetrics(ctx, testMetrics(false)), context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
	e.gate <- struct{}{}
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	require.ErrorIs(t, e.pushMetrics(canceled, testMetrics(false)), context.Canceled)
	<-e.gate
}

func TestRetryExhaustion(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(503) }))
	defer server.Close()
	e := newDiscordExporter(testConfig())
	e.endpoint = server.URL
	e.retryDelay = time.Millisecond
	require.Error(t, e.pushMetrics(context.Background(), testMetrics(false)))
	require.Equal(t, 4, calls)
}

func TestInflightRequestCancellation(t *testing.T) {
	arrived := make(chan struct{})
	released := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(arrived)
		<-r.Context().Done()
		close(released)
	}))
	defer server.Close()
	e := newDiscordExporter(testConfig())
	e.endpoint = server.URL
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- e.pushMetrics(ctx, testMetrics(false)) }()
	select {
	case <-arrived:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("request ignored cancellation")
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("HTTP connection did not close")
	}
}
