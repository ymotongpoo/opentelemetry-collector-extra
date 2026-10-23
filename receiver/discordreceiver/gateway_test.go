// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package discordreceiver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

func TestGatewayHandshakeAndMessages(t *testing.T) {
	closed := make(chan struct{})
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()
	old := discordgo.EndpointGateway
	discordgo.EndpointGateway = server.URL + "/gateway"
	defer func() { discordgo.EndpointGateway = old }()
	mux.HandleFunc("/gateway", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"url": "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"})
	})
	mux.HandleFunc("/ws/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		defer close(closed)
		_ = conn.WriteJSON(map[string]any{"op": 10, "d": map[string]int{"heartbeat_interval": 45000}})
		var identify struct {
			Op   int `json:"op"`
			Data struct {
				Intents int `json:"intents"`
			} `json:"d"`
		}
		if conn.ReadJSON(&identify) != nil {
			return
		}
		if identify.Op != 2 || identify.Data.Intents&int(discordgo.IntentMessageContent) == 0 {
			t.Error("message content intent missing from identify")
		}
		_ = conn.WriteJSON(map[string]any{"op": 0, "t": "READY", "s": 1, "d": map[string]any{"v": 10, "session_id": "test-session", "user": map[string]any{"id": "bot", "bot": true}, "guilds": []any{}}})
		_ = conn.WriteJSON(map[string]any{"op": 0, "t": "MESSAGE_CREATE", "s": 2, "d": map[string]any{"id": "1", "guild_id": "G", "channel_id": "C", "content": "日本語😀", "author": map[string]any{"id": "U", "bot": false}}})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	sink := &consumertest.MetricsSink{}
	r := testReceiver(t, sink)
	r.config.BufferInterval = 10 * time.Millisecond
	require.NoError(t, r.Start(context.Background(), nil))
	t.Cleanup(func() { _ = r.Shutdown(context.Background()) })
	require.Eventually(t, func() bool { return len(sink.AllMetrics()) > 0 }, 2*time.Second, time.Millisecond)
	require.NoError(t, r.Shutdown(context.Background()))
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("gateway connection did not close")
	}
	md := sink.AllMetrics()[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, int64(1), md.At(0).Sum().DataPoints().At(0).IntValue())
	require.Equal(t, int64(4), md.At(1).Sum().DataPoints().At(0).IntValue())
}

func TestGatewayHandshakeCancellation(t *testing.T) {
	closed := make(chan struct{})
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()
	old := discordgo.EndpointGateway
	discordgo.EndpointGateway = server.URL + "/gateway"
	defer func() { discordgo.EndpointGateway = old }()
	mux.HandleFunc("/gateway", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"url": "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"})
	})
	mux.HandleFunc("/ws/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		defer close(closed)
		// Deliberately omit HELLO. Open must still respond to cancellation.
		_, _, _ = conn.ReadMessage()
	})
	gateway, err := newGatewaySession(testConfig())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, gateway.Open(ctx), context.DeadlineExceeded)
	require.NoError(t, gateway.Close())
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("stalled socket did not close")
	}
}

func TestGatewayRateLimitCancellation(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"retry_after":3600,"global":false}`))
	}))
	defer server.Close()
	old := discordgo.EndpointGateway
	discordgo.EndpointGateway = server.URL
	defer func() { discordgo.EndpointGateway = old }()
	gateway, err := newGatewaySession(testConfig())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var limited *discordgo.RateLimitError
	require.ErrorAs(t, gateway.Open(ctx), &limited)
	require.ErrorIs(t, gateway.Open(ctx), context.DeadlineExceeded)
	require.Equal(t, 1, calls)
}
