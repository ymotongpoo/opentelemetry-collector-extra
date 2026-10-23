// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package slackreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

func TestSocketModeLifecycleAndAcknowledgements(t *testing.T) {
	acked := make(chan string, 4)
	closed := make(chan struct{})
	send := make(chan struct{})
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()
	mux.HandleFunc("/auth.test", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"team_id":"T1","user_id":"U1"}`))
	})
	mux.HandleFunc("/apps.connections.open", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "url": "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"})
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		defer close(closed)
		_ = conn.WriteJSON(map[string]any{"type": "hello"})
		<-send
		for _, id := range []string{"E1", "E2"} {
			_ = conn.WriteJSON(map[string]any{
				"type": "events_api", "envelope_id": id,
				"payload": map[string]any{"type": "event_callback", "team_id": "T1", "event_id": "same-event",
					"event": map[string]any{"type": "message", "channel": "C1", "user": "U1", "ts": "100.001", "text": "日本語😀"}},
			})
		}
		for {
			var msg struct {
				EnvelopeID string `json:"envelope_id"`
			}
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			acked <- msg.EnvelopeID
		}
	})
	sink := &consumertest.MetricsSink{}
	r := testReceiver(t, sink)
	api := slack.New("xoxb-test", slack.OptionAppLevelToken("xapp-test"), slack.OptionAPIURL(server.URL+"/"))
	r.client = &socketAdapter{socketmode.New(api)}
	r.authTest = api.AuthTestContext
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	startCtx, startCancel := context.WithCancel(ctx)
	require.NoError(t, r.Start(startCtx, nil))
	startCancel()
	close(send)
	for i := 0; i < 2; i++ {
		select {
		case <-acked:
		case <-ctx.Done():
			t.Fatal("Socket Mode did not acknowledge message")
		}
	}
	require.NoError(t, r.Shutdown(ctx))
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("Socket Mode connection did not close")
	}
	require.NoError(t, r.Shutdown(ctx))
	require.Len(t, sink.AllMetrics(), 1)
	metrics := sink.AllMetrics()[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, int64(1), metrics.At(0).Sum().DataPoints().At(0).IntValue())
	require.Equal(t, int64(4), metrics.At(1).Sum().DataPoints().At(0).IntValue())
}

type fakeSocket struct {
	eventCh chan socketmode.Event
	runErr  error
}

func (f *fakeSocket) events() <-chan socketmode.Event           { return f.eventCh }
func (f *fakeSocket) RunContext(context.Context) error          { return f.runErr }
func (f *fakeSocket) AckCtx(context.Context, string, any) error { return nil }

func TestStartupFailureAndShutdownBeforeStart(t *testing.T) {
	r := testReceiver(t, &consumertest.MetricsSink{})
	require.NoError(t, r.Shutdown(context.Background()))
	r = testReceiver(t, &consumertest.MetricsSink{})
	r.authTest = func(context.Context) (*slack.AuthTestResponse, error) { return nil, errors.New("invalid_auth") }
	require.ErrorContains(t, r.Start(context.Background(), nil), "invalid_auth")
	require.NoError(t, r.Shutdown(context.Background()))
	r = testReceiver(t, &consumertest.MetricsSink{})
	r.authTest = func(context.Context) (*slack.AuthTestResponse, error) {
		return &slack.AuthTestResponse{TeamID: "T"}, nil
	}
	r.client = &fakeSocket{eventCh: make(chan socketmode.Event), runErr: errors.New("invalid app token")}
	require.ErrorContains(t, r.Start(context.Background(), nil), "invalid app token")
	require.NoError(t, r.Shutdown(context.Background()))
}

func TestMessageFiltering(t *testing.T) {
	r := testReceiver(t, &consumertest.MetricsSink{})
	r.messageEvent("T", nil)
	for _, subtype := range []string{"message_changed", "message_deleted", "channel_join", "channel_leave", "bot_message"} {
		r.messageEvent("T", &slackevents.MessageEvent{Channel: "C", SubType: subtype})
	}
	r.messageEvent("T", &slackevents.MessageEvent{Channel: "C", BotID: "B"})
	require.Empty(t, r.stats.totals)
	for i, subtype := range []string{"", "file_share", "me_message", "thread_broadcast"} {
		r.messageEvent("T", &slackevents.MessageEvent{Channel: "C", SubType: subtype, TimeStamp: string(rune('0' + i)), Text: "x"})
	}
	require.Equal(t, int64(4), r.stats.totals["T"]["C"].count)
	r.config.IncludeBots = true
	r.messageEvent("T", &slackevents.MessageEvent{Channel: "C", SubType: "bot_message"})
	require.Equal(t, int64(5), r.stats.totals["T"]["C"].count)
}

type runningSocket struct {
	fakeSocket
	fail chan error
}

func (f *runningSocket) RunContext(ctx context.Context) error {
	select {
	case f.eventCh <- socketmode.Event{Type: socketmode.EventTypeConnected}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-f.fail:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type statusHost struct {
	component.Host
	events chan *componentstatus.Event
}

func (h *statusHost) Report(event *componentstatus.Event) { h.events <- event }

func TestRuntimeSocketFailureReportsFatalStatus(t *testing.T) {
	r := testReceiver(t, &consumertest.MetricsSink{})
	r.authTest = func(context.Context) (*slack.AuthTestResponse, error) {
		return &slack.AuthTestResponse{TeamID: "T"}, nil
	}
	client := &runningSocket{fakeSocket: fakeSocket{eventCh: make(chan socketmode.Event)}, fail: make(chan error)}
	r.client = client
	host := &statusHost{events: make(chan *componentstatus.Event, 2)}
	require.NoError(t, r.Start(context.Background(), host))
	client.fail <- errors.New("connection rejected")
	select {
	case event := <-host.events:
		require.Equal(t, componentstatus.StatusFatalError, event.Status())
	case <-time.After(time.Second):
		t.Fatal("runtime error was not reported to Collector")
	}
	require.NoError(t, r.Shutdown(context.Background()))
}
