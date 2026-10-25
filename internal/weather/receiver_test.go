package weather

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestConsumerFailureReplaysBeforeCollect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := openTestStore(t, path)
	r := fixtureRecord()
	at := r.PeriodEnd.Add(time.Hour)
	if err := s.Stage([]Record{r}, at, 120*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	id := s.Pending()[0].EventID
	_ = s.Close()
	fail := true
	var delivered []Event
	next, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
		if time.Since(lr.Timestamp().AsTime()) > time.Minute {
			t.Error("transport timestamp was backdated")
		}
		var e Event
		if err := json.Unmarshal([]byte(lr.Body().Str()), &e); err != nil {
			return err
		}
		delivered = append(delivered, e)
		if fail {
			return errors.New("unavailable")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(time.Hour)
	cfg.StateFile = path
	collected := false
	recv, err := NewReceiver(cfg, "fixture", "jma", receivertest.NewNopSettings(component.MustNewType("jma")), next, func(context.Context, time.Time, *Store, Publish) error { collected = true; return nil })
	if err != nil {
		t.Fatal(err)
	}
	recv.store = openTestStore(t, path)
	defer recv.store.Close()
	if err = recv.poll(context.Background()); err == nil || collected {
		t.Fatal("did not stop on failed replay")
	}
	if len(recv.store.Pending()) != 1 {
		t.Fatal("unacknowledged event lost")
	}
	fail = false
	if err = recv.poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !collected || len(delivered) != 2 || delivered[0].EventID != id || delivered[1].EventID != id || len(recv.store.Pending()) != 0 {
		t.Fatal("retry identity or ordering failed")
	}
}

func TestShutdownCancelsCollectionAndReleasesState(t *testing.T) {
	entered := make(chan struct{})
	done := make(chan struct{})
	next, _ := consumer.NewLogs(func(context.Context, plog.Logs) error { return nil })
	cfg := DefaultConfig(time.Hour)
	cfg.StateFile = filepath.Join(t.TempDir(), "state.json")
	r, err := NewReceiver(cfg, "fixture", "jma", receivertest.NewNopSettings(component.MustNewType("jma")), next, func(ctx context.Context, _ time.Time, _ *Store, _ Publish) error {
		close(entered)
		<-ctx.Done()
		close(done)
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err = r.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("collection did not start")
	}
	if err = r.Start(context.Background(), nil); err == nil {
		t.Fatal("double start accepted")
	}
	stop, c := context.WithTimeout(context.Background(), 3*time.Second)
	defer c()
	if err = r.Shutdown(stop); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	default:
		t.Fatal("collection leaked")
	}
	s := openTestStore(t, cfg.StateFile)
	_ = s.Close()
	if err = r.Shutdown(stop); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPFailuresAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/wait" {
			<-r.Context().Done()
			return
		}
		http.Error(w, "private provider body", 429)
	}))
	defer server.Close()
	client := NewHTTPClient(time.Second)
	if _, err := Get(context.Background(), client, server.URL); err == nil || err.Error() != "weather HTTP status 429" {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Get(ctx, client, server.URL+"/wait"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("cross-origin redirect followed") }))
	defer other.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL, 302) }))
	defer redirect.Close()
	if _, err := Get(context.Background(), client, redirect.URL); err == nil {
		t.Fatal("cross-origin redirect allowed")
	}
}
