// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package weather

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

type entry struct {
	Hash     string    `json:"hash"`
	EventID  string    `json:"event_id"`
	Revision uint64    `json:"revision"`
	Updated  time.Time `json:"updated"`
}

type state struct {
	Version     int                  `json:"version"`
	Namespace   string               `json:"namespace"`
	Entries     map[string]entry     `json:"entries"`
	Checkpoints map[string]time.Time `json:"checkpoints"`
	Pending     []Event              `json:"pending,omitempty"`
}

type Store struct {
	path string
	lock *flock.Flock
	data state
}

func OpenStore(path, namespace string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	l := flock.New(path + ".lock")
	ok, err := l.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("weather state_file is already in use")
	}
	s := &Store{path: path, lock: l, data: state{Version: 1, Namespace: namespace, Entries: map[string]entry{}, Checkpoints: map[string]time.Time{}}}
	if err = s.load(); err != nil {
		_ = l.Unlock()
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	f, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return s.save(s.data)
	}
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() > 64<<20 {
		return errors.New("weather state exceeds 64 MiB")
	}
	decoder := json.NewDecoder(io.LimitReader(f, 64<<20))
	decoder.DisallowUnknownFields()
	var st state
	if err = decoder.Decode(&st); err != nil {
		return fmt.Errorf("read weather state: %w", err)
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return errors.New("weather state has trailing data")
	}
	if st.Version != 1 || st.Namespace != s.data.Namespace || st.Entries == nil || st.Checkpoints == nil {
		return errors.New("incompatible weather state; use a new state_file for a different source/location/model")
	}
	for _, e := range st.Pending {
		if err = e.Record.validate(); err != nil || e.EventID == "" || e.RetrievedAt.IsZero() {
			return errors.New("invalid pending weather event")
		}
	}
	s.data = st
	return nil
}

func (s *Store) save(st state) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	if len(b) > 64<<20 {
		return errors.New("weather state exceeds 64 MiB")
	}
	f, err := os.CreateTemp(filepath.Dir(s.path), ".weather-state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, s.path); err != nil {
		return err
	}
	s.data = st
	// Directory sync makes the rename durable on filesystems that support it.
	if d, e := os.Open(filepath.Dir(s.path)); e == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Stage writes the outbox before delivery. A consumer error leaves it available
// for replay, including after a process crash. Keys and event IDs are stable on retry.
func (s *Store) Stage(records []Record, retrieved time.Time, ttl time.Duration) error {
	if len(s.data.Pending) > 0 {
		return errors.New("weather outbox must be drained before staging")
	}
	next := s.data
	next.Entries = maps.Clone(s.data.Entries)
	next.Pending = nil
	seen := make(map[string]string, len(records))
	for _, r := range records {
		if err := r.validate(); err != nil {
			return err
		}
		r.PeriodStart = r.PeriodStart.UTC()
		r.PeriodEnd = r.PeriodEnd.UTC()
		key := r.key()
		hash, err := hashJSON(r)
		if err != nil {
			return err
		}
		if h, ok := seen[key]; ok {
			if h != hash {
				return errors.New("conflicting weather records in one response")
			}
			continue
		}
		seen[key] = hash
		old := next.Entries[key]
		if old.Hash == hash {
			continue
		}
		e := Event{Record: r, SchemaVersion: 1, ObservationKey: key, Revision: old.Revision + 1, RetrievedAt: retrieved.UTC(), Supersedes: old.EventID}
		e.EventID, _ = hashJSON([]any{key, hash, e.Revision, e.RetrievedAt})
		next.Pending = append(next.Pending, e)
		next.Entries[key] = entry{Hash: hash, EventID: e.EventID, Revision: e.Revision, Updated: retrieved.UTC()}
	}
	for key, e := range next.Entries {
		if e.Updated.Before(retrieved.Add(-ttl)) {
			delete(next.Entries, key)
		}
	}
	return s.save(next)
}

func (s *Store) Pending() []Event                { return s.data.Pending }
func (s *Store) Ack() error                      { next := s.data; next.Pending = nil; return s.save(next) }
func (s *Store) Checkpoint(key string) time.Time { return s.data.Checkpoints[key] }
func (s *Store) SetCheckpoint(key string, t time.Time) error {
	next := s.data
	next.Checkpoints = maps.Clone(s.data.Checkpoints)
	next.Checkpoints[key] = t.UTC()
	return s.save(next)
}
func (s *Store) Close() error { return s.lock.Unlock() }
