// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package weather

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"time"
)

// Record distinguishes valid time from the time a provider response was retrieved.
// Instantaneous values have equal PeriodStart and PeriodEnd.
type Record struct {
	Source      string            `json:"source"`
	Kind        string            `json:"kind"`
	LocationID  string            `json:"location_id"`
	Model       string            `json:"model,omitempty"`
	Variable    string            `json:"variable"`
	PeriodStart time.Time         `json:"period_start"`
	PeriodEnd   time.Time         `json:"period_end"`
	Aggregation string            `json:"aggregation"`
	Value       *float64          `json:"value"`
	TextValue   string            `json:"text_value,omitempty"`
	Unit        string            `json:"unit"`
	Quality     string            `json:"quality"`
	RawValue    string            `json:"raw_value,omitempty"`
	Details     map[string]string `json:"details,omitempty"`
}

type Event struct {
	Record
	SchemaVersion  int       `json:"schema_version"`
	ObservationKey string    `json:"observation_key"`
	EventID        string    `json:"event_id"`
	Supersedes     string    `json:"supersedes,omitempty"`
	Revision       uint64    `json:"revision"`
	RetrievedAt    time.Time `json:"retrieved_at"`
}

func Number(v float64) *float64 { return &v }

func hashJSON(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func (r Record) validate() error {
	if r.Source == "" || r.LocationID == "" || r.Kind == "" || r.Variable == "" || r.Unit == "" || r.Quality == "" || r.Aggregation == "" || r.PeriodStart.IsZero() || r.PeriodEnd.Before(r.PeriodStart) {
		return errors.New("invalid weather record identity or interval")
	}
	if r.Value != nil && (math.IsNaN(*r.Value) || math.IsInf(*r.Value, 0)) {
		return errors.New("non-finite weather value")
	}
	return nil
}

func (r Record) key() string {
	s, _ := hashJSON([]any{r.Source, r.Kind, r.LocationID, r.Model, r.Variable, r.PeriodStart.UTC(), r.PeriodEnd.UTC(), r.Aggregation})
	return s
}
