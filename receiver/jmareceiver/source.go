// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package jmareceiver

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ymotongpoo/opentelemetry-collector-extra/internal/weather"
)

type source struct {
	cfg         *Config
	client      *http.Client
	lastRequest time.Time
}

func newSource(c *Config) *source { return &source{cfg: c, client: weather.NewHTTPClient(c.Timeout)} }

func (s *source) requestURL(day time.Time) string {
	u, _ := url.Parse(s.cfg.endpoint())
	q := u.Query()
	q.Set("prec_no", s.cfg.PrecNo)
	q.Set("block_no", s.cfg.BlockNo)
	q.Set("year", strconv.Itoa(day.Year()))
	q.Set("month", strconv.Itoa(int(day.Month())))
	q.Set("day", strconv.Itoa(day.Day()))
	q.Set("view", "p1")
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *source) fetchDay(ctx context.Context, day time.Time, publish weather.Publish) error {
	if wait := time.Until(s.lastRequest.Add(s.cfg.MinRequestInterval)); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	s.lastRequest = time.Now()
	data, err := weather.Get(ctx, s.client, s.requestURL(day))
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	records, err := parsePage(data, day, s.cfg)
	if err != nil {
		return err
	}
	return publish(records, at)
}

func (s *source) collect(ctx context.Context, now time.Time, store *weather.Store, publish weather.Publish) error {
	local := now.In(japan)
	today := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, japan)
	// A completed daily pass survives restarts. Failed passes retry on the next
	// poll; already accepted observations are deduplicated by the shared journal.
	if store.Checkpoint("recent").Before(today) {
		for n := 1; n <= s.cfg.RecentDays; n++ {
			if err := s.fetchDay(ctx, today.AddDate(0, 0, -n), publish); err != nil {
				return err
			}
		}
		if err := store.SetCheckpoint("recent", now); err != nil {
			return err
		}
	}
	if last := store.Checkpoint("revision"); last.IsZero() || now.Sub(last) >= s.cfg.RevisionInterval {
		for n := s.cfg.RecentDays + 1; n <= s.cfg.RevisionDays; n++ {
			if err := s.fetchDay(ctx, today.AddDate(0, 0, -n), publish); err != nil {
				return err
			}
		}
		if err := store.SetCheckpoint("revision", now); err != nil {
			return err
		}
	}
	if b := s.cfg.Backfill; b.StartDate != "" {
		start, _ := time.ParseInLocation(time.DateOnly, b.StartDate, japan)
		end, _ := time.ParseInLocation(time.DateOnly, b.EndDate, japan)
		if !end.Before(today) {
			return errors.New("JMA backfill end_date must be before today in Japan")
		}
		key := "backfill|" + b.RequestID + "|" + b.StartDate + "|" + b.EndDate
		last := store.Checkpoint(key)
		for day := start; !day.After(end); day = day.AddDate(0, 0, 1) {
			if !last.IsZero() && !day.After(last) {
				continue
			}
			if err := s.fetchDay(ctx, day, publish); err != nil {
				return err
			}
			if err := store.SetCheckpoint(key, day); err != nil {
				return err
			}
		}
	}
	return nil
}
