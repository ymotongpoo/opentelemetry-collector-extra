// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

// Package weather implements the durable change journal shared by weather receivers.
package weather

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	Timeout            time.Duration `mapstructure:"timeout"`
	StateFile          string        `mapstructure:"state_file"`
	StateTTL           time.Duration `mapstructure:"state_ttl"`
}

func DefaultConfig(interval time.Duration) Config {
	return Config{CollectionInterval: interval, Timeout: 30 * time.Second, StateTTL: 120 * 24 * time.Hour}
}

func (c Config) Validate() error {
	if c.CollectionInterval <= 0 || c.Timeout <= 0 || c.StateTTL < 24*time.Hour {
		return errors.New("collection_interval and timeout must be positive; state_ttl must be at least 24h")
	}
	if strings.TrimSpace(c.StateFile) == "" {
		return errors.New("state_file is required (use a different file for each receiver instance)")
	}
	return nil
}

func ValidateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("endpoint must be an HTTP(S) URL without credentials, query or fragment")
	}
	return nil
}

func NewHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 || req.URL.Host != via[0].URL.Host || req.URL.Scheme != via[0].URL.Scheme {
			return errors.New("cross-origin redirect refused")
		}
		return nil
	}}
}

// Get bounds responses and never includes response bodies or query strings in errors.
func Get(ctx context.Context, client *http.Client, endpoint string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, errors.New("invalid weather request")
	}
	req.Header.Set("User-Agent", "otelcol-extra-weather/1 (+https://github.com/ymotongpoo/opentelemetry-collector-extra)")
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("weather HTTP request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("weather HTTP status %d", resp.StatusCode)
	}
	const limit = 8 << 20
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, errors.New("reading weather response failed")
	}
	if len(data) > limit {
		return nil, errors.New("weather response exceeds 8 MiB")
	}
	return data, nil
}
