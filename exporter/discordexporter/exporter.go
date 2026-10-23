// Copyright 2023 Yoshi Yamaguchi
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package discordexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type discordExporter struct {
	config     *Config
	client     *http.Client
	endpoint   string
	gate       chan struct{}
	nextSend   time.Time // Accessed only while holding gate.
	retryDelay time.Duration
}

func newDiscordExporter(cfg *Config) *discordExporter {
	return &discordExporter{config: cfg, client: &http.Client{Timeout: cfg.Timeout}, endpoint: "https://discord.com/api/v10/channels/" + url.PathEscape(cfg.Channel) + "/messages",
		gate: make(chan struct{}, 1), retryDelay: time.Second}
}

func (e *discordExporter) Shutdown(context.Context) error {
	e.client.CloseIdleConnections()
	return nil
}

func (e *discordExporter) pushMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	if metrics.DataPointCount() == 0 {
		return nil
	}
	buf, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(metrics)
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	text := string(buf)

	parts := splitMessage(text, 1900)
	// Serialize batches so their parts stay together. Cancellation also interrupts this wait.
	select {
	case e.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-e.gate }()
	for i, part := range parts {
		text := fmt.Sprintf("[OTLP metrics %d/%d]\n%s", i+1, len(parts), part)
		if err := e.sendWithRetry(ctx, text); err != nil {
			err = fmt.Errorf("delivered %d of %d message parts: %w", i, len(parts), err)
			if i > 0 {
				// A caller retrying the entire batch would duplicate the delivered prefix.
				return consumererror.NewPermanent(err)
			}
			return err
		}
	}
	return nil
}

func (e *discordExporter) sendWithRetry(ctx context.Context, text string) error {
	for attempt := 0; ; attempt++ {
		if err := wait(ctx, time.Until(e.nextSend)); err != nil {
			return err
		}
		delay, err := e.send(ctx, text)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if delay > 0 {
			e.nextSend = time.Now().Add(delay)
		}
		if consumererror.IsPermanent(err) || attempt >= e.config.MaxRetries {
			return err
		}
		backoff := e.retryDelay * time.Duration(1<<attempt)
		if delay < backoff {
			delay = backoff
		}
		e.nextSend = time.Now().Add(delay)
	}
}

func (e *discordExporter) send(ctx context.Context, text string) (time.Duration, error) {
	payload := map[string]any{"content": text, "allowed_mentions": map[string]any{"parse": []string{}}, "flags": 4}
	buf, err := json.Marshal(payload)
	if err != nil {
		return 0, consumererror.NewPermanent(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(buf))
	if err != nil {
		return 0, consumererror.NewPermanent(err)
	}
	request.Header.Set("Authorization", "Bot "+string(e.config.Token))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "DiscordBot (https://github.com/ymotongpoo/opentelemetry-collector-extra, 0.1.0)")
	response, err := e.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		return 0, err
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		if response.Header.Get("X-RateLimit-Remaining") == "0" {
			e.nextSend = time.Now().Add(seconds(response.Header.Get("X-RateLimit-Reset-After")))
		}
		return 0, nil
	}
	delay := seconds(response.Header.Get("Retry-After"))
	if response.StatusCode == http.StatusTooManyRequests {
		var rate struct {
			RetryAfter float64 `json:"retry_after"`
		}
		if json.Unmarshal(body, &rate) == nil && rate.RetryAfter > 0 {
			delay = seconds(strconv.FormatFloat(rate.RetryAfter, 'f', -1, 64))
		}
		return delay, fmt.Errorf("Discord rate limit (HTTP 429)")
	}
	err = fmt.Errorf("Discord returned HTTP %d", response.StatusCode)
	if response.StatusCode >= 500 || response.StatusCode == http.StatusRequestTimeout {
		return 0, err
	}
	return 0, consumererror.NewPermanent(err)
}

func wait(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func seconds(value string) time.Duration {
	n, err := strconv.ParseFloat(value, 64)
	if err != nil || n <= 0 {
		return 0
	}
	// Clamp server-controlled waits to avoid duration overflow.
	if n > 86400 {
		n = 86400
	}
	return time.Duration(n * float64(time.Second))
}

// splitMessage reserves space for part headers and never splits a UTF-8 sequence.
// Count supplementary characters as two units, also satisfying UTF-16 based limits.
func splitMessage(text string, limit int) []string {
	var parts []string
	start, used := 0, 0
	for index, r := range text {
		units := 1
		if r > 0xffff {
			units = 2
		}
		if used+units > limit {
			parts = append(parts, text[start:index])
			start = index
			used = 0
		}
		used += units
	}
	if start < len(text) {
		parts = append(parts, text[start:])
	}
	return parts
}
