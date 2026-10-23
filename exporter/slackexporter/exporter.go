// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package slackexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type slackExporter struct {
	config     *Config
	client     *http.Client
	endpoint   string
	gate       chan struct{}
	nextSend   time.Time // Accessed only while holding gate.
	retryDelay time.Duration
}

func newSlackExporter(cfg *Config) *slackExporter {
	return &slackExporter{config: cfg, client: &http.Client{Timeout: cfg.Timeout}, endpoint: "https://slack.com/api/chat.postMessage",
		gate: make(chan struct{}, 1), retryDelay: time.Second}
}

func (e *slackExporter) Shutdown(context.Context) error { e.client.CloseIdleConnections(); return nil }

func (e *slackExporter) pushMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	if metrics.DataPointCount() == 0 {
		return nil
	}
	buf, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(metrics)
	if err != nil {
		return consumererror.NewPermanent(err)
	}
	text := string(buf)
	// Escape Slack's special characters within JSON strings before splitting.
	// The resulting text remains valid JSON and cannot contain Slack mention syntax.
	text = strings.NewReplacer("&", `\u0026`, "<", `\u003c`, ">", `\u003e`).Replace(text)
	parts := splitMessage(text, 3900)
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

func (e *slackExporter) sendWithRetry(ctx context.Context, text string) error {
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

func (e *slackExporter) send(ctx context.Context, text string) (time.Duration, error) {
	payload := map[string]any{"channel": e.config.Channel, "text": text, "mrkdwn": false, "parse": "none", "link_names": false, "unfurl_links": false, "unfurl_media": false}
	buf, err := json.Marshal(payload)
	if err != nil {
		return 0, consumererror.NewPermanent(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(buf))
	if err != nil {
		return 0, consumererror.NewPermanent(err)
	}
	request.Header.Set("Authorization", "Bearer "+string(e.config.BotToken))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "OpenTelemetry-Collector-Extra (https://github.com/ymotongpoo/opentelemetry-collector-extra)")
	response, err := e.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
	if err != nil {
		return 0, err
	}
	delay := seconds(response.Header.Get("Retry-After"))
	if response.StatusCode == http.StatusTooManyRequests {
		return delay, errors.New("Slack rate limit (HTTP 429)")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		err = fmt.Errorf("Slack returned HTTP %d", response.StatusCode)
		if response.StatusCode >= 500 || response.StatusCode == http.StatusRequestTimeout {
			return 0, err
		}
		return 0, consumererror.NewPermanent(err)
	}
	var result struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("decode Slack response: %w", err)
	}
	if result.OK {
		// Slack allows approximately one message per second per channel.
		e.nextSend = time.Now().Add(time.Second)
		return 0, nil
	}
	err = fmt.Errorf("Slack API error: %s", result.Error)
	switch result.Error {
	case "ratelimited", "rate_limited":
		return delay, err
	case "internal_error", "fatal_error", "service_unavailable", "request_timeout":
		return 0, err
	default:
		return 0, consumererror.NewPermanent(err)
	}
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
