// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package switchbotreceiver

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

var errQuota = errors.New("SwitchBot request budget or cooldown is active")

type device struct {
	ID   string `json:"deviceId"`
	Name string `json:"deviceName"`
	Type string `json:"deviceType"`
}

type cloudClient struct {
	cfg      CloudConfig
	http     *http.Client
	mu       sync.Mutex
	requests []time.Time
	next     time.Time
	cooldown time.Time
}

func newCloudClient(cfg CloudConfig) *cloudClient {
	return &cloudClient{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (c *cloudClient) reserve(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if now.Before(c.cooldown) {
		return errQuota
	}
	first := 0
	for first < len(c.requests) && now.Sub(c.requests[first]) >= 24*time.Hour {
		first++
	}
	c.requests = c.requests[first:]
	if len(c.requests) >= c.cfg.DailyRequestLimit {
		return errQuota
	}
	if delay := time.Until(c.next); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.requests = append(c.requests, time.Now())
	c.next = time.Now().Add(c.cfg.MinRequestInterval)
	return nil
}

func (c *cloudClient) request(ctx context.Context, path string, out any) error {
	if err := c.reserve(ctx); err != nil {
		return err
	}
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	nonce := uuid.NewString()
	mac := hmac.New(sha256.New, []byte(c.cfg.Secret))
	_, _ = mac.Write([]byte(string(c.cfg.Token) + ts + nonce))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.cfg.Endpoint, "/")+path, nil)
	if err != nil {
		return errors.New("invalid SwitchBot request")
	}
	req.Header.Set("Authorization", string(c.cfg.Token))
	req.Header.Set("sign", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	req.Header.Set("t", ts)
	req.Header.Set("nonce", nonce)
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("SwitchBot HTTP request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 429 {
		delay := time.Hour
		if resp.StatusCode == 429 {
			delay = 5 * time.Minute
			if n, e := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 32); e == nil && n > 0 {
				delay = time.Duration(n) * time.Second
			} else if date, e := http.ParseTime(resp.Header.Get("Retry-After")); e == nil && time.Until(date) > 0 {
				delay = time.Until(date)
			}
		}
		c.mu.Lock()
		c.cooldown = time.Now().Add(min(delay, 24*time.Hour))
		c.mu.Unlock()
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SwitchBot HTTP status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024+1))
	if err != nil || len(data) > 1024*1024 {
		return errors.New("invalid SwitchBot response size")
	}
	var envelope struct {
		Status int             `json:"statusCode"`
		Body   json.RawMessage `json:"body"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return errors.New("invalid SwitchBot response")
	}
	if envelope.Status != 100 {
		return fmt.Errorf("SwitchBot API status %d", envelope.Status)
	}
	if len(envelope.Body) == 0 || string(envelope.Body) == "null" || json.Unmarshal(envelope.Body, out) != nil {
		return errors.New("invalid SwitchBot response body")
	}
	return nil
}

func (c *cloudClient) devices(ctx context.Context) ([]device, error) {
	var body struct {
		Devices []device `json:"deviceList"`
	}
	err := c.request(ctx, "/devices", &body)
	if err == nil && body.Devices == nil {
		err = errors.New("SwitchBot response has no deviceList")
	}
	return body.Devices, err
}

func (c *cloudClient) status(ctx context.Context, id string) (map[string]json.RawMessage, error) {
	var body map[string]json.RawMessage
	err := c.request(ctx, "/devices/"+url.PathEscape(id)+"/status", &body)
	return body, err
}
