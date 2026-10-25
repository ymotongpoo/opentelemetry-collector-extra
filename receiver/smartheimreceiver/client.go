// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package smartheimreceiver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/html"
)

const summaryPath = "/cgi-bin/inst_val_sv.cgi"
const branchesPath = "/cgi-bin/inst_val_all_branch_sv.cgi"
const maxPageBytes = 1 << 20

func newHTTPClient(timeout, connectTimeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // The device is local, including when HTTP_PROXY is set.
	transport.DialContext = (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext
	transport.MaxIdleConnsPerHost = 1
	transport.IdleConnTimeout = 30 * time.Second
	return &http.Client{Timeout: timeout, Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func fetchPage(ctx context.Context, client *http.Client, endpoint, path string) (*html.Node, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", "opentelemetry-collector-extra/smartheimreceiver")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPageBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxPageBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, maxPageBytes)
	}
	return document(bytes.NewReader(body), resp.Header.Get("Content-Type"))
}

func readSummary(ctx context.Context, client *http.Client, endpoint string) (summary, error) {
	root, err := fetchPage(ctx, client, endpoint, summaryPath)
	if err != nil {
		return summary{}, err
	}
	return parseSummary(root)
}
