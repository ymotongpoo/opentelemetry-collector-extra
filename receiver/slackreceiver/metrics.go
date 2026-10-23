// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package slackreceiver

import (
	"context"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/slackreceiver/internal/metadata"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const deduplicationWindow = 8192

type channelTotals struct{ count, characters int64 }

// messageStats retains cumulative totals and a bounded window of message IDs.
// Event callbacks never call the downstream consumer.
type messageStats struct {
	mu      sync.Mutex
	stopped bool
	totals  map[string]map[string]channelTotals
	seen    map[string]struct{}
	recent  [deduplicationWindow]string
	next    int
}

func (s *messageStats) record(cfg *Config, server, channel, id, content string) {
	if channel == "" {
		return
	}
	if len(cfg.Channels) > 0 && !slices.Contains(cfg.Channels, channel) {
		return
	}
	if cfg.ServerWide {
		channel = "@all@"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	if s.totals == nil {
		s.totals = make(map[string]map[string]channelTotals)
		s.seen = make(map[string]struct{})
	}
	if id != "" {
		if _, ok := s.seen[id]; ok {
			return
		}
		delete(s.seen, s.recent[s.next])
		s.recent[s.next] = id
		s.next = (s.next + 1) % len(s.recent)
		s.seen[id] = struct{}{}
	}
	if s.totals[server] == nil {
		s.totals[server] = make(map[string]channelTotals)
	}
	total := s.totals[server][channel]
	total.count++
	total.characters += int64(utf8.RuneCountInString(content))
	s.totals[server][channel] = total
}

func (s *messageStats) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
}

func (s *messageStats) metrics(mb *metadata.MetricsBuilder) pmetric.Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := pcommon.NewTimestampFromTime(time.Now())
	servers := make([]string, 0, len(s.totals))
	for server := range s.totals {
		servers = append(servers, server)
	}
	slices.Sort(servers)
	for _, server := range servers {
		channels := make([]string, 0, len(s.totals[server]))
		for channel := range s.totals[server] {
			channels = append(channels, channel)
		}
		slices.Sort(channels)
		for _, channel := range channels {
			total := s.totals[server][channel]
			mb.RecordSlackMessagesCountDataPoint(now, total.count, channel)
			mb.RecordSlackMessagesLengthDataPoint(now, total.characters, channel)
		}
		resource := pcommon.NewResource()
		resource.Attributes().PutStr("slack.team.id", server)
		mb.EmitForResource(metadata.WithResource(resource))
	}
	return mb.Emit()
}

func (r *slackReceiver) flush(ctx context.Context) error {
	metrics := r.stats.metrics(r.mb)
	if metrics.DataPointCount() == 0 {
		return nil
	}
	ctx = r.obsrecv.StartMetricsOp(ctx)
	count := metrics.DataPointCount()
	err := r.consumer.ConsumeMetrics(ctx, metrics)
	r.obsrecv.EndMetricsOp(ctx, "slack", count, err)
	return err
}
