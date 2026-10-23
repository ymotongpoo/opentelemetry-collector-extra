// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package discordreceiver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/discordreceiver/internal/metadata"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func testConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.Token = "test"
	return cfg
}

func testReceiver(t *testing.T, next consumer.Metrics) *discordReceiver {
	t.Helper()
	r, err := newDiscordReceiver(testConfig(), receivertest.NewNopSettings(metadata.Type), next)
	require.NoError(t, err)
	return r
}

func TestConfigDecodeAndValidate(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"token":           "test",
		"buffer_interval": "25ms", "channels": []any{"C1"},
		"metrics": map[string]any{"discord.messages.length": map[string]any{"enabled": false}},
	}).Unmarshal(cfg))
	require.NoError(t, cfg.Validate())
	require.Equal(t, 25*time.Millisecond, cfg.BufferInterval)
	require.False(t, cfg.Metrics.DiscordMessagesLength.Enabled)
	for _, interval := range []time.Duration{0, -time.Second} {
		cfg.BufferInterval = interval
		require.Error(t, cfg.Validate())
	}
	cfg = testConfig()
	cfg.ServerWide = true
	cfg.Channels = []string{"C1"}
	require.Error(t, cfg.Validate())
	cfg = testConfig()
	cfg.Channels = []string{" "}
	require.Error(t, cfg.Validate())
	require.Error(t, createDefaultConfig().(*Config).Validate())
	_, err := createMetricsReceiver(context.Background(), receivertest.NewNopSettings(metadata.Type), testConfig(), nil)
	require.Error(t, err)
}

func TestCumulativeTotalsAndResourceIsolation(t *testing.T) {
	sink := &consumertest.MetricsSink{}
	r := testReceiver(t, sink)
	r.stats.record(r.config, "S1", "C1", "1", "日本語😀")
	r.stats.record(r.config, "S1", "C1", "2", "ab")
	r.stats.record(r.config, "S1", "C1", "2", "duplicate")
	r.stats.record(r.config, "S2", "C1", "3", "x")
	require.NoError(t, r.flush(context.Background()))
	first := sink.AllMetrics()[0]
	require.Equal(t, 2, first.ResourceMetrics().Len())
	metrics := first.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, int64(2), metrics.At(0).Sum().DataPoints().At(0).IntValue())
	require.Equal(t, int64(6), metrics.At(1).Sum().DataPoints().At(0).IntValue())
	require.Equal(t, 1, metrics.At(0).Sum().DataPoints().Len())
	require.Equal(t, pmetric.AggregationTemporalityCumulative, metrics.At(0).Sum().AggregationTemporality())
	require.True(t, metrics.At(0).Sum().IsMonotonic())
	start := metrics.At(0).Sum().DataPoints().At(0).StartTimestamp()
	require.NotZero(t, start)
	server, ok := first.ResourceMetrics().At(0).Resource().Attributes().Get("discord.guild.id")
	require.True(t, ok)
	require.Equal(t, "S1", server.Str())
	r.stats.record(r.config, "S1", "C1", "4", "z")
	require.NoError(t, r.flush(context.Background()))
	second := sink.AllMetrics()[1].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
	require.Equal(t, int64(3), second.At(0).IntValue())
	require.Equal(t, start, second.At(0).StartTimestamp())
	require.Equal(t, int64(2), metrics.At(0).Sum().DataPoints().At(0).IntValue(), "emission must not mutate previously delivered data")
}

func TestFiltersAndServerWide(t *testing.T) {
	for _, serverWide := range []bool{false, true} {
		t.Run(fmt.Sprint(serverWide), func(t *testing.T) {
			r := testReceiver(t, &consumertest.MetricsSink{})
			r.config.ServerWide = serverWide
			if !serverWide {
				r.config.Channels = []string{"C1"}
			}
			r.stats.record(r.config, "S1", "C1", "1", "a")
			r.stats.record(r.config, "S1", "C2", "2", "b")
			r.stats.record(r.config, "S2", "C2", "3", "c")
			m := r.stats.metrics(r.mb)
			dp := m.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0)
			attr, _ := dp.Attributes().Get("discord.channel.id")
			if serverWide {
				require.Equal(t, "@all@", attr.Str())
				require.Equal(t, int64(2), dp.IntValue())
				require.Equal(t, 2, m.ResourceMetrics().Len())
			} else {
				require.Equal(t, "C1", attr.Str())
				require.Equal(t, int64(1), dp.IntValue())
				require.Equal(t, 1, m.ResourceMetrics().Len())
			}
		})
	}
}

func TestConsumerFailureKeepsCumulativeTotals(t *testing.T) {
	fail := true
	next, err := consumer.NewMetrics(func(_ context.Context, md pmetric.Metrics) error {
		if fail {
			return errors.New("downstream unavailable")
		}
		require.Equal(t, int64(2), md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).IntValue())
		return nil
	})
	require.NoError(t, err)
	r := testReceiver(t, next)
	r.stats.record(r.config, "S1", "C1", "1", "a")
	require.Error(t, r.flush(context.Background()))
	fail = false
	r.stats.record(r.config, "S1", "C1", "2", "a")
	require.NoError(t, r.flush(context.Background()))
}

func TestConcurrentEventsAndBoundedDeduplication(t *testing.T) {
	r := testReceiver(t, &consumertest.MetricsSink{})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				r.stats.record(r.config, "S", "C", fmt.Sprintf("%d/%d", g, i), "😀")
			}
		}(g)
	}
	for i := 0; i < 10; i++ {
		_ = r.stats.metrics(r.mb)
	}
	wg.Wait()
	require.Len(t, r.stats.seen, deduplicationWindow)
	md := r.stats.metrics(r.mb)
	require.Equal(t, int64(16000), md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).IntValue())
	r.stats.stop()
	r.stats.record(r.config, "S", "C", "after-stop", "a")
	require.Equal(t, int64(16000), r.stats.totals["S"]["C"].count)
}
