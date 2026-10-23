// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package discordexporter

import (
	"context"

	"github.com/ymotongpoo/opentelemetry-collector-extra/exporter/discordexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// NewFactory creates the Discord metrics exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(metadata.Type, createDefaultConfig, exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability))
}

func createMetricsExporter(ctx context.Context, settings exporter.Settings, config component.Config) (exporter.Metrics, error) {
	cfg := config.(*Config)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	e := newDiscordExporter(cfg)
	return exporterhelper.NewMetrics(ctx, settings, cfg, e.pushMetrics,
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: cfg.Timeout}),
		// Retry individual messages in pushMetrics, avoiding replay of successful parts.
		exporterhelper.WithRetry(configretry.BackOffConfig{Enabled: false}),
		exporterhelper.WithShutdown(e.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}))
}
