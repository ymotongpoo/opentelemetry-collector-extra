// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package smartheimreceiver

import (
	"context"
	"errors"

	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/smartheimreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

func NewFactory() receiver.Factory {
	return receiver.NewFactory(metadata.Type, createDefaultConfig, receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability))
}

func createMetricsReceiver(_ context.Context, settings receiver.Settings, cfg component.Config, next consumer.Metrics) (receiver.Metrics, error) {
	if next == nil {
		return nil, errors.New("nil next consumer")
	}
	c := cfg.(*Config)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return newReceiver(c, settings, next)
}
