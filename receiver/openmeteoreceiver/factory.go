// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package openmeteoreceiver

import (
	"context"
	"fmt"

	"github.com/ymotongpoo/opentelemetry-collector-extra/internal/weather"
	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/openmeteoreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

func NewFactory() receiver.Factory {
	return receiver.NewFactory(metadata.Type, createDefaultConfig, receiver.WithLogs(createLogsReceiver, metadata.LogsStability))
}

func createLogsReceiver(_ context.Context, settings receiver.Settings, cfg component.Config, next consumer.Logs) (receiver.Logs, error) {
	c := cfg.(*Config)
	if err := c.Validate(); err != nil {
		return nil, err
	}
	s := newSource(c)
	namespace := fmt.Sprintf("openmeteo|%s|%g|%g|%s|%s|%s", c.Endpoint, *c.Latitude, *c.Longitude, c.LocationID, c.Model, c.Timezone)
	return weather.NewReceiver(c.Config, namespace, "openmeteo", settings, next, s.collect)
}
