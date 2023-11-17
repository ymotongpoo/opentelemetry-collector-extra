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
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	metadata "github.com/ymotongpoo/opentelemetry-collector-extra/exporter/discordexporter/internal/metadata"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDeafultConfig,
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	)
}

func createMetricsExporter(
	ctx context.Context,
	param exporter.CreateSettings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	de, err := newDiscordExporter(c)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetricsExporter(
		ctx,
		param,
		cfg,
		de.pushMetrics,
		exporterhelper.WithShutdown(de.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}
