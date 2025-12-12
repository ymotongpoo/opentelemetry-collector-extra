// Copyright 2025 Yoshi Yamaguchi <ymotongpoo@gmail.com>
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

package s3tablesexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// typeStr is the type string for the S3 Tables exporter.
	typeStr   = "s3tables"
	// stability defines the stability level of this exporter.
	stability = component.StabilityLevelDevelopment
)

// NewFactory creates a new S3 Tables exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, stability),
		exporter.WithTraces(createTracesExporter, stability),
		exporter.WithLogs(createLogsExporter, stability),
	)
}

// createMetricsExporter creates a metrics exporter for S3 Tables.
func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	exp, err := newS3TablesExporter(c, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exp.pushMetrics,
	)
}

// createTracesExporter creates a traces exporter for S3 Tables.
func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)
	exp, err := newS3TablesExporter(c, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		exp.pushTraces,
	)
}

// createLogsExporter creates a logs exporter for S3 Tables.
func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c := cfg.(*Config)
	exp, err := newS3TablesExporter(c, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exp.pushLogs,
	)
}