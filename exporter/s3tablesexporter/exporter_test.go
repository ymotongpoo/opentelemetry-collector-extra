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
	"testing"

	"go.opentelemetry.io/collector/component"

	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestNewS3TablesExporter(t *testing.T) {
	cfg := &Config{
		Region:    "us-east-1",
		Namespace: "test-namespace",
		TableName: "test-table",
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))

	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}
	if exporter == nil {
		t.Fatal("newS3TablesExporter() returned nil")
	}
	if exporter.config != cfg {
		t.Error("config not set correctly")
	}
}

func TestPushMetrics(t *testing.T) {
	cfg := &Config{
		Region:    "us-east-1",
		Namespace: "test-namespace",
		TableName: "test-table",
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("test-metric")
	gauge := metric.SetEmptyGauge()
	dp := gauge.DataPoints().AppendEmpty()
	dp.SetDoubleValue(42.0)

	err = exporter.pushMetrics(context.Background(), md)
	if err != nil {
		t.Errorf("pushMetrics() failed: %v", err)
	}
}

func TestPushTraces(t *testing.T) {
	cfg := &Config{
		Region:    "us-east-1",
		Namespace: "test-namespace",
		TableName: "test-table",
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-service")
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("test-span")

	err = exporter.pushTraces(context.Background(), td)
	if err != nil {
		t.Errorf("pushTraces() failed: %v", err)
	}
}

func TestPushLogs(t *testing.T) {
	cfg := &Config{
		Region:    "us-east-1",
		Namespace: "test-namespace",
		TableName: "test-table",
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	sl := rl.ScopeLogs().AppendEmpty()
	log := sl.LogRecords().AppendEmpty()
	log.Body().SetStr("test log message")

	err = exporter.pushLogs(context.Background(), ld)
	if err != nil {
		t.Errorf("pushLogs() failed: %v", err)
	}
}

func TestUploadToS3Tables(t *testing.T) {
	cfg := &Config{
		Region:    "us-east-1",
		Namespace: "test-namespace",
		TableName: "test-table",
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	// Test with empty data
	err = exporter.uploadToS3Tables(context.Background(), []byte{}, "test")
	if err != nil {
		t.Errorf("uploadToS3Tables() with empty data failed: %v", err)
	}

	// Test with data
	err = exporter.uploadToS3Tables(context.Background(), []byte("test data"), "test")
	if err != nil {
		t.Errorf("uploadToS3Tables() with data failed: %v", err)
	}
}