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
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		TableName:      "test-table",
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

// TestNewS3TablesExporter_WithValidTableBucketArn tests exporter initialization with valid TableBucketArn
// Requirements: 1.4
func TestNewS3TablesExporter_WithValidTableBucketArn(t *testing.T) {
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-west-2:987654321098:bucket/my-table-bucket",
		Region:         "us-west-2",
		Namespace:      "production",
		TableName:      "telemetry-data",
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))

	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() with valid TableBucketArn failed: %v", err)
	}
	if exporter == nil {
		t.Fatal("newS3TablesExporter() returned nil")
	}
	if exporter.config.TableBucketArn != cfg.TableBucketArn {
		t.Errorf("expected TableBucketArn to be '%s', got '%s'", cfg.TableBucketArn, exporter.config.TableBucketArn)
	}
}

// TestNewS3TablesExporter_WithEmptyTableBucketArn tests that exporter initialization fails with empty TableBucketArn
// Requirements: 1.4
func TestNewS3TablesExporter_WithEmptyTableBucketArn(t *testing.T) {
	cfg := &Config{
		TableBucketArn: "",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		TableName:      "test-table",
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))

	exporter, err := newS3TablesExporter(cfg, set)
	if err == nil {
		t.Fatal("newS3TablesExporter() with empty TableBucketArn should have failed")
	}
	if exporter != nil {
		t.Error("newS3TablesExporter() should return nil when TableBucketArn is empty")
	}
	expectedMsg := "table_bucket_arn is required"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestPushMetrics(t *testing.T) {
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		TableName:      "test-table",
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
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		TableName:      "test-table",
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
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		TableName:      "test-table",
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
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		TableName:      "test-table",
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

// TestUploadToS3Tables_LogsIncludeTableBucketArn tests that logs include the configured TableBucketArn
// Requirements: 4.1
// Note: このテストは実際のログ出力を検証するのではなく、エクスポーターが正しく設定されていることを確認します
func TestUploadToS3Tables_LogsIncludeTableBucketArn(t *testing.T) {
	testARN := "arn:aws:s3tables:ap-northeast-1:111222333444:bucket/production-bucket"
	cfg := &Config{
		TableBucketArn: testARN,
		Region:         "ap-northeast-1",
		Namespace:      "production",
		TableName:      "metrics-data",
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	// エクスポーターの設定にTableBucketArnが含まれていることを確認
	if exporter.config.TableBucketArn != testARN {
		t.Errorf("expected exporter config to have TableBucketArn '%s', got '%s'", testARN, exporter.config.TableBucketArn)
	}

	// uploadToS3Tablesを呼び出してエラーが発生しないことを確認
	// 実際のログ出力はloggerによって処理されるため、ここでは設定が正しく渡されることを確認
	err = exporter.uploadToS3Tables(context.Background(), []byte("test data"), "test")
	if err != nil {
		t.Errorf("uploadToS3Tables() failed: %v", err)
	}
}