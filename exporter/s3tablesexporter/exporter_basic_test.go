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

	"github.com/aws/aws-sdk-go-v2/service/s3tables"
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
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
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
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
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
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
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
	t.Skip("Integration test: requires pre-created table in S3 Tables. Run with -integration flag.")
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
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

	// 実際のS3 Tables APIを呼び出すため、統合テスト環境でのみ実行
	// テーブルは事前に作成されている必要がある
	err = exporter.pushMetrics(context.Background(), md)
	if err != nil {
		t.Logf("pushMetrics() returned error (expected in unit test): %v", err)
	}
}

func TestPushTraces(t *testing.T) {
	t.Skip("Integration test: requires pre-created table in S3 Tables. Run with -integration flag.")
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
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

	// 実際のS3 Tables APIを呼び出すため、統合テスト環境でのみ実行
	// テーブルは事前に作成されている必要がある
	err = exporter.pushTraces(context.Background(), td)
	if err != nil {
		t.Logf("pushTraces() returned error (expected in unit test): %v", err)
	}
}

func TestPushLogs(t *testing.T) {
	t.Skip("Integration test: requires pre-created table in S3 Tables. Run with -integration flag.")
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
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

	// 実際のS3 Tables APIを呼び出すため、統合テスト環境でのみ実行
	// テーブルは事前に作成されている必要がある
	err = exporter.pushLogs(context.Background(), ld)
	if err != nil {
		t.Logf("pushLogs() returned error (expected in unit test): %v", err)
	}
}

// TestNewS3TablesExporter_CatalogFieldsInitialized tests that configuration fields are initialized
// Requirements: 3.1, 3.2
// Note: このテストはテーブルが事前に作成されている前提で、設定が正しく初期化されることを検証します
func TestNewS3TablesExporter_CatalogFieldsInitialized(t *testing.T) {
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-west-2:123456789012:bucket/test-bucket",
		Region:         "us-west-2",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
	}

	// 設定に必要なパラメータが正しく設定されていることを確認
	if cfg.TableBucketArn == "" {
		t.Error("TableBucketArn should not be empty")
	}
	if cfg.Region == "" {
		t.Error("Region should not be empty")
	}
	if cfg.Namespace == "" {
		t.Error("Namespace should not be empty")
	}

	// エクスポーターが正しく初期化されることを確認
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}
	if exporter == nil {
		t.Fatal("newS3TablesExporter() returned nil")
	}
}

// TestGetTableInfo_WithMock tests that getTableInfo works correctly with mocked GetTable
// Requirements: 3.1, 3.2
func TestGetTableInfo_WithMock(t *testing.T) {
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
	}
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
	exporter, err := newS3TablesExporter(cfg, set)
	if err != nil {
		t.Fatalf("newS3TablesExporter() failed: %v", err)
	}

	// モックのS3TablesClientを設定
	mockClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			// テーブルが存在する場合の成功レスポンスを返す
			tableArn := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/otel_metrics"
			warehouseLocation := "s3://test-warehouse-bucket/data"
			versionToken := "test-version-token"
			return &s3tables.GetTableOutput{
				TableARN:          &tableArn,
				WarehouseLocation: &warehouseLocation,
				VersionToken:      &versionToken,
			}, nil
		},
	}
	exporter.s3TablesClient = mockClient

	// getTableInfoを呼び出してテーブル情報を取得
	tableInfo, err := exporter.getTableInfo(context.Background(), "test-namespace", "otel_metrics")
	if err != nil {
		t.Fatalf("getTableInfo() failed: %v", err)
	}

	// テーブル情報が正しく取得されることを確認
	if tableInfo.WarehouseLocation != "s3://test-warehouse-bucket/data" {
		t.Errorf("expected warehouse location 's3://test-warehouse-bucket/data', got '%s'", tableInfo.WarehouseLocation)
	}
	if tableInfo.VersionToken != "test-version-token" {
		t.Errorf("expected version token 'test-version-token', got '%s'", tableInfo.VersionToken)
	}
}

// TestExtractBucketNameFromArn tests bucket name extraction from Table Bucket ARN
// Requirements: 1.2
func TestExtractBucketNameFromArn(t *testing.T) {
	tests := []struct {
		name           string
		arn            string
		expectedBucket string
		expectError    bool
	}{
		{
			name:           "valid ARN",
			arn:            "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
			expectedBucket: "test-bucket",
			expectError:    false,
		},
		{
			name:           "valid ARN with hyphens",
			arn:            "arn:aws:s3tables:ap-northeast-1:987654321098:bucket/my-test-bucket-01",
			expectedBucket: "my-test-bucket-01",
			expectError:    false,
		},
		{
			name:        "invalid ARN format",
			arn:         "invalid-arn",
			expectError: true,
		},
		{
			name:        "missing bucket name",
			arn:         "arn:aws:s3tables:us-east-1:123456789012:bucket/",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, err := extractBucketNameFromArn(tt.arn)
			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if bucket != tt.expectedBucket {
					t.Errorf("expected bucket '%s', got '%s'", tt.expectedBucket, bucket)
				}
			}
		})
	}
}

// TestExtractBucketNameFromArn_ErrorContext tests that ARN extraction errors include context
// Requirements: 5.1, 5.4
func TestExtractBucketNameFromArn_ErrorContext(t *testing.T) {
	invalidArn := "invalid-arn-format"
	_, err := extractBucketNameFromArn(invalidArn)
	if err == nil {
		t.Fatal("extractBucketNameFromArn() should return error for invalid ARN")
	}

	// エラーメッセージに無効なARNが含まれることを確認
	if len(err.Error()) == 0 {
		t.Error("error message should not be empty")
	}
	expectedPrefix := "invalid Table Bucket ARN format"
	if len(err.Error()) < len(expectedPrefix) || err.Error()[:len(expectedPrefix)] != expectedPrefix {
		t.Errorf("expected error message to start with '%s', got '%s'", expectedPrefix, err.Error())
	}
}

// TestExtractBucketFromWarehouseLocation tests bucket name extraction from warehouse location
// Requirements: 1.2
func TestExtractBucketFromWarehouseLocation(t *testing.T) {
	tests := []struct {
		name              string
		warehouseLocation string
		expectedBucket    string
		expectError       bool
	}{
		{
			name:              "valid s3 URI",
			warehouseLocation: "s3://my-warehouse-bucket",
			expectedBucket:    "my-warehouse-bucket",
			expectError:       false,
		},
		{
			name:              "valid s3 URI with path",
			warehouseLocation: "s3://my-warehouse-bucket/data/path",
			expectedBucket:    "my-warehouse-bucket",
			expectError:       false,
		},
		{
			name:              "valid s3 URI with complex bucket name",
			warehouseLocation: "s3://63a8e430-6e0b-46f5-k833abtwr6s8tmtsycedn8s4yc3xhuse1b--table-s3",
			expectedBucket:    "63a8e430-6e0b-46f5-k833abtwr6s8tmtsycedn8s4yc3xhuse1b--table-s3",
			expectError:       false,
		},
		{
			name:              "invalid format - no s3 prefix",
			warehouseLocation: "https://bucket-name",
			expectError:       true,
		},
		{
			name:              "invalid format - empty",
			warehouseLocation: "",
			expectError:       true,
		},
		{
			name:              "invalid format - no bucket name",
			warehouseLocation: "s3://",
			expectError:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, err := extractBucketFromWarehouseLocation(tt.warehouseLocation)
			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if bucket != tt.expectedBucket {
					t.Errorf("expected bucket '%s', got '%s'", tt.expectedBucket, bucket)
				}
			}
		})
	}
}

// TestGenerateDataFileKey tests object key generation
// Requirements: 1.3
func TestGenerateDataFileKey(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
	}{
		{
			name:     "metrics data type",
			dataType: "metrics",
		},
		{
			name:     "traces data type",
			dataType: "traces",
		},
		{
			name:     "logs data type",
			dataType: "logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := generateDataFileKey(tt.dataType)

			// キーが "data/" で始まることを確認
			if len(key) < 5 || key[:5] != "data/" {
				t.Errorf("expected key to start with 'data/', got '%s'", key)
			}

			// キーが ".parquet" で終わることを確認
			if len(key) < 8 || key[len(key)-8:] != ".parquet" {
				t.Errorf("expected key to end with '.parquet', got '%s'", key)
			}

			// キーにタイムスタンプとUUIDが含まれることを確認（形式: data/{timestamp}-{uuid}.parquet）
			// タイムスタンプは20060102-150405形式（15文字）
			// UUIDは36文字
			// 最小長: 5 (data/) + 15 (timestamp) + 1 (-) + 36 (uuid) + 8 (.parquet) = 65
			if len(key) < 65 {
				t.Errorf("expected key length to be at least 65, got %d: '%s'", len(key), key)
			}
		})
	}
}

// TestGenerateDataFileKey_Uniqueness tests that generated keys are unique
// Requirements: 1.3
func TestGenerateDataFileKey_Uniqueness(t *testing.T) {
	// 複数のキーを生成して一意性を確認
	keys := make(map[string]bool)
	iterations := 100

	for i := 0; i < iterations; i++ {
		key := generateDataFileKey("metrics")
		if keys[key] {
			t.Errorf("duplicate key generated: '%s'", key)
		}
		keys[key] = true
	}

	if len(keys) != iterations {
		t.Errorf("expected %d unique keys, got %d", iterations, len(keys))
	}
}
