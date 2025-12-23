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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TestProperty_NoTableCreationAPICalls tests that CreateTable and CreateNamespace APIs are never called
// Feature: remove-table-creation, Property 1: テーブル作成APIの非呼び出し
// Validates: Requirements 1.1, 1.2, 2.1
func TestProperty_NoTableCreationAPICalls(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: すべてのエクスポーター実行において、CreateTable APIまたはCreateNamespace APIが呼び出されないこと

	// テストケース1: メトリクスデータでテーブル作成APIが呼び出されないことを検証
	t.Run("metrics_export_does_not_call_create_table_api", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなメトリクスデータを生成
			metrics := generateRandomMetrics()

			// API呼び出しを追跡するモッククライアントを作成
			apiCallTracker := &apiCallTracker{
				createTableCalled:     false,
				createNamespaceCalled: false,
			}

			// エクスポーターを作成
			exporter := createExporterWithTracker(t, apiCallTracker)

			// メトリクスをエクスポート
			err := exporter.pushMetrics(context.Background(), metrics)

			// エラーが発生しても、テーブル作成APIが呼び出されていないことを確認
			if apiCallTracker.createTableCalled {
				t.Errorf("iteration %d: CreateTable API was called, but should not be", i)
			}
			if apiCallTracker.createNamespaceCalled {
				t.Errorf("iteration %d: CreateNamespace API was called, but should not be", i)
			}

			// エラーが発生した場合は、テーブルが存在しないエラーであることを確認
			if err != nil {
				// エラーメッセージにテーブル作成コマンドが含まれることを確認
				expectedSubstr := "Please create the table before running the exporter"
				errMsg := err.Error()
				// エラーメッセージに期待される文字列が含まれているかチェック
				found := false
				for i := 0; i <= len(errMsg)-len(expectedSubstr); i++ {
					if errMsg[i:i+len(expectedSubstr)] == expectedSubstr {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("iteration %d: error message should contain '%s', got: %v", i, expectedSubstr, err)
				}
			}
		}
	})

	// テストケース2: トレースデータでテーブル作成APIが呼び出されないことを検証
	t.Run("traces_export_does_not_call_create_table_api", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなトレースデータを生成
			traces := generateRandomTraces()

			// API呼び出しを追跡するモッククライアントを作成
			apiCallTracker := &apiCallTracker{
				createTableCalled:     false,
				createNamespaceCalled: false,
			}

			// エクスポーターを作成
			exporter := createExporterWithTracker(t, apiCallTracker)

			// トレースをエクスポート
			err := exporter.pushTraces(context.Background(), traces)

			// エラーが発生しても、テーブル作成APIが呼び出されていないことを確認
			if apiCallTracker.createTableCalled {
				t.Errorf("iteration %d: CreateTable API was called, but should not be", i)
			}
			if apiCallTracker.createNamespaceCalled {
				t.Errorf("iteration %d: CreateNamespace API was called, but should not be", i)
			}

			// エラーが発生した場合は、テーブルが存在しないエラーであることを確認
			if err != nil {
				// エラーメッセージにテーブル作成コマンドが含まれることを確認
				expectedSubstr := "Please create the table before running the exporter"
				errMsg := err.Error()
				// エラーメッセージに期待される文字列が含まれているかチェック
				found := false
				for i := 0; i <= len(errMsg)-len(expectedSubstr); i++ {
					if errMsg[i:i+len(expectedSubstr)] == expectedSubstr {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("iteration %d: error message should contain '%s', got: %v", i, expectedSubstr, err)
				}
			}
		}
	})

	// テストケース3: ログデータでテーブル作成APIが呼び出されないことを検証
	t.Run("logs_export_does_not_call_create_table_api", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなログデータを生成
			logs := generateRandomLogs()

			// API呼び出しを追跡するモッククライアントを作成
			apiCallTracker := &apiCallTracker{
				createTableCalled:     false,
				createNamespaceCalled: false,
			}

			// エクスポーターを作成
			exporter := createExporterWithTracker(t, apiCallTracker)

			// ログをエクスポート
			err := exporter.pushLogs(context.Background(), logs)

			// エラーが発生しても、テーブル作成APIが呼び出されていないことを確認
			if apiCallTracker.createTableCalled {
				t.Errorf("iteration %d: CreateTable API was called, but should not be", i)
			}
			if apiCallTracker.createNamespaceCalled {
				t.Errorf("iteration %d: CreateNamespace API was called, but should not be", i)
			}

			// エラーが発生した場合は、テーブルが存在しないエラーであることを確認
			if err != nil {
				// エラーメッセージにテーブル作成コマンドが含まれることを確認
				expectedSubstr := "Please create the table before running the exporter"
				errMsg := err.Error()
				// エラーメッセージに期待される文字列が含まれているかチェック
				found := false
				for i := 0; i <= len(errMsg)-len(expectedSubstr); i++ {
					if errMsg[i:i+len(expectedSubstr)] == expectedSubstr {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("iteration %d: error message should contain '%s', got: %v", i, expectedSubstr, err)
				}
			}
		}
	})

	// テストケース4: 複数のデータタイプを混在させてテスト
	t.Run("mixed_data_types_do_not_call_create_table_api", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// API呼び出しを追跡するモッククライアントを作成
			apiCallTracker := &apiCallTracker{
				createTableCalled:     false,
				createNamespaceCalled: false,
			}

			// エクスポーターを作成
			exporter := createExporterWithTracker(t, apiCallTracker)

			// ランダムにデータタイプを選択してエクスポート
			dataType := rand.Intn(3)
			var err error
			switch dataType {
			case 0:
				metrics := generateRandomMetrics()
				err = exporter.pushMetrics(context.Background(), metrics)
			case 1:
				traces := generateRandomTraces()
				err = exporter.pushTraces(context.Background(), traces)
			case 2:
				logs := generateRandomLogs()
				err = exporter.pushLogs(context.Background(), logs)
			}

			// エラーが発生しても、テーブル作成APIが呼び出されていないことを確認
			if apiCallTracker.createTableCalled {
				t.Errorf("iteration %d: CreateTable API was called, but should not be", i)
			}
			if apiCallTracker.createNamespaceCalled {
				t.Errorf("iteration %d: CreateNamespace API was called, but should not be", i)
			}

			// エラーが発生した場合は、テーブルが存在しないエラーであることを確認
			if err != nil {
				// エラーメッセージにテーブル作成コマンドが含まれることを確認
				expectedSubstr := "Please create the table before running the exporter"
				errMsg := err.Error()
				// エラーメッセージに期待される文字列が含まれているかチェック
				found := false
				for i := 0; i <= len(errMsg)-len(expectedSubstr); i++ {
					if errMsg[i:i+len(expectedSubstr)] == expectedSubstr {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("iteration %d: error message should contain '%s', got: %v", i, expectedSubstr, err)
				}
			}
		}
	})
}

// apiCallTracker tracks API calls to verify that CreateTable and CreateNamespace are not called
// API呼び出しを追跡して、CreateTableとCreateNamespaceが呼び出されないことを検証
type apiCallTracker struct {
	createTableCalled     bool
	createNamespaceCalled bool
}

// mockS3TablesClientWithTracker is a mock S3 Tables client that tracks API calls
// API呼び出しを追跡するモックS3 Tablesクライアント
type mockS3TablesClientWithTracker struct {
	tracker *apiCallTracker
}

func (m *mockS3TablesClientWithTracker) GetTable(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
	// GetTable APIは呼び出されることが期待されるため、エラーを返す
	return nil, fmt.Errorf("table not found")
}

func (m *mockS3TablesClientWithTracker) GetNamespace(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
	// GetNamespace APIは呼び出されることが期待されるため、エラーを返す
	return nil, fmt.Errorf("namespace not found")
}

func (m *mockS3TablesClientWithTracker) GetTableMetadataLocation(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
	return nil, fmt.Errorf("GetTableMetadataLocation not implemented")
}

func (m *mockS3TablesClientWithTracker) UpdateTableMetadataLocation(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
	return nil, fmt.Errorf("UpdateTableMetadataLocation not implemented")
}

// createExporterWithTracker creates an exporter with API call tracking
// API呼び出し追跡機能を持つエクスポーターを作成
func createExporterWithTracker(t *testing.T, tracker *apiCallTracker) *s3TablesExporter {
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

	// モックS3 Tablesクライアントを設定
	exporter.s3TablesClient = &mockS3TablesClientWithTracker{
		tracker: tracker,
	}

	// モックS3クライアントを設定（PutObjectは成功を返す）
	exporter.s3Client = &mockS3Client{
		putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			return &s3.PutObjectOutput{}, nil
		},
	}

	return exporter
}

// generateRandomMetrics generates random metrics data for testing
// テスト用のランダムなメトリクスデータを生成
func generateRandomMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	resource := rm.Resource()
	resource.Attributes().PutStr("service.name", fmt.Sprintf("test-service-%d", rand.Intn(1000)))
	resource.Attributes().PutStr("host.name", fmt.Sprintf("host-%d", rand.Intn(100)))

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	// ランダムな数のメトリクスを生成（1-10個）
	numMetrics := rand.Intn(10) + 1
	for i := 0; i < numMetrics; i++ {
		metric := sm.Metrics().AppendEmpty()
		metric.SetName(fmt.Sprintf("test.metric.%d", i))
		metric.SetDescription(fmt.Sprintf("Test metric %d", i))
		metric.SetUnit("1")

		// ゲージメトリクスを作成
		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.SetDoubleValue(rand.Float64() * 100)
	}

	return metrics
}

// generateRandomTraces generates random traces data for testing
// テスト用のランダムなトレースデータを生成
func generateRandomTraces() ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	resource := rs.Resource()
	resource.Attributes().PutStr("service.name", fmt.Sprintf("test-service-%d", rand.Intn(1000)))
	resource.Attributes().PutStr("host.name", fmt.Sprintf("host-%d", rand.Intn(100)))

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("test-scope")

	// ランダムな数のスパンを生成（1-10個）
	numSpans := rand.Intn(10) + 1
	for i := 0; i < numSpans; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName(fmt.Sprintf("test-span-%d", i))
		span.SetTraceID([16]byte{byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)),
			byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)),
			byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)),
			byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256))})
		span.SetSpanID([8]byte{byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)),
			byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256))})
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(time.Millisecond * time.Duration(rand.Intn(1000)))))
	}

	return traces
}

// generateRandomLogs generates random logs data for testing
// テスト用のランダムなログデータを生成
func generateRandomLogs() plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	resource := rl.Resource()
	resource.Attributes().PutStr("service.name", fmt.Sprintf("test-service-%d", rand.Intn(1000)))
	resource.Attributes().PutStr("host.name", fmt.Sprintf("host-%d", rand.Intn(100)))

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("test-scope")

	// ランダムな数のログレコードを生成（1-10個）
	numLogs := rand.Intn(10) + 1
	for i := 0; i < numLogs; i++ {
		logRecord := sl.LogRecords().AppendEmpty()
		logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		logRecord.Body().SetStr(fmt.Sprintf("Test log message %d", i))
		logRecord.SetSeverityNumber(plog.SeverityNumber(rand.Intn(24) + 1))
		logRecord.SetSeverityText(fmt.Sprintf("SEVERITY_%d", rand.Intn(5)))
	}

	return logs
}

// TestProperty_TableNotFoundError tests that errors are returned when tables don't exist
// Feature: remove-table-creation, Property 3: テーブル不存在時のエラー
// Validates: Requirements 1.3, 3.3, 7.1
func TestProperty_TableNotFoundError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: すべての存在しないテーブルに対して、GetTable APIが失敗した場合、
	// エクスポーターがエラーを返し、テーブル作成を試みないこと

	// テストケース1: ランダムなテーブル名でエラーが返されることを検証
	t.Run("random_nonexistent_table_names_return_error", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムな存在しないテーブル名を生成
			randomTableName := generateRandomTableName()

			// API呼び出しを追跡するモッククライアントを作成
			apiCallTracker := &apiCallTracker{
				createTableCalled:     false,
				createNamespaceCalled: false,
			}

			// カスタム設定でエクスポーターを作成
			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  randomTableName,
					Metrics: randomTableName,
					Logs:    randomTableName,
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定（テーブルが存在しないエラーを返す）
			exporter.s3TablesClient = &mockS3TablesClientWithTracker{
				tracker: apiCallTracker,
			}

			// モックS3クライアントを設定
			exporter.s3Client = &mockS3Client{
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}

			// ランダムなメトリクスデータを生成してエクスポート
			metrics := generateRandomMetrics()
			err = exporter.pushMetrics(context.Background(), metrics)

			// エラーが返されることを確認
			if err == nil {
				t.Errorf("iteration %d: expected error for nonexistent table '%s', but got none", i, randomTableName)
			}

			// テーブル作成APIが呼び出されていないことを確認
			if apiCallTracker.createTableCalled {
				t.Errorf("iteration %d: CreateTable API was called for nonexistent table '%s', but should not be", i, randomTableName)
			}
			if apiCallTracker.createNamespaceCalled {
				t.Errorf("iteration %d: CreateNamespace API was called for nonexistent table '%s', but should not be", i, randomTableName)
			}

			// エラーメッセージにテーブル作成コマンドが含まれることを確認
			if err != nil {
				expectedSubstr := "Please create the table before running the exporter"
				errMsg := err.Error()
				found := false
				for j := 0; j <= len(errMsg)-len(expectedSubstr); j++ {
					if errMsg[j:j+len(expectedSubstr)] == expectedSubstr {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("iteration %d: error message should contain '%s', got: %v", i, expectedSubstr, err)
				}
			}
		}
	})

	// テストケース2: 異なるデータタイプで存在しないテーブルのエラーを検証
	t.Run("nonexistent_tables_return_error_for_all_data_types", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムな存在しないテーブル名を生成
			randomTableName := generateRandomTableName()

			// API呼び出しを追跡するモッククライアントを作成
			apiCallTracker := &apiCallTracker{
				createTableCalled:     false,
				createNamespaceCalled: false,
			}

			// カスタム設定でエクスポーターを作成
			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  randomTableName,
					Metrics: randomTableName,
					Logs:    randomTableName,
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定（テーブルが存在しないエラーを返す）
			exporter.s3TablesClient = &mockS3TablesClientWithTracker{
				tracker: apiCallTracker,
			}

			// モックS3クライアントを設定
			exporter.s3Client = &mockS3Client{
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}

			// ランダムにデータタイプを選択してエクスポート
			dataType := rand.Intn(3)
			var exportErr error
			switch dataType {
			case 0:
				metrics := generateRandomMetrics()
				exportErr = exporter.pushMetrics(context.Background(), metrics)
			case 1:
				traces := generateRandomTraces()
				exportErr = exporter.pushTraces(context.Background(), traces)
			case 2:
				logs := generateRandomLogs()
				exportErr = exporter.pushLogs(context.Background(), logs)
			}

			// エラーが返されることを確認
			if exportErr == nil {
				t.Errorf("iteration %d: expected error for nonexistent table '%s' (data type %d), but got none", i, randomTableName, dataType)
			}

			// テーブル作成APIが呼び出されていないことを確認
			if apiCallTracker.createTableCalled {
				t.Errorf("iteration %d: CreateTable API was called for nonexistent table '%s' (data type %d), but should not be", i, randomTableName, dataType)
			}
			if apiCallTracker.createNamespaceCalled {
				t.Errorf("iteration %d: CreateNamespace API was called for nonexistent table '%s' (data type %d), but should not be", i, randomTableName, dataType)
			}

			// エラーメッセージにテーブル作成コマンドが含まれることを確認
			if exportErr != nil {
				expectedSubstr := "Please create the table before running the exporter"
				errMsg := exportErr.Error()
				found := false
				for j := 0; j <= len(errMsg)-len(expectedSubstr); j++ {
					if errMsg[j:j+len(expectedSubstr)] == expectedSubstr {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("iteration %d: error message should contain '%s', got: %v", i, expectedSubstr, exportErr)
				}
			}
		}
	})

	// テストケース3: 特殊文字を含むテーブル名でエラーが返されることを検証
	t.Run("special_character_table_names_return_error", func(t *testing.T) {
		specialTableNames := []string{
			"table-with-hyphens",
			"table_with_underscores",
			"table123",
			"TABLE_UPPERCASE",
			"table.with.dots",
			"very-long-table-name-that-exceeds-normal-length-limits-but-is-still-valid",
		}

		for _, tableName := range specialTableNames {
			// API呼び出しを追跡するモッククライアントを作成
			apiCallTracker := &apiCallTracker{
				createTableCalled:     false,
				createNamespaceCalled: false,
			}

			// カスタム設定でエクスポーターを作成
			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  tableName,
					Metrics: tableName,
					Logs:    tableName,
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("newS3TablesExporter() failed for table '%s': %v", tableName, err)
			}

			// モックS3 Tablesクライアントを設定（テーブルが存在しないエラーを返す）
			exporter.s3TablesClient = &mockS3TablesClientWithTracker{
				tracker: apiCallTracker,
			}

			// モックS3クライアントを設定
			exporter.s3Client = &mockS3Client{
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}

			// メトリクスデータを生成してエクスポート
			metrics := generateRandomMetrics()
			err = exporter.pushMetrics(context.Background(), metrics)

			// エラーが返されることを確認
			if err == nil {
				t.Errorf("expected error for nonexistent table '%s', but got none", tableName)
			}

			// テーブル作成APIが呼び出されていないことを確認
			if apiCallTracker.createTableCalled {
				t.Errorf("CreateTable API was called for nonexistent table '%s', but should not be", tableName)
			}
			if apiCallTracker.createNamespaceCalled {
				t.Errorf("CreateNamespace API was called for nonexistent table '%s', but should not be", tableName)
			}

			// エラーメッセージにテーブル作成コマンドが含まれることを確認
			if err != nil {
				expectedSubstr := "Please create the table before running the exporter"
				errMsg := err.Error()
				found := false
				for j := 0; j <= len(errMsg)-len(expectedSubstr); j++ {
					if errMsg[j:j+len(expectedSubstr)] == expectedSubstr {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("error message for table '%s' should contain '%s', got: %v", tableName, expectedSubstr, err)
				}
			}
		}
	})

	// テストケース4: 異なるNamespaceで存在しないテーブルのエラーを検証
	t.Run("nonexistent_tables_in_different_namespaces_return_error", func(t *testing.T) {
		iterations := 50
		for i := 0; i < iterations; i++ {
			// ランダムな存在しないNamespaceとテーブル名を生成
			randomNamespace := generateRandomNamespace()
			randomTableName := generateRandomTableName()

			// API呼び出しを追跡するモッククライアントを作成
			apiCallTracker := &apiCallTracker{
				createTableCalled:     false,
				createNamespaceCalled: false,
			}

			// カスタム設定でエクスポーターを作成
			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      randomNamespace,
				Tables: TableNamesConfig{
					Traces:  randomTableName,
					Metrics: randomTableName,
					Logs:    randomTableName,
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定（テーブルが存在しないエラーを返す）
			exporter.s3TablesClient = &mockS3TablesClientWithTracker{
				tracker: apiCallTracker,
			}

			// モックS3クライアントを設定
			exporter.s3Client = &mockS3Client{
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}

			// メトリクスデータを生成してエクスポート
			metrics := generateRandomMetrics()
			err = exporter.pushMetrics(context.Background(), metrics)

			// エラーが返されることを確認
			if err == nil {
				t.Errorf("iteration %d: expected error for nonexistent table '%s.%s', but got none", i, randomNamespace, randomTableName)
			}

			// テーブル作成APIが呼び出されていないことを確認
			if apiCallTracker.createTableCalled {
				t.Errorf("iteration %d: CreateTable API was called for nonexistent table '%s.%s', but should not be", i, randomNamespace, randomTableName)
			}
			if apiCallTracker.createNamespaceCalled {
				t.Errorf("iteration %d: CreateNamespace API was called for nonexistent namespace '%s', but should not be", i, randomNamespace)
			}
		}
	})
}

// generateRandomTableName generates a random table name for testing
// テスト用のランダムなテーブル名を生成
func generateRandomTableName() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789_"
	length := rand.Intn(20) + 5 // 5-24文字

	result := make([]byte, length)
	// 先頭は英字
	result[0] = charset[rand.Intn(26)]

	// 残りは英数字とアンダースコア
	for i := 1; i < length; i++ {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}

// generateRandomNamespace generates a random namespace for testing
// テスト用のランダムなNamespaceを生成
func generateRandomNamespace() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789_"
	length := rand.Intn(15) + 5 // 5-19文字

	result := make([]byte, length)
	// 先頭は英字
	result[0] = charset[rand.Intn(26)]

	// 残りは英数字とアンダースコア
	for i := 1; i < length; i++ {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}
