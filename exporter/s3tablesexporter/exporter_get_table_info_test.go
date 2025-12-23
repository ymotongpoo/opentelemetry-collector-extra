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
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestGetTableInfo_CacheHit tests that cached table information is returned
// キャッシュが正しく動作することを検証
// Requirements: 3.1, 3.2
func TestGetTableInfo_CacheHit(t *testing.T) {
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

	// GetTable APIの呼び出し回数をカウント
	callCount := 0
	mockClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			callCount++
			return &s3tables.GetTableOutput{
				TableARN:          aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table"),
				WarehouseLocation: aws.String("s3://test-warehouse-bucket/test-namespace/test-table"),
				VersionToken:      aws.String("version-token-1"),
			}, nil
		},
	}
	exporter.s3TablesClient = mockClient

	// 1回目の呼び出し - キャッシュミス
	tableInfo1, err := exporter.getTableInfo(context.Background(), "test-namespace", "test-table")
	if err != nil {
		t.Fatalf("getTableInfo() failed on first call: %v", err)
	}

	if callCount != 1 {
		t.Errorf("expected GetTable to be called once, got %d calls", callCount)
	}

	// 2回目の呼び出し - キャッシュヒット
	tableInfo2, err := exporter.getTableInfo(context.Background(), "test-namespace", "test-table")
	if err != nil {
		t.Fatalf("getTableInfo() failed on second call: %v", err)
	}

	// GetTable APIが再度呼び出されないことを確認
	if callCount != 1 {
		t.Errorf("expected GetTable to be called only once (cached), got %d calls", callCount)
	}

	// 両方の呼び出しで同じ情報が返されることを確認
	if tableInfo1.TableARN != tableInfo2.TableARN {
		t.Errorf("cached TableARN mismatch: %s != %s", tableInfo1.TableARN, tableInfo2.TableARN)
	}
	if tableInfo1.WarehouseLocation != tableInfo2.WarehouseLocation {
		t.Errorf("cached WarehouseLocation mismatch: %s != %s", tableInfo1.WarehouseLocation, tableInfo2.WarehouseLocation)
	}
	if tableInfo1.VersionToken != tableInfo2.VersionToken {
		t.Errorf("cached VersionToken mismatch: %s != %s", tableInfo1.VersionToken, tableInfo2.VersionToken)
	}
}

// TestGetTableInfo_Success tests successful table information retrieval
// テーブルが存在する場合の正常系テスト
// Requirements: 3.1, 3.2
func TestGetTableInfo_Success(t *testing.T) {
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

	expectedTableARN := "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/test-table"
	expectedWarehouseLocation := "s3://test-warehouse-bucket/test-namespace/test-table"
	expectedVersionToken := "version-token-123"

	mockClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			// パラメータの検証
			if *params.TableBucketARN != cfg.TableBucketArn {
				t.Errorf("expected TableBucketARN %s, got %s", cfg.TableBucketArn, *params.TableBucketARN)
			}
			if *params.Namespace != "test-namespace" {
				t.Errorf("expected Namespace 'test-namespace', got %s", *params.Namespace)
			}
			if *params.Name != "test-table" {
				t.Errorf("expected Name 'test-table', got %s", *params.Name)
			}

			return &s3tables.GetTableOutput{
				TableARN:          aws.String(expectedTableARN),
				WarehouseLocation: aws.String(expectedWarehouseLocation),
				VersionToken:      aws.String(expectedVersionToken),
			}, nil
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル情報を取得
	tableInfo, err := exporter.getTableInfo(context.Background(), "test-namespace", "test-table")
	if err != nil {
		t.Fatalf("getTableInfo() failed: %v", err)
	}

	// 返された情報を検証
	if tableInfo.TableARN != expectedTableARN {
		t.Errorf("expected TableARN %s, got %s", expectedTableARN, tableInfo.TableARN)
	}
	if tableInfo.WarehouseLocation != expectedWarehouseLocation {
		t.Errorf("expected WarehouseLocation %s, got %s", expectedWarehouseLocation, tableInfo.WarehouseLocation)
	}
	if tableInfo.VersionToken != expectedVersionToken {
		t.Errorf("expected VersionToken %s, got %s", expectedVersionToken, tableInfo.VersionToken)
	}
}

// TestGetTableInfo_TableNotFound tests error handling when table does not exist
// テーブルが存在しない場合のエラーハンドリングテスト
// Requirements: 3.3, 7.1, 7.2
func TestGetTableInfo_TableNotFound(t *testing.T) {
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

	mockClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return nil, fmt.Errorf("ResourceNotFoundException: table not found")
		},
	}
	exporter.s3TablesClient = mockClient

	// テーブル情報の取得を試行
	_, err = exporter.getTableInfo(context.Background(), "test-namespace", "nonexistent-table")
	if err == nil {
		t.Fatal("expected error when table does not exist")
	}

	// エラーメッセージの内容を検証
	errMsg := err.Error()

	// エラーメッセージに必要な情報が含まれていることを確認
	expectedStrings := []string{
		"failed to get table information",
		"test-namespace.nonexistent-table",
		"Please create the table before running the exporter",
		"aws s3tables create-table",
		"--table-bucket-arn",
		cfg.TableBucketArn,
		"--namespace",
		"test-namespace",
		"--name",
		"nonexistent-table",
		"--format",
		"ICEBERG",
		"--region",
		cfg.Region,
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(errMsg, expected) {
			t.Errorf("error message missing expected string '%s'\nFull error: %s", expected, errMsg)
		}
	}
}

// TestGetTableInfo_MultipleTables tests caching with different tables
// 異なるテーブルに対するキャッシュの動作を検証
// Requirements: 3.1, 3.2
func TestGetTableInfo_MultipleTables(t *testing.T) {
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

	callCount := 0
	mockClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			callCount++
			tableName := *params.Name
			return &s3tables.GetTableOutput{
				TableARN:          aws.String(fmt.Sprintf("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-namespace/%s", tableName)),
				WarehouseLocation: aws.String(fmt.Sprintf("s3://test-warehouse-bucket/test-namespace/%s", tableName)),
				VersionToken:      aws.String(fmt.Sprintf("version-token-%s", tableName)),
			}, nil
		},
	}
	exporter.s3TablesClient = mockClient

	// 異なるテーブルを取得
	table1, err := exporter.getTableInfo(context.Background(), "test-namespace", "table1")
	if err != nil {
		t.Fatalf("getTableInfo() failed for table1: %v", err)
	}

	table2, err := exporter.getTableInfo(context.Background(), "test-namespace", "table2")
	if err != nil {
		t.Fatalf("getTableInfo() failed for table2: %v", err)
	}

	// 各テーブルに対してGetTableが1回ずつ呼ばれることを確認
	if callCount != 2 {
		t.Errorf("expected GetTable to be called twice (once per table), got %d calls", callCount)
	}

	// 同じテーブルを再度取得 - キャッシュヒット
	table1Again, err := exporter.getTableInfo(context.Background(), "test-namespace", "table1")
	if err != nil {
		t.Fatalf("getTableInfo() failed for table1 (second call): %v", err)
	}

	// GetTableが再度呼ばれないことを確認
	if callCount != 2 {
		t.Errorf("expected GetTable to still be called only twice (cached), got %d calls", callCount)
	}

	// キャッシュされた情報が一致することを確認
	if table1.TableARN != table1Again.TableARN {
		t.Errorf("cached table1 TableARN mismatch: %s != %s", table1.TableARN, table1Again.TableARN)
	}

	// 異なるテーブルの情報が異なることを確認
	if table1.TableARN == table2.TableARN {
		t.Error("table1 and table2 should have different TableARNs")
	}
}

// TestGetTableInfo_ContextCancellation tests context cancellation handling
// コンテキストキャンセルのハンドリングをテスト
// Requirements: 3.1
func TestGetTableInfo_ContextCancellation(t *testing.T) {
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

	mockClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return nil, context.Canceled
		},
	}
	exporter.s3TablesClient = mockClient

	// キャンセルされたコンテキストで呼び出し
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = exporter.getTableInfo(ctx, "test-namespace", "test-table")
	if err == nil {
		t.Fatal("expected error when context is cancelled")
	}

	// エラーメッセージにコンテキストキャンセルが含まれることを確認
	if !strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "cancelled") {
		t.Errorf("expected error to mention context cancellation, got: %v", err)
	}
}
