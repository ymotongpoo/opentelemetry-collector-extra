// Copyright 2025 Yoshi Yamaguchi <ymotongpoo@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file excepte with the License.
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
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
)

// TestGenerateInitialMetadata_Success tests successful initial metadata generation
// 初期メタデータの生成が成功する場合のテスト
func TestGenerateInitialMetadata_Success(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
	}{
		{
			name:      "metrics table",
			tableName: "metrics",
		},
		{
			name:      "traces table",
			tableName: "traces",
		},
		{
			name:      "logs table",
			tableName: "logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// テスト用の設定
			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Metrics: "metrics",
					Traces:  "traces",
					Logs:    "logs",
				},
			}

			// モックS3 Tablesクライアントを設定
			mockS3TablesClient := &mockS3TablesClient{
				getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
					return &s3tables.GetTableOutput{
						TableARN:  aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"),
						Name:      aws.String(tt.tableName),
						Namespace: []string{"test-namespace"},
						Format:    types.OpenTableFormatIceberg,
					}, nil
				},
			}

			// エクスポーターを作成
			exporter := &s3TablesExporter{
				config:         cfg,
				logger:         newSlogLogger(),
				s3TablesClient: mockS3TablesClient,
				tableCache:     make(map[string]*TableInfo),
			}

			// TableInfoを作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-warehouse-bucket/test-namespace/" + tt.tableName,
				VersionToken:      "",
			}

			// generateInitialMetadataを呼び出す
			metadata, err := exporter.generateInitialMetadata(context.Background(), "test-namespace", tt.tableName, tableInfo)

			// エラーがないことを確認
			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}

			// メタデータが生成されたことを確認
			if metadata == nil {
				t.Fatal("Expected metadata to be generated, got nil")
			}

			// 必須フィールドの検証
			if metadata.FormatVersion != 2 {
				t.Errorf("Expected FormatVersion 2, got %d", metadata.FormatVersion)
			}

			if metadata.TableUUID == "" {
				t.Error("Expected TableUUID to be generated, got empty string")
			}

			if metadata.Location != tableInfo.WarehouseLocation {
				t.Errorf("Expected Location %s, got %s", tableInfo.WarehouseLocation, metadata.Location)
			}

			if metadata.LastSequenceNumber != 0 {
				t.Errorf("Expected LastSequenceNumber 0, got %d", metadata.LastSequenceNumber)
			}

			if metadata.CurrentSnapshotID != -1 {
				t.Errorf("Expected CurrentSnapshotID -1, got %d", metadata.CurrentSnapshotID)
			}

			if len(metadata.Snapshots) != 0 {
				t.Errorf("Expected empty Snapshots, got %d snapshots", len(metadata.Snapshots))
			}

			if len(metadata.Schemas) != 1 {
				t.Errorf("Expected 1 schema, got %d schemas", len(metadata.Schemas))
			}

			// スキーマが空でないことを確認
			if len(metadata.Schemas[0].Fields) == 0 {
				t.Error("Expected schema to have fields, got empty schema")
			}

			// LastColumnIDが設定されていることを確認
			if metadata.LastColumnID <= 0 {
				t.Errorf("Expected LastColumnID > 0, got %d", metadata.LastColumnID)
			}

			// VersionTokenがデフォルト値（空文字列）に設定されていることを確認
			if tableInfo.VersionToken != "" {
				t.Errorf("Expected VersionToken to be empty string, got %s", tableInfo.VersionToken)
			}
		})
	}
}

// TestGenerateInitialMetadata_GetTableError tests error handling when GetTable API fails
// GetTable APIが失敗した場合のエラーハンドリングのテスト
func TestGenerateInitialMetadata_GetTableError(t *testing.T) {
	// テスト用の設定
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Metrics: "metrics",
			Traces:  "traces",
			Logs:    "logs",
		},
	}

	// モックS3 Tablesクライアントを設定（エラーを返す）
	mockS3TablesClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return nil, errors.New("table not found")
		},
	}

	// エクスポーターを作成
	exporter := &s3TablesExporter{
		config:         cfg,
		logger:         newSlogLogger(),
		s3TablesClient: mockS3TablesClient,
		tableCache:     make(map[string]*TableInfo),
	}

	// TableInfoを作成
	tableInfo := &TableInfo{
		TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
		WarehouseLocation: "s3://test-warehouse-bucket/test-namespace/metrics",
		VersionToken:      "",
	}

	// generateInitialMetadataを呼び出す
	metadata, err := exporter.generateInitialMetadata(context.Background(), "test-namespace", "metrics", tableInfo)

	// エラーが返されることを確認
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// メタデータがnilであることを確認
	if metadata != nil {
		t.Errorf("Expected metadata to be nil, got %v", metadata)
	}

	// エラーメッセージにテーブル名とnamespaceが含まれることを確認
	expectedSubstrings := []string{"test-namespace", "metrics", "initial metadata"}
	for _, substr := range expectedSubstrings {
		if !contains(err.Error(), substr) {
			t.Errorf("Expected error message to contain '%s', got: %s", substr, err.Error())
		}
	}
}

// TestGenerateInitialMetadata_ContextCancellation tests context cancellation handling
// コンテキストキャンセルのハンドリングのテスト
func TestGenerateInitialMetadata_ContextCancellation(t *testing.T) {
	// テスト用の設定
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Metrics: "metrics",
			Traces:  "traces",
			Logs:    "logs",
		},
	}

	// モックS3 Tablesクライアントを設定
	mockS3TablesClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return &s3tables.GetTableOutput{
				TableARN:  aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"),
				Name:      aws.String("metrics"),
				Namespace: []string{"test-namespace"},
				Format:    types.OpenTableFormatIceberg,
			}, nil
		},
	}

	// エクスポーターを作成
	exporter := &s3TablesExporter{
		config:         cfg,
		logger:         newSlogLogger(),
		s3TablesClient: mockS3TablesClient,
		tableCache:     make(map[string]*TableInfo),
	}

	// TableInfoを作成
	tableInfo := &TableInfo{
		TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
		WarehouseLocation: "s3://test-warehouse-bucket/test-namespace/metrics",
		VersionToken:      "",
	}

	// キャンセル可能なコンテキストを作成
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // すぐにキャンセル

	// generateInitialMetadataを呼び出す
	metadata, err := exporter.generateInitialMetadata(ctx, "test-namespace", "metrics", tableInfo)

	// エラーが返されることを確認
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// メタデータがnilであることを確認
	if metadata != nil {
		t.Errorf("Expected metadata to be nil, got %v", metadata)
	}

	// エラーメッセージに"cancelled"が含まれることを確認
	if !contains(err.Error(), "cancelled") {
		t.Errorf("Expected error message to contain 'cancelled', got: %s", err.Error())
	}
}

// TestGenerateInitialMetadata_UnknownTableType tests handling of unknown table types
// 未知のテーブルタイプのハンドリングのテスト
func TestGenerateInitialMetadata_UnknownTableType(t *testing.T) {
	// テスト用の設定
	cfg := &Config{
		TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
		Region:         "us-east-1",
		Namespace:      "test-namespace",
		Tables: TableNamesConfig{
			Metrics: "metrics",
			Traces:  "traces",
			Logs:    "logs",
		},
	}

	// モックS3 Tablesクライアントを設定
	mockS3TablesClient := &mockS3TablesClient{
		getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
			return &s3tables.GetTableOutput{
				TableARN:  aws.String("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id"),
				Name:      aws.String("unknown-table"),
				Namespace: []string{"test-namespace"},
				Format:    types.OpenTableFormatIceberg,
			}, nil
		},
	}

	// エクスポーターを作成
	exporter := &s3TablesExporter{
		config:         cfg,
		logger:         newSlogLogger(),
		s3TablesClient: mockS3TablesClient,
		tableCache:     make(map[string]*TableInfo),
	}

	// TableInfoを作成
	tableInfo := &TableInfo{
		TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
		WarehouseLocation: "s3://test-warehouse-bucket/test-namespace/unknown-table",
		VersionToken:      "",
	}

	// generateInitialMetadataを呼び出す
	metadata, err := exporter.generateInitialMetadata(context.Background(), "test-namespace", "unknown-table", tableInfo)

	// エラーがないことを確認（空のスキーマで処理される）
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// メタデータが生成されたことを確認
	if metadata == nil {
		t.Fatal("Expected metadata to be generated, got nil")
	}

	// スキーマが空であることを確認
	if len(metadata.Schemas) != 1 {
		t.Errorf("Expected 1 schema, got %d schemas", len(metadata.Schemas))
	}

	if len(metadata.Schemas[0].Fields) != 0 {
		t.Errorf("Expected empty schema for unknown table type, got %d fields", len(metadata.Schemas[0].Fields))
	}

	// LastColumnIDが0であることを確認
	if metadata.LastColumnID != 0 {
		t.Errorf("Expected LastColumnID 0 for empty schema, got %d", metadata.LastColumnID)
	}
}

// contains checks if a string contains a substring
// 文字列に部分文字列が含まれているかチェック
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
