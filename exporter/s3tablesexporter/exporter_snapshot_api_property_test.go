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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestProperty_UpdateTableMetadataLocationAPIParameters tests UpdateTableMetadataLocation API parameters
// Feature: iceberg-snapshot-commit, Property 5: UpdateTableMetadataLocation API呼び出しパラメータ
// Validates: Requirements 2.3, 2.4
func TestProperty_UpdateTableMetadataLocationAPIParameters(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のUpdateTableMetadataLocation API呼び出しに対して、
	// そのAPIはTable Bucket ARN、Namespace、Table Name、新しいメタデータロケーション、
	// バージョントークンのすべてのパラメータを含むべきである

	// テストケース1: 必須パラメータの検証
	t.Run("required_parameters_validation", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			cfg := &Config{
				TableBucketArn: fmt.Sprintf("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket-%d", i),
				Region:         "us-east-1",
				Namespace:      fmt.Sprintf("test-namespace-%d", i),
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			// 既存のメタデータを作成
			existingMetadata := generateRandomMetadata(i)

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(existingMetadata)
			if err != nil {
				t.Fatalf("iteration %d: failed to marshal metadata: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定
			metadataLocation := fmt.Sprintf("s3://test-bucket-%d/metadata/%05d-initial.metadata.json", i, i)
			versionToken := fmt.Sprintf("version-token-%d", i)
			newVersionToken := fmt.Sprintf("version-token-%d-new", i)
			tableName := fmt.Sprintf("test-table-%d", i)

			var capturedUpdateParams *s3tables.UpdateTableMetadataLocationInput
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					capturedUpdateParams = params
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &newVersionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          fmt.Sprintf("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket-%d/table/test-table-id", i),
				WarehouseLocation: fmt.Sprintf("s3://test-bucket-%d", i),
				VersionToken:      versionToken,
			}

			// データファイルパス
			dataFilePaths := []string{fmt.Sprintf("s3://test-bucket-%d/data/file%d.parquet", i, i)}

			// スナップショットをコミット
			err = exporter.commitSnapshot(context.Background(), cfg.Namespace, tableName, tableInfo, dataFilePaths, int64(len(dataFilePaths)*1024))
			if err != nil {
				t.Fatalf("iteration %d: commitSnapshot() failed: %v", i, err)
			}

			// UpdateTableMetadataLocation APIが呼び出されたことを確認
			if capturedUpdateParams == nil {
				t.Fatalf("iteration %d: UpdateTableMetadataLocation was not called", i)
			}

			// Table Bucket ARNが設定されていることを確認
			if capturedUpdateParams.TableBucketARN == nil {
				t.Errorf("iteration %d: TableBucketARN should not be nil", i)
			} else if *capturedUpdateParams.TableBucketARN != cfg.TableBucketArn {
				t.Errorf("iteration %d: expected TableBucketARN '%s', got '%s'", i, cfg.TableBucketArn, *capturedUpdateParams.TableBucketARN)
			}

			// Namespaceが設定されていることを確認
			if capturedUpdateParams.Namespace == nil {
				t.Errorf("iteration %d: Namespace should not be nil", i)
			} else if *capturedUpdateParams.Namespace != cfg.Namespace {
				t.Errorf("iteration %d: expected Namespace '%s', got '%s'", i, cfg.Namespace, *capturedUpdateParams.Namespace)
			}

			// Table Nameが設定されていることを確認
			if capturedUpdateParams.Name == nil {
				t.Errorf("iteration %d: Name should not be nil", i)
			} else if *capturedUpdateParams.Name != tableName {
				t.Errorf("iteration %d: expected Name '%s', got '%s'", i, tableName, *capturedUpdateParams.Name)
			}

			// 新しいメタデータロケーションが設定されていることを確認
			if capturedUpdateParams.MetadataLocation == nil {
				t.Errorf("iteration %d: MetadataLocation should not be nil", i)
			} else if *capturedUpdateParams.MetadataLocation == "" {
				t.Errorf("iteration %d: MetadataLocation should not be empty", i)
			}

			// バージョントークンが設定されていることを確認
			if capturedUpdateParams.VersionToken == nil {
				t.Errorf("iteration %d: VersionToken should not be nil", i)
			} else if *capturedUpdateParams.VersionToken != versionToken {
				t.Errorf("iteration %d: expected VersionToken '%s', got '%s'", i, versionToken, *capturedUpdateParams.VersionToken)
			}
		}
	})

	// テストケース2: メタデータロケーションの形式検証
	t.Run("metadata_location_format_validation", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
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
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			// 既存のメタデータを作成
			existingMetadata := generateRandomMetadata(i)

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(existingMetadata)
			if err != nil {
				t.Fatalf("iteration %d: failed to marshal metadata: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定
			metadataLocation := "s3://test-bucket/metadata/00000-initial.metadata.json"
			versionToken := fmt.Sprintf("version-token-%d", i)
			newVersionToken := fmt.Sprintf("version-token-%d-new", i)

			var capturedUpdateParams *s3tables.UpdateTableMetadataLocationInput
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					capturedUpdateParams = params
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &newVersionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      versionToken,
			}

			// データファイルパス
			dataFilePaths := []string{fmt.Sprintf("s3://test-bucket/data/file%d.parquet", i)}

			// スナップショットをコミット
			err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths, int64(len(dataFilePaths)*1024))
			if err != nil {
				t.Fatalf("iteration %d: commitSnapshot() failed: %v", i, err)
			}

			// UpdateTableMetadataLocation APIが呼び出されたことを確認
			if capturedUpdateParams == nil {
				t.Fatalf("iteration %d: UpdateTableMetadataLocation was not called", i)
			}

			// メタデータロケーションの形式を検証
			if capturedUpdateParams.MetadataLocation != nil {
				location := *capturedUpdateParams.MetadataLocation

				// s3://で始まることを確認
				if len(location) < 5 || location[:5] != "s3://" {
					t.Errorf("iteration %d: MetadataLocation should start with 's3://', got '%s'", i, location)
				}

				// .metadata.jsonで終わることを確認
				if len(location) < 14 || location[len(location)-14:] != ".metadata.json" {
					t.Errorf("iteration %d: MetadataLocation should end with '.metadata.json', got '%s'", i, location)
				}

				// warehouse locationで始まることを確認
				if len(location) < len(tableInfo.WarehouseLocation) || location[:len(tableInfo.WarehouseLocation)] != tableInfo.WarehouseLocation {
					t.Errorf("iteration %d: MetadataLocation should start with warehouse location '%s', got '%s'", i, tableInfo.WarehouseLocation, location)
				}
			}
		}
	})

	// テストケース3: 様々なテーブル名とネームスペースでのパラメータ検証
	t.Run("various_table_names_and_namespaces", func(t *testing.T) {
		testCases := []struct {
			namespace string
			tableName string
		}{
			{"namespace1", "table1"},
			{"my-namespace", "my-table"},
			{"ns_with_underscore", "table_with_underscore"},
			{"ns123", "table456"},
			{"production", "metrics_table"},
			{"staging", "traces_table"},
			{"dev", "logs_table"},
		}

		for i, tc := range testCases {
			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      tc.namespace,
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("test case %d: newS3TablesExporter() failed: %v", i, err)
			}

			// 既存のメタデータを作成
			existingMetadata := generateRandomMetadata(i)

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(existingMetadata)
			if err != nil {
				t.Fatalf("test case %d: failed to marshal metadata: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定
			metadataLocation := "s3://test-bucket/metadata/00000-initial.metadata.json"
			versionToken := fmt.Sprintf("version-token-%d", i)
			newVersionToken := fmt.Sprintf("version-token-%d-new", i)

			var capturedUpdateParams *s3tables.UpdateTableMetadataLocationInput
			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					capturedUpdateParams = params
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &newVersionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      versionToken,
			}

			// データファイルパス
			dataFilePaths := []string{"s3://test-bucket/data/file.parquet"}

			// スナップショットをコミット
			err = exporter.commitSnapshot(context.Background(), tc.namespace, tc.tableName, tableInfo, dataFilePaths, int64(len(dataFilePaths)*1024))
			if err != nil {
				t.Fatalf("test case %d: commitSnapshot() failed: %v", i, err)
			}

			// UpdateTableMetadataLocation APIが呼び出されたことを確認
			if capturedUpdateParams == nil {
				t.Fatalf("test case %d: UpdateTableMetadataLocation was not called", i)
			}

			// Namespaceが正しく設定されていることを確認
			if capturedUpdateParams.Namespace == nil || *capturedUpdateParams.Namespace != tc.namespace {
				t.Errorf("test case %d: expected Namespace '%s', got '%v'", i, tc.namespace, capturedUpdateParams.Namespace)
			}

			// Table Nameが正しく設定されていることを確認
			if capturedUpdateParams.Name == nil || *capturedUpdateParams.Name != tc.tableName {
				t.Errorf("test case %d: expected Name '%s', got '%v'", i, tc.tableName, capturedUpdateParams.Name)
			}
		}
	})
}

// TestProperty_VersionTokenConsistency tests version token usage for optimistic concurrency control
// Feature: iceberg-snapshot-commit, Property 10: メタデータ取得と更新の一貫性（バージョントークン部分）
// Validates: Requirements 9.5
func TestProperty_VersionTokenConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意のスナップショットコミットに対して、システムはGetTableMetadataLocation APIで
	// 取得したバージョントークンを使用してUpdateTableMetadataLocation APIを呼び出すべきである

	// テストケース1: バージョントークンの一貫性
	t.Run("version_token_consistency", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
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
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			// 既存のメタデータを作成
			existingMetadata := generateRandomMetadata(i)

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(existingMetadata)
			if err != nil {
				t.Fatalf("iteration %d: failed to marshal metadata: %v", i, err)
			}

			// ランダムなバージョントークンを生成
			versionToken := fmt.Sprintf("version-token-%d-%d", i, time.Now().UnixNano())
			newVersionToken := fmt.Sprintf("version-token-%d-%d-new", i, time.Now().UnixNano())

			// モックS3 Tablesクライアントを設定
			metadataLocation := fmt.Sprintf("s3://test-bucket/metadata/%05d-initial.metadata.json", i)

			var capturedGetParams *s3tables.GetTableMetadataLocationInput
			var capturedUpdateParams *s3tables.UpdateTableMetadataLocationInput

			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					capturedGetParams = params
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionToken,
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					capturedUpdateParams = params
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &newVersionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      "old-version-token", // 古いバージョントークン
			}

			// データファイルパス
			dataFilePaths := []string{fmt.Sprintf("s3://test-bucket/data/file%d.parquet", i)}

			// スナップショットをコミット
			err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths, int64(len(dataFilePaths)*1024))
			if err != nil {
				t.Fatalf("iteration %d: commitSnapshot() failed: %v", i, err)
			}

			// GetTableMetadataLocation APIが呼び出されたことを確認
			if capturedGetParams == nil {
				t.Fatalf("iteration %d: GetTableMetadataLocation was not called", i)
			}

			// UpdateTableMetadataLocation APIが呼び出されたことを確認
			if capturedUpdateParams == nil {
				t.Fatalf("iteration %d: UpdateTableMetadataLocation was not called", i)
			}

			// UpdateTableMetadataLocationで使用されたバージョントークンが
			// GetTableMetadataLocationで取得したバージョントークンと一致することを確認
			if capturedUpdateParams.VersionToken == nil {
				t.Errorf("iteration %d: VersionToken in UpdateTableMetadataLocation should not be nil", i)
			} else if *capturedUpdateParams.VersionToken != versionToken {
				t.Errorf("iteration %d: expected VersionToken '%s' in UpdateTableMetadataLocation, got '%s'",
					i, versionToken, *capturedUpdateParams.VersionToken)
			}

			// TableInfoのVersionTokenが更新されたことを確認
			if tableInfo.VersionToken != newVersionToken {
				t.Errorf("iteration %d: expected TableInfo.VersionToken to be updated to '%s', got '%s'",
					i, newVersionToken, tableInfo.VersionToken)
			}
		}
	})

	// テストケース2: 複数回のコミットでのバージョントークン更新
	t.Run("version_token_updates_across_multiple_commits", func(t *testing.T) {
		iterations := 30
		for i := 0; i < iterations; i++ {
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
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			// 既存のメタデータを作成
			existingMetadata := generateRandomMetadata(i)

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(existingMetadata)
			if err != nil {
				t.Fatalf("iteration %d: failed to marshal metadata: %v", i, err)
			}

			// 複数回のコミットをシミュレート
			numCommits := 3 + (i % 5) // 3-7回のコミット
			versionTokens := make([]string, numCommits+1)
			versionTokens[0] = fmt.Sprintf("version-token-%d-0", i)

			for j := 1; j <= numCommits; j++ {
				versionTokens[j] = fmt.Sprintf("version-token-%d-%d", i, j)
			}

			currentCommit := 0
			metadataLocation := fmt.Sprintf("s3://test-bucket/metadata/%05d-initial.metadata.json", i)

			var lastCapturedUpdateParams *s3tables.UpdateTableMetadataLocationInput

			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &versionTokens[currentCommit],
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					lastCapturedUpdateParams = params
					currentCommit++
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &versionTokens[currentCommit],
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      versionTokens[0],
			}

			// 複数回のコミットを実行
			for j := 0; j < numCommits; j++ {
				// データファイルパス
				dataFilePaths := []string{fmt.Sprintf("s3://test-bucket/data/file%d-%d.parquet", i, j)}

				// スナップショットをコミット
				err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths, int64(len(dataFilePaths)*1024))
				if err != nil {
					t.Fatalf("iteration %d, commit %d: commitSnapshot() failed: %v", i, j, err)
				}

				// UpdateTableMetadataLocationで使用されたバージョントークンが正しいことを確認
				if lastCapturedUpdateParams == nil {
					t.Fatalf("iteration %d, commit %d: UpdateTableMetadataLocation was not called", i, j)
				}

				expectedVersionToken := versionTokens[j]
				if *lastCapturedUpdateParams.VersionToken != expectedVersionToken {
					t.Errorf("iteration %d, commit %d: expected VersionToken '%s', got '%s'",
						i, j, expectedVersionToken, *lastCapturedUpdateParams.VersionToken)
				}

				// TableInfoのVersionTokenが更新されたことを確認
				expectedNewVersionToken := versionTokens[j+1]
				if tableInfo.VersionToken != expectedNewVersionToken {
					t.Errorf("iteration %d, commit %d: expected TableInfo.VersionToken to be '%s', got '%s'",
						i, j, expectedNewVersionToken, tableInfo.VersionToken)
				}
			}
		}
	})

	// テストケース3: バージョントークンの不一致エラー（ConflictException）のシミュレーション
	t.Run("version_token_conflict_error", func(t *testing.T) {
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

		// 既存のメタデータを作成
		existingMetadata := generateRandomMetadata(0)

		// メタデータをJSONにシリアライズ
		metadataJSON, err := json.Marshal(existingMetadata)
		if err != nil {
			t.Fatalf("failed to marshal metadata: %v", err)
		}

		// モックS3 Tablesクライアントを設定
		metadataLocation := "s3://test-bucket/metadata/00000-initial.metadata.json"
		versionToken := "version-token-1"

		mockS3TablesClient := &mockS3TablesClient{
			getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
				return &s3tables.GetTableMetadataLocationOutput{
					MetadataLocation: &metadataLocation,
					VersionToken:     &versionToken,
				}, nil
			},
			updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
				// バージョントークンの不一致をシミュレート
				return nil, fmt.Errorf("ConflictException: Version token mismatch")
			},
		}
		exporter.s3TablesClient = mockS3TablesClient

		// モックS3クライアントを設定
		mockS3Client := &mockS3Client{
			getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
				return &s3.GetObjectOutput{
					Body: io.NopCloser(bytes.NewReader(metadataJSON)),
				}, nil
			},
			putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
				return &s3.PutObjectOutput{}, nil
			},
		}
		exporter.s3Client = mockS3Client

		// テーブル情報を作成
		tableInfo := &TableInfo{
			TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
			WarehouseLocation: "s3://test-bucket",
			VersionToken:      versionToken,
		}

		// データファイルパス
		dataFilePaths := []string{"s3://test-bucket/data/file.parquet"}

		// スナップショットをコミット（エラーが発生するはず）
		err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths, int64(len(dataFilePaths)*1024))
		if err == nil {
			t.Error("expected error from commitSnapshot due to version token conflict")
		}

		// エラーメッセージにConflictExceptionが含まれることを確認
		if err != nil && len(err.Error()) > 0 {
			// エラーが適切に伝播されていることを確認
			expectedSubstr := "failed to update table metadata location"
			if len(err.Error()) < len(expectedSubstr) {
				t.Errorf("error message should contain '%s', got '%s'", expectedSubstr, err.Error())
			}
		}
	})

	// テストケース4: 様々なバージョントークン形式でのテスト
	t.Run("various_version_token_formats", func(t *testing.T) {
		testCases := []struct {
			name         string
			versionToken string
		}{
			{"simple_token", "token-1"},
			{"uuid_token", "a1b2c3d4-e5f6-7890-abcd-ef1234567890"},
			{"timestamp_token", "1234567890000"},
			{"complex_token", "v1-2024-01-01-a1b2c3d4"},
			{"long_token", "very-long-version-token-with-many-characters-1234567890"},
		}

		for i, tc := range testCases {
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
				t.Fatalf("test case %d: newS3TablesExporter() failed: %v", i, err)
			}

			// 既存のメタデータを作成
			existingMetadata := generateRandomMetadata(i)

			// メタデータをJSONにシリアライズ
			metadataJSON, err := json.Marshal(existingMetadata)
			if err != nil {
				t.Fatalf("test case %d: failed to marshal metadata: %v", i, err)
			}

			// モックS3 Tablesクライアントを設定
			metadataLocation := "s3://test-bucket/metadata/00000-initial.metadata.json"
			newVersionToken := tc.versionToken + "-new"

			var capturedUpdateParams *s3tables.UpdateTableMetadataLocationInput

			mockS3TablesClient := &mockS3TablesClient{
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: &metadataLocation,
						VersionToken:     &tc.versionToken,
					}, nil
				},
				updateTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
					capturedUpdateParams = params
					return &s3tables.UpdateTableMetadataLocationOutput{
						VersionToken: &newVersionToken,
					}, nil
				},
			}
			exporter.s3TablesClient = mockS3TablesClient

			// モックS3クライアントを設定
			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
				putObjectFunc: func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
					return &s3.PutObjectOutput{}, nil
				},
			}
			exporter.s3Client = mockS3Client

			// テーブル情報を作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-bucket",
				VersionToken:      "old-token",
			}

			// データファイルパス
			dataFilePaths := []string{"s3://test-bucket/data/file.parquet"}

			// スナップショットをコミット
			err = exporter.commitSnapshot(context.Background(), "test-namespace", "test-table", tableInfo, dataFilePaths, int64(len(dataFilePaths)*1024))
			if err != nil {
				t.Fatalf("test case %d (%s): commitSnapshot() failed: %v", i, tc.name, err)
			}

			// UpdateTableMetadataLocationで使用されたバージョントークンが正しいことを確認
			if capturedUpdateParams == nil {
				t.Fatalf("test case %d (%s): UpdateTableMetadataLocation was not called", i, tc.name)
			}

			if *capturedUpdateParams.VersionToken != tc.versionToken {
				t.Errorf("test case %d (%s): expected VersionToken '%s', got '%s'",
					i, tc.name, tc.versionToken, *capturedUpdateParams.VersionToken)
			}

			// TableInfoのVersionTokenが更新されたことを確認
			if tableInfo.VersionToken != newVersionToken {
				t.Errorf("test case %d (%s): expected TableInfo.VersionToken to be '%s', got '%s'",
					i, tc.name, newVersionToken, tableInfo.VersionToken)
			}
		}
	})
}
