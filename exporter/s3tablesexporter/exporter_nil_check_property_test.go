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
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
)

// Feature: fix-nil-pointer-metadata-location, Property 1: Nil チェックの完全性
// すべてのGetTableMetadataLocation APIレスポンスに対して、
// MetadataLocationとVersionTokenフィールドがnilでないことを確認してから
// デリファレンスする場合、nil pointer dereferenceエラーは発生しない
//
// TestNilCheckCompleteness tests that nil pointer dereference errors do not occur
// when MetadataLocation and VersionToken fields are properly checked
// MetadataLocationとVersionTokenフィールドが適切にチェックされた場合、
// nil pointer dereferenceエラーが発生しないことをテスト
func TestNilCheckCompleteness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// テストケース: 様々なnil/非nilの組み合わせ
	testCases := []struct {
		name             string
		metadataLocation *string
		versionToken     *string
		expectError      bool
		description      string
	}{
		{
			name:             "both nil",
			metadataLocation: nil,
			versionToken:     nil,
			expectError:      false,
			description:      "MetadataLocationとVersionTokenの両方がnilの場合、初期メタデータを生成",
		},
		{
			name:             "metadata nil, token present",
			metadataLocation: nil,
			versionToken:     aws.String("test-token"),
			expectError:      false,
			description:      "MetadataLocationがnilでVersionTokenが存在する場合、初期メタデータを生成",
		},
		{
			name:             "metadata present, token nil",
			metadataLocation: aws.String("s3://test-bucket/metadata/v1.metadata.json"),
			versionToken:     nil,
			expectError:      false,
			description:      "MetadataLocationが存在しVersionTokenがnilの場合、デフォルト値を使用（S3クライアントが必要）",
		},
		{
			name:             "both present",
			metadataLocation: aws.String("s3://test-bucket/metadata/v1.metadata.json"),
			versionToken:     aws.String("test-token"),
			expectError:      false,
			description:      "MetadataLocationとVersionTokenの両方が存在する場合、通常の処理（S3クライアントが必要）",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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
				getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
					return &s3tables.GetTableMetadataLocationOutput{
						MetadataLocation: tc.metadataLocation,
						VersionToken:     tc.versionToken,
					}, nil
				},
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
				s3Client:       nil,
				tableCache:     make(map[string]*TableInfo),
			}

			// MetadataLocationが存在する場合はS3クライアントのモックを追加
			if tc.metadataLocation != nil {
				// テスト用のメタデータを作成
				testMetadata := &IcebergMetadata{
					FormatVersion:      2,
					TableUUID:          "test-uuid",
					Location:           "s3://test-warehouse-bucket/test-namespace/metrics",
					LastSequenceNumber: 0,
					LastUpdatedMS:      1234567890,
					LastColumnID:       10,
					Schemas: []IcebergSchema{
						{
							SchemaID: 0,
							Fields: []IcebergSchemaField{
								{
									ID:       1,
									Name:     "timestamp",
									Required: true,
									Type:     "timestamptz",
								},
							},
						},
					},
					CurrentSchemaID:   0,
					PartitionSpecs:    []IcebergPartitionSpec{},
					DefaultSpecID:     0,
					LastPartitionID:   0,
					Properties:        map[string]string{},
					CurrentSnapshotID: -1,
					Snapshots:         []IcebergSnapshot{},
					SnapshotLog:       []IcebergSnapshotLog{},
					MetadataLog:       []IcebergMetadataLog{},
				}

				metadataJSON, err := json.Marshal(testMetadata)
				if err != nil {
					t.Fatalf("Failed to marshal test metadata: %v", err)
				}

				mockS3Client := &mockS3Client{
					getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						return &s3.GetObjectOutput{
							Body: io.NopCloser(bytes.NewReader(metadataJSON)),
						}, nil
					},
				}
				exporter.s3Client = mockS3Client
			}

			// TableInfoを作成
			tableInfo := &TableInfo{
				TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
				WarehouseLocation: "s3://test-warehouse-bucket/test-namespace/metrics",
				VersionToken:      "",
			}

			// getTableMetadataを呼び出す
			// この呼び出しでnil pointer dereferenceエラーが発生しないことを検証
			metadata, err := exporter.getTableMetadata(context.Background(), "test-namespace", "metrics", tableInfo)

			// エラーの検証
			if tc.expectError && err == nil {
				t.Errorf("Expected error, got nil")
			}
			if !tc.expectError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}

			// nil pointer dereferenceエラーが発生しなかったことを確認
			// （panicが発生した場合、テストは失敗する）
			if !tc.expectError && metadata == nil {
				t.Errorf("Expected metadata to be returned, got nil")
			}

			t.Logf("Test case '%s' passed: %s", tc.name, tc.description)
		})
	}
}

// TestNilCheckCompleteness_Iterations tests nil check completeness with multiple iterations
// 複数回の反復でnilチェックの完全性をテスト
func TestNilCheckCompleteness_Iterations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// 100回の反復でテスト
	iterations := 100

	for i := 0; i < iterations; i++ {
		// ランダムにnil/非nilを選択
		var metadataLocation *string
		var versionToken *string

		// i % 4 で4つのパターンを循環
		switch i % 4 {
		case 0:
			// 両方nil
			metadataLocation = nil
			versionToken = nil
		case 1:
			// MetadataLocationのみnil
			metadataLocation = nil
			versionToken = aws.String("test-token")
		case 2:
			// VersionTokenのみnil
			metadataLocation = aws.String("s3://test-bucket/metadata/v1.metadata.json")
			versionToken = nil
		case 3:
			// 両方存在
			metadataLocation = aws.String("s3://test-bucket/metadata/v1.metadata.json")
			versionToken = aws.String("test-token")
		}

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
			getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
				return &s3tables.GetTableMetadataLocationOutput{
					MetadataLocation: metadataLocation,
					VersionToken:     versionToken,
				}, nil
			},
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
			s3Client:       nil,
			tableCache:     make(map[string]*TableInfo),
		}

		// MetadataLocationが存在する場合はS3クライアントのモックを追加
		if metadataLocation != nil {
			// テスト用のメタデータを作成
			testMetadata := &IcebergMetadata{
				FormatVersion:      2,
				TableUUID:          "test-uuid",
				Location:           "s3://test-warehouse-bucket/test-namespace/metrics",
				LastSequenceNumber: 0,
				LastUpdatedMS:      1234567890,
				LastColumnID:       10,
				Schemas: []IcebergSchema{
					{
						SchemaID: 0,
						Fields: []IcebergSchemaField{
							{
								ID:       1,
								Name:     "timestamp",
								Required: true,
								Type:     "timestamptz",
							},
						},
					},
				},
				CurrentSchemaID:   0,
				PartitionSpecs:    []IcebergPartitionSpec{},
				DefaultSpecID:     0,
				LastPartitionID:   0,
				Properties:        map[string]string{},
				CurrentSnapshotID: -1,
				Snapshots:         []IcebergSnapshot{},
				SnapshotLog:       []IcebergSnapshotLog{},
				MetadataLog:       []IcebergMetadataLog{},
			}

			metadataJSON, err := json.Marshal(testMetadata)
			if err != nil {
				t.Fatalf("Failed to marshal test metadata: %v", err)
			}

			mockS3Client := &mockS3Client{
				getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
					return &s3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(metadataJSON)),
					}, nil
				},
			}
			exporter.s3Client = mockS3Client
		}

		// TableInfoを作成
		tableInfo := &TableInfo{
			TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
			WarehouseLocation: "s3://test-warehouse-bucket/test-namespace/metrics",
			VersionToken:      "",
		}

		// getTableMetadataを呼び出す
		// この呼び出しでnil pointer dereferenceエラーが発生しないことを検証
		_, err := exporter.getTableMetadata(context.Background(), "test-namespace", "metrics", tableInfo)

		// エラーが発生しないことを確認
		if err != nil {
			t.Errorf("Iteration %d: Expected no error, got: %v", i, err)
		}

		// nil pointer dereferenceエラーが発生しなかったことを確認
		// （panicが発生した場合、テストは失敗する）
	}

	t.Logf("Completed %d iterations without nil pointer dereference errors", iterations)
}
