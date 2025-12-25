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

// TestGetTableMetadata_MetadataLocationNil tests getTableMetadata when MetadataLocation is nil
// MetadataLocationがnilの場合のgetTableMetadataのテスト
func TestGetTableMetadata_MetadataLocationNil(t *testing.T) {
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
			// MetadataLocationがnilのレスポンスを返す
			return &s3tables.GetTableMetadataLocationOutput{
				MetadataLocation: nil,
				VersionToken:     aws.String("test-version-token"),
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
		s3Client:       nil, // S3クライアントは使用しない
		tableCache:     make(map[string]*TableInfo),
	}

	// TableInfoを作成
	tableInfo := &TableInfo{
		TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
		WarehouseLocation: "s3://test-warehouse-bucket/test-namespace/metrics",
		VersionToken:      "",
	}

	// getTableMetadataを呼び出す
	metadata, err := exporter.getTableMetadata(context.Background(), "test-namespace", "metrics", tableInfo)

	// エラーがないことを確認
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// メタデータが生成されたことを確認
	if metadata == nil {
		t.Fatal("Expected metadata to be generated, got nil")
	}

	// 初期メタデータの検証
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

	// VersionTokenがデフォルト値（空文字列）に設定されていることを確認
	if tableInfo.VersionToken != "" {
		t.Errorf("Expected VersionToken to be empty string, got %s", tableInfo.VersionToken)
	}
}

// TestGetTableMetadata_VersionTokenNil tests getTableMetadata when VersionToken is nil
// VersionTokenがnilの場合のgetTableMetadataのテスト
func TestGetTableMetadata_VersionTokenNil(t *testing.T) {
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
		CurrentSchemaID: 0,
		PartitionSpecs:  []IcebergPartitionSpec{},
		DefaultSpecID:   0,
		LastPartitionID: 0,
		Properties:      map[string]string{},
		CurrentSnapshotID: -1,
		Snapshots:         []IcebergSnapshot{},
		SnapshotLog:       []IcebergSnapshotLog{},
		MetadataLog:       []IcebergMetadataLog{},
	}

	metadataJSON, err := json.Marshal(testMetadata)
	if err != nil {
		t.Fatalf("Failed to marshal test metadata: %v", err)
	}

	// モックS3 Tablesクライアントを設定
	mockS3TablesClient := &mockS3TablesClient{
		getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
			// VersionTokenがnilのレスポンスを返す
			return &s3tables.GetTableMetadataLocationOutput{
				MetadataLocation: aws.String("s3://test-warehouse-bucket/metadata/v1.metadata.json"),
				VersionToken:     nil,
			}, nil
		},
	}

	// モックS3クライアントを設定
	mockS3Client := &mockS3Client{
		getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader(metadataJSON)),
			}, nil
		},
	}

	// エクスポーターを作成
	exporter := &s3TablesExporter{
		config:         cfg,
		logger:         newSlogLogger(),
		s3TablesClient: mockS3TablesClient,
		s3Client:       mockS3Client,
		tableCache:     make(map[string]*TableInfo),
	}

	// TableInfoを作成
	tableInfo := &TableInfo{
		TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
		WarehouseLocation: "s3://test-warehouse-bucket/test-namespace/metrics",
		VersionToken:      "old-version-token",
	}

	// getTableMetadataを呼び出す
	metadata, err := exporter.getTableMetadata(context.Background(), "test-namespace", "metrics", tableInfo)

	// エラーがないことを確認
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// メタデータが取得されたことを確認
	if metadata == nil {
		t.Fatal("Expected metadata to be retrieved, got nil")
	}

	// VersionTokenがデフォルト値（空文字列）に設定されていることを確認
	if tableInfo.VersionToken != "" {
		t.Errorf("Expected VersionToken to be empty string, got %s", tableInfo.VersionToken)
	}
}

// TestGetTableMetadata_ExistingMetadata tests getTableMetadata when metadata exists
// 既存のメタデータが存在する場合のgetTableMetadataのテスト
func TestGetTableMetadata_ExistingMetadata(t *testing.T) {
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

	// テスト用のメタデータを作成
	testMetadata := &IcebergMetadata{
		FormatVersion:      2,
		TableUUID:          "test-uuid",
		Location:           "s3://test-warehouse-bucket/test-namespace/metrics",
		LastSequenceNumber: 5,
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
		CurrentSchemaID: 0,
		PartitionSpecs:  []IcebergPartitionSpec{},
		DefaultSpecID:   0,
		LastPartitionID: 0,
		Properties:      map[string]string{},
		CurrentSnapshotID: 123456789,
		Snapshots: []IcebergSnapshot{
			{
				SnapshotID:     123456789,
				TimestampMS:    1234567890,
				SequenceNumber: 5,
				Summary: map[string]string{
					"operation": "append",
				},
				ManifestList: "s3://test-warehouse-bucket/metadata/snap-123456789.avro",
			},
		},
		SnapshotLog: []IcebergSnapshotLog{},
		MetadataLog: []IcebergMetadataLog{},
	}

	metadataJSON, err := json.Marshal(testMetadata)
	if err != nil {
		t.Fatalf("Failed to marshal test metadata: %v", err)
	}

	// モックS3 Tablesクライアントを設定
	mockS3TablesClient := &mockS3TablesClient{
		getTableMetadataLocationFunc: func(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
			return &s3tables.GetTableMetadataLocationOutput{
				MetadataLocation: aws.String("s3://test-warehouse-bucket/metadata/v1.metadata.json"),
				VersionToken:     aws.String("test-version-token"),
			}, nil
		},
	}

	// モックS3クライアントを設定
	mockS3Client := &mockS3Client{
		getObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader(metadataJSON)),
			}, nil
		},
	}

	// エクスポーターを作成
	exporter := &s3TablesExporter{
		config:         cfg,
		logger:         newSlogLogger(),
		s3TablesClient: mockS3TablesClient,
		s3Client:       mockS3Client,
		tableCache:     make(map[string]*TableInfo),
	}

	// TableInfoを作成
	tableInfo := &TableInfo{
		TableARN:          "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-table-id",
		WarehouseLocation: "s3://test-warehouse-bucket/test-namespace/metrics",
		VersionToken:      "",
	}

	// getTableMetadataを呼び出す
	metadata, err := exporter.getTableMetadata(context.Background(), "test-namespace", "metrics", tableInfo)

	// エラーがないことを確認
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// メタデータが取得されたことを確認
	if metadata == nil {
		t.Fatal("Expected metadata to be retrieved, got nil")
	}

	// 既存のメタデータが正しく取得されたことを確認
	if metadata.TableUUID != testMetadata.TableUUID {
		t.Errorf("Expected TableUUID %s, got %s", testMetadata.TableUUID, metadata.TableUUID)
	}

	if metadata.LastSequenceNumber != testMetadata.LastSequenceNumber {
		t.Errorf("Expected LastSequenceNumber %d, got %d", testMetadata.LastSequenceNumber, metadata.LastSequenceNumber)
	}

	if metadata.CurrentSnapshotID != testMetadata.CurrentSnapshotID {
		t.Errorf("Expected CurrentSnapshotID %d, got %d", testMetadata.CurrentSnapshotID, metadata.CurrentSnapshotID)
	}

	if len(metadata.Snapshots) != len(testMetadata.Snapshots) {
		t.Errorf("Expected %d snapshots, got %d snapshots", len(testMetadata.Snapshots), len(metadata.Snapshots))
	}

	// VersionTokenが更新されたことを確認
	if tableInfo.VersionToken != "test-version-token" {
		t.Errorf("Expected VersionToken to be 'test-version-token', got %s", tableInfo.VersionToken)
	}
}
