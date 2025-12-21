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
	"encoding/json"
	"testing"
)

// TestIcebergMetadataJSONMarshalling tests JSON marshalling and unmarshalling of IcebergMetadata
// IcebergMetadataのJSONマーシャリングとアンマーシャリングをテスト
func TestIcebergMetadataJSONMarshalling(t *testing.T) {
	// テストデータを作成
	metadata := IcebergMetadata{
		FormatVersion:      2,
		TableUUID:          "test-uuid-1234",
		Location:           "s3://bucket/table",
		LastSequenceNumber: 1,
		LastUpdatedMS:      1234567890000,
		LastColumnID:       5,
		Schemas: []IcebergSchema{
			{
				SchemaID: 0,
				Fields: []IcebergSchemaField{
					{
						ID:       1,
						Name:     "timestamp",
						Required: true,
						Type:     "long",
					},
					{
						ID:       2,
						Name:     "name",
						Required: true,
						Type:     "string",
					},
				},
			},
		},
		CurrentSchemaID: 0,
		PartitionSpecs: []IcebergPartitionSpec{
			{
				SpecID: 0,
				Fields: []IcebergPartitionField{},
			},
		},
		DefaultSpecID:     0,
		LastPartitionID:   0,
		Properties:        map[string]string{"key": "value"},
		CurrentSnapshotID: 1,
		Snapshots: []IcebergSnapshot{
			{
				SnapshotID:     1,
				TimestampMS:    1234567890000,
				SequenceNumber: 1,
				Summary: map[string]string{
					"operation": "append",
				},
				ManifestList: "s3://bucket/metadata/snap-1.avro",
			},
		},
		SnapshotLog: []IcebergSnapshotLog{
			{
				TimestampMS: 1234567890000,
				SnapshotID:  1,
			},
		},
		MetadataLog: []IcebergMetadataLog{
			{
				TimestampMS:  1234567890000,
				MetadataFile: "s3://bucket/metadata/v1.metadata.json",
			},
		},
	}

	// JSONにマーシャル
	jsonData, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("Failed to marshal metadata: %v", err)
	}

	// JSONからアンマーシャル
	var unmarshalled IcebergMetadata
	err = json.Unmarshal(jsonData, &unmarshalled)
	if err != nil {
		t.Fatalf("Failed to unmarshal metadata: %v", err)
	}

	// 基本フィールドの検証
	if unmarshalled.FormatVersion != metadata.FormatVersion {
		t.Errorf("FormatVersion mismatch: got %d, want %d", unmarshalled.FormatVersion, metadata.FormatVersion)
	}
	if unmarshalled.TableUUID != metadata.TableUUID {
		t.Errorf("TableUUID mismatch: got %s, want %s", unmarshalled.TableUUID, metadata.TableUUID)
	}
	if unmarshalled.Location != metadata.Location {
		t.Errorf("Location mismatch: got %s, want %s", unmarshalled.Location, metadata.Location)
	}
	if unmarshalled.CurrentSnapshotID != metadata.CurrentSnapshotID {
		t.Errorf("CurrentSnapshotID mismatch: got %d, want %d", unmarshalled.CurrentSnapshotID, metadata.CurrentSnapshotID)
	}

	// スキーマの検証
	if len(unmarshalled.Schemas) != len(metadata.Schemas) {
		t.Errorf("Schemas length mismatch: got %d, want %d", len(unmarshalled.Schemas), len(metadata.Schemas))
	}
	if len(unmarshalled.Schemas) > 0 && len(unmarshalled.Schemas[0].Fields) != len(metadata.Schemas[0].Fields) {
		t.Errorf("Schema fields length mismatch: got %d, want %d", len(unmarshalled.Schemas[0].Fields), len(metadata.Schemas[0].Fields))
	}

	// スナップショットの検証
	if len(unmarshalled.Snapshots) != len(metadata.Snapshots) {
		t.Errorf("Snapshots length mismatch: got %d, want %d", len(unmarshalled.Snapshots), len(metadata.Snapshots))
	}
	if len(unmarshalled.Snapshots) > 0 {
		if unmarshalled.Snapshots[0].SnapshotID != metadata.Snapshots[0].SnapshotID {
			t.Errorf("Snapshot ID mismatch: got %d, want %d", unmarshalled.Snapshots[0].SnapshotID, metadata.Snapshots[0].SnapshotID)
		}
	}
}

// TestIcebergManifestJSONMarshalling tests JSON marshalling and unmarshalling of IcebergManifest
// IcebergManifestのJSONマーシャリングとアンマーシャリングをテスト
func TestIcebergManifestJSONMarshalling(t *testing.T) {
	// テストデータを作成
	manifest := IcebergManifest{
		FormatVersion: 2,
		Content:       "data",
		Entries: []IcebergManifestEntry{
			{
				Status:         1, // ADDED
				SnapshotID:     1,
				SequenceNumber: 1,
				DataFile: IcebergDataFile{
					FilePath:      "s3://bucket/data/file1.parquet",
					FileFormat:    "PARQUET",
					RecordCount:   100,
					FileSizeBytes: 1024,
					ColumnSizes: map[int]int64{
						1: 512,
						2: 512,
					},
					ValueCounts: map[int]int64{
						1: 100,
						2: 100,
					},
				},
			},
		},
	}

	// JSONにマーシャル
	jsonData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("Failed to marshal manifest: %v", err)
	}

	// JSONからアンマーシャル
	var unmarshalled IcebergManifest
	err = json.Unmarshal(jsonData, &unmarshalled)
	if err != nil {
		t.Fatalf("Failed to unmarshal manifest: %v", err)
	}

	// 基本フィールドの検証
	if unmarshalled.FormatVersion != manifest.FormatVersion {
		t.Errorf("FormatVersion mismatch: got %d, want %d", unmarshalled.FormatVersion, manifest.FormatVersion)
	}
	if unmarshalled.Content != manifest.Content {
		t.Errorf("Content mismatch: got %s, want %s", unmarshalled.Content, manifest.Content)
	}

	// エントリの検証
	if len(unmarshalled.Entries) != len(manifest.Entries) {
		t.Errorf("Entries length mismatch: got %d, want %d", len(unmarshalled.Entries), len(manifest.Entries))
	}
	if len(unmarshalled.Entries) > 0 {
		entry := unmarshalled.Entries[0]
		originalEntry := manifest.Entries[0]

		if entry.Status != originalEntry.Status {
			t.Errorf("Entry status mismatch: got %d, want %d", entry.Status, originalEntry.Status)
		}
		if entry.SnapshotID != originalEntry.SnapshotID {
			t.Errorf("Entry snapshot ID mismatch: got %d, want %d", entry.SnapshotID, originalEntry.SnapshotID)
		}
		if entry.DataFile.FilePath != originalEntry.DataFile.FilePath {
			t.Errorf("DataFile path mismatch: got %s, want %s", entry.DataFile.FilePath, originalEntry.DataFile.FilePath)
		}
		if entry.DataFile.RecordCount != originalEntry.DataFile.RecordCount {
			t.Errorf("DataFile record count mismatch: got %d, want %d", entry.DataFile.RecordCount, originalEntry.DataFile.RecordCount)
		}
	}
}

// TestIcebergSchemaJSONMarshalling tests JSON marshalling and unmarshalling of IcebergSchema
// IcebergSchemaのJSONマーシャリングとアンマーシャリングをテスト
func TestIcebergSchemaJSONMarshalling(t *testing.T) {
	// テストデータを作成
	schema := IcebergSchema{
		SchemaID: 0,
		Fields: []IcebergSchemaField{
			{
				ID:       1,
				Name:     "id",
				Required: true,
				Type:     "long",
			},
			{
				ID:       2,
				Name:     "data",
				Required: false,
				Type:     "string",
			},
		},
	}

	// JSONにマーシャル
	jsonData, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("Failed to marshal schema: %v", err)
	}

	// JSONからアンマーシャル
	var unmarshalled IcebergSchema
	err = json.Unmarshal(jsonData, &unmarshalled)
	if err != nil {
		t.Fatalf("Failed to unmarshal schema: %v", err)
	}

	// 検証
	if unmarshalled.SchemaID != schema.SchemaID {
		t.Errorf("SchemaID mismatch: got %d, want %d", unmarshalled.SchemaID, schema.SchemaID)
	}
	if len(unmarshalled.Fields) != len(schema.Fields) {
		t.Errorf("Fields length mismatch: got %d, want %d", len(unmarshalled.Fields), len(schema.Fields))
	}

	for i, field := range unmarshalled.Fields {
		originalField := schema.Fields[i]
		if field.ID != originalField.ID {
			t.Errorf("Field %d ID mismatch: got %d, want %d", i, field.ID, originalField.ID)
		}
		if field.Name != originalField.Name {
			t.Errorf("Field %d Name mismatch: got %s, want %s", i, field.Name, originalField.Name)
		}
		if field.Required != originalField.Required {
			t.Errorf("Field %d Required mismatch: got %v, want %v", i, field.Required, originalField.Required)
		}
		if field.Type != originalField.Type {
			t.Errorf("Field %d Type mismatch: got %s, want %s", i, field.Type, originalField.Type)
		}
	}
}

// TestIcebergSnapshotJSONMarshalling tests JSON marshalling and unmarshalling of IcebergSnapshot
// IcebergSnapshotのJSONマーシャリングとアンマーシャリングをテスト
func TestIcebergSnapshotJSONMarshalling(t *testing.T) {
	// テストデータを作成
	snapshot := IcebergSnapshot{
		SnapshotID:     12345,
		TimestampMS:    1234567890000,
		SequenceNumber: 1,
		Summary: map[string]string{
			"operation":     "append",
			"added-files":   "1",
			"added-records": "100",
			"total-files":   "1",
			"total-records": "100",
		},
		ManifestList: "s3://bucket/metadata/snap-12345.avro",
	}

	// JSONにマーシャル
	jsonData, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("Failed to marshal snapshot: %v", err)
	}

	// JSONからアンマーシャル
	var unmarshalled IcebergSnapshot
	err = json.Unmarshal(jsonData, &unmarshalled)
	if err != nil {
		t.Fatalf("Failed to unmarshal snapshot: %v", err)
	}

	// 検証
	if unmarshalled.SnapshotID != snapshot.SnapshotID {
		t.Errorf("SnapshotID mismatch: got %d, want %d", unmarshalled.SnapshotID, snapshot.SnapshotID)
	}
	if unmarshalled.TimestampMS != snapshot.TimestampMS {
		t.Errorf("TimestampMS mismatch: got %d, want %d", unmarshalled.TimestampMS, snapshot.TimestampMS)
	}
	if unmarshalled.SequenceNumber != snapshot.SequenceNumber {
		t.Errorf("SequenceNumber mismatch: got %d, want %d", unmarshalled.SequenceNumber, snapshot.SequenceNumber)
	}
	if unmarshalled.ManifestList != snapshot.ManifestList {
		t.Errorf("ManifestList mismatch: got %s, want %s", unmarshalled.ManifestList, snapshot.ManifestList)
	}

	// Summaryの検証
	if len(unmarshalled.Summary) != len(snapshot.Summary) {
		t.Errorf("Summary length mismatch: got %d, want %d", len(unmarshalled.Summary), len(snapshot.Summary))
	}
	for key, value := range snapshot.Summary {
		if unmarshalled.Summary[key] != value {
			t.Errorf("Summary[%s] mismatch: got %s, want %s", key, unmarshalled.Summary[key], value)
		}
	}
}

// TestExtractBucketAndPrefixFromWarehouseLocation tests extracting bucket and prefix from warehouse location
// Warehouse locationからバケット名とプレフィックスを抽出するテスト
func TestExtractBucketAndPrefixFromWarehouseLocation(t *testing.T) {
	tests := []struct {
		name              string
		warehouseLocation string
		wantBucket        string
		wantPrefix        string
		wantErr           bool
	}{
		{
			name:              "bucket only",
			warehouseLocation: "s3://test-bucket",
			wantBucket:        "test-bucket",
			wantPrefix:        "",
			wantErr:           false,
		},
		{
			name:              "bucket with prefix",
			warehouseLocation: "s3://test-bucket/warehouse/table1",
			wantBucket:        "test-bucket",
			wantPrefix:        "warehouse/table1",
			wantErr:           false,
		},
		{
			name:              "bucket with single level prefix",
			warehouseLocation: "s3://test-bucket/table",
			wantBucket:        "test-bucket",
			wantPrefix:        "table",
			wantErr:           false,
		},
		{
			name:              "invalid format - no s3 prefix",
			warehouseLocation: "test-bucket/table",
			wantBucket:        "",
			wantPrefix:        "",
			wantErr:           true,
		},
		{
			name:              "invalid format - empty",
			warehouseLocation: "",
			wantBucket:        "",
			wantPrefix:        "",
			wantErr:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, prefix, err := extractBucketAndPrefixFromWarehouseLocation(tt.warehouseLocation)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractBucketAndPrefixFromWarehouseLocation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if bucket != tt.wantBucket {
				t.Errorf("extractBucketAndPrefixFromWarehouseLocation() bucket = %v, want %v", bucket, tt.wantBucket)
			}
			if prefix != tt.wantPrefix {
				t.Errorf("extractBucketAndPrefixFromWarehouseLocation() prefix = %v, want %v", prefix, tt.wantPrefix)
			}
		})
	}
}

// TestGenerateMetadataFileName tests metadata file name generation
// メタデータファイル名生成のテスト
func TestGenerateMetadataFileName(t *testing.T) {
	tests := []struct {
		name    string
		version int
	}{
		{
			name:    "version 1",
			version: 1,
		},
		{
			name:    "version 10",
			version: 10,
		},
		{
			name:    "version 100",
			version: 100,
		},
		{
			name:    "version 12345",
			version: 12345,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileName := generateMetadataFileName(tt.version)

			// ファイル名が.metadata.jsonで終わることを確認
			if len(fileName) < 14 || fileName[len(fileName)-14:] != ".metadata.json" {
				t.Errorf("generateMetadataFileName() = %v, does not end with .metadata.json", fileName)
			}

			// 簡易チェック: 5桁の数字で始まることを確認
			if len(fileName) < 6 || fileName[5] != '-' {
				t.Errorf("generateMetadataFileName() = %v, does not have expected format {version}-{uuid}.metadata.json", fileName)
			}
		})
	}
}

// TestGenerateManifestFileName tests manifest file name generation
// マニフェストファイル名生成のテスト
func TestGenerateManifestFileName(t *testing.T) {
	tests := []struct {
		name       string
		snapshotID int64
	}{
		{
			name:       "snapshot 1",
			snapshotID: 1,
		},
		{
			name:       "snapshot 12345",
			snapshotID: 12345,
		},
		{
			name:       "snapshot with timestamp",
			snapshotID: 1234567890000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileName := generateManifestFileName(tt.snapshotID)

			// ファイル名が.manifest.jsonで終わることを確認
			if len(fileName) < 14 || fileName[len(fileName)-14:] != ".manifest.json" {
				t.Errorf("generateManifestFileName() = %v, does not end with .manifest.json", fileName)
			}

			// スナップショットIDで始まることを確認（簡易チェック）
			// UUIDの前にハイフンがあることを確認
			hasHyphen := false
			for i := 0; i < len(fileName); i++ {
				if fileName[i] == '-' {
					hasHyphen = true
					break
				}
			}
			if !hasHyphen {
				t.Errorf("generateManifestFileName() = %v, does not have expected format {snapshot-id}-{uuid}.manifest.json", fileName)
			}
		})
	}
}

// TestIsPrimitiveType tests IcebergSchemaField.IsPrimitiveType method
// IcebergSchemaField.IsPrimitiveTypeメソッドのテスト
func TestIsPrimitiveType(t *testing.T) {
	tests := []struct {
		name  string
		field IcebergSchemaField
		want  bool
	}{
		{
			name: "primitive type - string",
			field: IcebergSchemaField{
				ID:       1,
				Name:     "name",
				Required: true,
				Type:     "string",
			},
			want: true,
		},
		{
			name: "primitive type - long",
			field: IcebergSchemaField{
				ID:       2,
				Name:     "timestamp",
				Required: true,
				Type:     "long",
			},
			want: true,
		},
		{
			name: "primitive type - double",
			field: IcebergSchemaField{
				ID:       3,
				Name:     "value",
				Required: false,
				Type:     "double",
			},
			want: true,
		},
		{
			name: "complex type - map",
			field: IcebergSchemaField{
				ID:       4,
				Name:     "attributes",
				Required: false,
				Type: map[string]interface{}{
					"type":           "map",
					"key-id":         5,
					"key":            "string",
					"value-id":       6,
					"value":          "string",
					"value-required": false,
				},
			},
			want: false,
		},
		{
			name: "complex type - list",
			field: IcebergSchemaField{
				ID:       7,
				Name:     "items",
				Required: false,
				Type: map[string]interface{}{
					"type":            "list",
					"element-id":      8,
					"element":         "string",
					"element-required": false,
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.field.IsPrimitiveType()
			if got != tt.want {
				t.Errorf("IsPrimitiveType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsMapType tests IcebergSchemaField.IsMapType method
// IcebergSchemaField.IsMapTypeメソッドのテスト
func TestIsMapType(t *testing.T) {
	tests := []struct {
		name  string
		field IcebergSchemaField
		want  bool
	}{
		{
			name: "map type",
			field: IcebergSchemaField{
				ID:       1,
				Name:     "attributes",
				Required: false,
				Type: map[string]interface{}{
					"type":           "map",
					"key-id":         2,
					"key":            "string",
					"value-id":       3,
					"value":          "string",
					"value-required": false,
				},
			},
			want: true,
		},
		{
			name: "primitive type",
			field: IcebergSchemaField{
				ID:       4,
				Name:     "name",
				Required: true,
				Type:     "string",
			},
			want: false,
		},
		{
			name: "list type",
			field: IcebergSchemaField{
				ID:       5,
				Name:     "items",
				Required: false,
				Type: map[string]interface{}{
					"type":            "list",
					"element-id":      6,
					"element":         "string",
					"element-required": false,
				},
			},
			want: false,
		},
		{
			name: "map without type field",
			field: IcebergSchemaField{
				ID:       7,
				Name:     "invalid",
				Required: false,
				Type: map[string]interface{}{
					"key":   "string",
					"value": "string",
				},
			},
			want: false,
		},
		{
			name: "empty map",
			field: IcebergSchemaField{
				ID:       8,
				Name:     "empty",
				Required: false,
				Type:     map[string]interface{}{},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.field.IsMapType()
			if got != tt.want {
				t.Errorf("IsMapType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestGetPrimitiveType tests IcebergSchemaField.GetPrimitiveType method
// IcebergSchemaField.GetPrimitiveTypeメソッドのテスト
func TestGetPrimitiveType(t *testing.T) {
	tests := []struct {
		name    string
		field   IcebergSchemaField
		want    string
		wantErr bool
	}{
		{
			name: "primitive type - string",
			field: IcebergSchemaField{
				ID:       1,
				Name:     "name",
				Required: true,
				Type:     "string",
			},
			want:    "string",
			wantErr: false,
		},
		{
			name: "primitive type - long",
			field: IcebergSchemaField{
				ID:       2,
				Name:     "timestamp",
				Required: true,
				Type:     "long",
			},
			want:    "long",
			wantErr: false,
		},
		{
			name: "primitive type - timestamptz",
			field: IcebergSchemaField{
				ID:       3,
				Name:     "created_at",
				Required: false,
				Type:     "timestamptz",
			},
			want:    "timestamptz",
			wantErr: false,
		},
		{
			name: "complex type - map",
			field: IcebergSchemaField{
				ID:       4,
				Name:     "attributes",
				Required: false,
				Type: map[string]interface{}{
					"type":           "map",
					"key-id":         5,
					"key":            "string",
					"value-id":       6,
					"value":          "string",
					"value-required": false,
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "complex type - list",
			field: IcebergSchemaField{
				ID:       7,
				Name:     "items",
				Required: false,
				Type: map[string]interface{}{
					"type":            "list",
					"element-id":      8,
					"element":         "string",
					"element-required": false,
				},
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.field.GetPrimitiveType()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetPrimitiveType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetPrimitiveType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestGetMapType tests IcebergSchemaField.GetMapType method
// IcebergSchemaField.GetMapTypeメソッドのテスト
func TestGetMapType(t *testing.T) {
	tests := []struct {
		name    string
		field   IcebergSchemaField
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "map type",
			field: IcebergSchemaField{
				ID:       1,
				Name:     "attributes",
				Required: false,
				Type: map[string]interface{}{
					"type":           "map",
					"key-id":         2,
					"key":            "string",
					"value-id":       3,
					"value":          "string",
					"value-required": false,
				},
			},
			want: map[string]interface{}{
				"type":           "map",
				"key-id":         2,
				"key":            "string",
				"value-id":       3,
				"value":          "string",
				"value-required": false,
			},
			wantErr: false,
		},
		{
			name: "list type",
			field: IcebergSchemaField{
				ID:       4,
				Name:     "items",
				Required: false,
				Type: map[string]interface{}{
					"type":            "list",
					"element-id":      5,
					"element":         "string",
					"element-required": false,
				},
			},
			want: map[string]interface{}{
				"type":            "list",
				"element-id":      5,
				"element":         "string",
				"element-required": false,
			},
			wantErr: false,
		},
		{
			name: "primitive type - string",
			field: IcebergSchemaField{
				ID:       6,
				Name:     "name",
				Required: true,
				Type:     "string",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "primitive type - long",
			field: IcebergSchemaField{
				ID:       7,
				Name:     "timestamp",
				Required: true,
				Type:     "long",
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.field.GetMapType()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetMapType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if got != nil {
					t.Errorf("GetMapType() = %v, want nil", got)
				}
				return
			}

			// マップの内容を検証
			if len(got) != len(tt.want) {
				t.Errorf("GetMapType() map length = %v, want %v", len(got), len(tt.want))
			}
			for key, wantValue := range tt.want {
				gotValue, ok := got[key]
				if !ok {
					t.Errorf("GetMapType() missing key %v", key)
					continue
				}
				if gotValue != wantValue {
					t.Errorf("GetMapType()[%v] = %v, want %v", key, gotValue, wantValue)
				}
			}
		})
	}
}
