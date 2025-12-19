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
			"operation":      "append",
			"added-files":    "1",
			"added-records":  "100",
			"total-files":    "1",
			"total-records":  "100",
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
