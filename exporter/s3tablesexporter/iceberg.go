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

// IcebergMetadata represents the structure of an Iceberg metadata file
// Icebergメタデータファイルの構造を表現
type IcebergMetadata struct {
	FormatVersion      int                    `json:"format-version"`
	TableUUID          string                 `json:"table-uuid"`
	Location           string                 `json:"location"`
	LastSequenceNumber int64                  `json:"last-sequence-number"`
	LastUpdatedMS      int64                  `json:"last-updated-ms"`
	LastColumnID       int                    `json:"last-column-id"`
	Schemas            []IcebergSchema        `json:"schemas"`
	CurrentSchemaID    int                    `json:"current-schema-id"`
	PartitionSpecs     []IcebergPartitionSpec `json:"partition-specs"`
	DefaultSpecID      int                    `json:"default-spec-id"`
	LastPartitionID    int                    `json:"last-partition-id"`
	Properties         map[string]string      `json:"properties"`
	CurrentSnapshotID  int64                  `json:"current-snapshot-id"`
	Snapshots          []IcebergSnapshot      `json:"snapshots"`
	SnapshotLog        []IcebergSnapshotLog   `json:"snapshot-log"`
	MetadataLog        []IcebergMetadataLog   `json:"metadata-log"`
}

// IcebergSchema represents an Iceberg schema
// Icebergスキーマを表現
type IcebergSchema struct {
	SchemaID int                  `json:"schema-id"`
	Fields   []IcebergSchemaField `json:"fields"`
}

// IcebergSchemaField represents a field in an Iceberg schema
// Icebergスキーマのフィールドを表現
type IcebergSchemaField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
}

// IcebergPartitionSpec represents an Iceberg partition specification
// Icebergパーティション仕様を表現
type IcebergPartitionSpec struct {
	SpecID int                     `json:"spec-id"`
	Fields []IcebergPartitionField `json:"fields"`
}

// IcebergPartitionField represents a field in an Iceberg partition specification
// Icebergパーティション仕様のフィールドを表現
type IcebergPartitionField struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

// IcebergSnapshot represents an Iceberg snapshot
// Icebergスナップショットを表現
type IcebergSnapshot struct {
	SnapshotID     int64             `json:"snapshot-id"`
	TimestampMS    int64             `json:"timestamp-ms"`
	SequenceNumber int64             `json:"sequence-number"`
	Summary        map[string]string `json:"summary"`
	ManifestList   string            `json:"manifest-list"`
}

// IcebergSnapshotLog represents an entry in the snapshot log
// スナップショットログのエントリを表現
type IcebergSnapshotLog struct {
	TimestampMS int64 `json:"timestamp-ms"`
	SnapshotID  int64 `json:"snapshot-id"`
}

// IcebergMetadataLog represents an entry in the metadata log
// メタデータログのエントリを表現
type IcebergMetadataLog struct {
	TimestampMS  int64  `json:"timestamp-ms"`
	MetadataFile string `json:"metadata-file"`
}

// IcebergManifest represents the structure of an Iceberg manifest file
// Icebergマニフェストファイルの構造を表現
type IcebergManifest struct {
	FormatVersion int                    `json:"format-version"`
	Content       string                 `json:"content"` // "data" or "deletes"
	Entries       []IcebergManifestEntry `json:"entries"`
}

// IcebergManifestEntry represents an entry in an Iceberg manifest
// Icebergマニフェストのエントリを表現
type IcebergManifestEntry struct {
	Status         int             `json:"status"` // 0=EXISTING, 1=ADDED, 2=DELETED
	SnapshotID     int64           `json:"snapshot-id"`
	SequenceNumber int64           `json:"sequence-number"`
	DataFile       IcebergDataFile `json:"data-file"`
}

// IcebergDataFile represents a data file in an Iceberg manifest
// Icebergマニフェスト内のデータファイルを表現
type IcebergDataFile struct {
	FilePath        string         `json:"file-path"`
	FileFormat      string         `json:"file-format"` // "PARQUET"
	RecordCount     int64          `json:"record-count"`
	FileSizeBytes   int64          `json:"file-size-in-bytes"`
	ColumnSizes     map[int]int64  `json:"column-sizes,omitempty"`
	ValueCounts     map[int]int64  `json:"value-counts,omitempty"`
	NullValueCounts map[int]int64  `json:"null-value-counts,omitempty"`
	LowerBounds     map[int][]byte `json:"lower-bounds,omitempty"`
	UpperBounds     map[int][]byte `json:"upper-bounds,omitempty"`
}

// generateNewMetadata generates a new metadata file with a new snapshot
// 新しいスナップショットを含む新しいメタデータファイルを生成
//
// この関数は既存のメタデータに新しいスナップショットを追加し、
// 既存のスキーマ、パーティション仕様、ソート順序を保持します。
func generateNewMetadata(
	existingMetadata *IcebergMetadata,
	newSnapshot IcebergSnapshot,
	newMetadataPath string,
) *IcebergMetadata {
	// 既存のメタデータをコピー
	newMetadata := &IcebergMetadata{
		FormatVersion:      existingMetadata.FormatVersion,
		TableUUID:          existingMetadata.TableUUID,
		Location:           existingMetadata.Location,
		LastSequenceNumber: newSnapshot.SequenceNumber,
		LastUpdatedMS:      newSnapshot.TimestampMS,
		LastColumnID:       existingMetadata.LastColumnID,
		Schemas:            existingMetadata.Schemas,
		CurrentSchemaID:    existingMetadata.CurrentSchemaID,
		PartitionSpecs:     existingMetadata.PartitionSpecs,
		DefaultSpecID:      existingMetadata.DefaultSpecID,
		LastPartitionID:    existingMetadata.LastPartitionID,
		Properties:         existingMetadata.Properties,
		CurrentSnapshotID:  newSnapshot.SnapshotID,
	}

	// 新しいスナップショットを追加
	newMetadata.Snapshots = make([]IcebergSnapshot, len(existingMetadata.Snapshots)+1)
	copy(newMetadata.Snapshots, existingMetadata.Snapshots)
	newMetadata.Snapshots[len(existingMetadata.Snapshots)] = newSnapshot

	// スナップショットログを更新
	newMetadata.SnapshotLog = make([]IcebergSnapshotLog, len(existingMetadata.SnapshotLog)+1)
	copy(newMetadata.SnapshotLog, existingMetadata.SnapshotLog)
	newMetadata.SnapshotLog[len(existingMetadata.SnapshotLog)] = IcebergSnapshotLog{
		TimestampMS: newSnapshot.TimestampMS,
		SnapshotID:  newSnapshot.SnapshotID,
	}

	// メタデータログを更新
	newMetadata.MetadataLog = make([]IcebergMetadataLog, len(existingMetadata.MetadataLog)+1)
	copy(newMetadata.MetadataLog, existingMetadata.MetadataLog)
	newMetadata.MetadataLog[len(existingMetadata.MetadataLog)] = IcebergMetadataLog{
		TimestampMS:  newSnapshot.TimestampMS,
		MetadataFile: newMetadataPath,
	}

	return newMetadata
}
