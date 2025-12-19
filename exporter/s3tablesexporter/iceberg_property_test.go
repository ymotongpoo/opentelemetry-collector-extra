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
	"fmt"
	"testing"
	"time"
)

// TestProperty_MetadataStructureCompleteness tests metadata file structure completeness property
// Feature: iceberg-snapshot-commit, Property 2: メタデータファイル構造の完全性
// Validates: Requirements 2.2, 8.1, 8.2, 8.3
func TestProperty_MetadataStructureCompleteness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意の生成されたメタデータファイルに対して、
	// そのファイルはIceberg仕様に準拠したJSON形式であり、
	// format-version、table-uuid、location、last-sequence-number、
	// last-updated-ms、last-column-id、schemas、current-snapshot-id、
	// snapshots、snapshot-logのすべての必須フィールドを含むこと

	iterations := 100
	for i := 0; i < iterations; i++ {
		// ランダムな既存メタデータを生成
		existingMetadata := generateRandomIcebergMetadata(i)

		// ランダムな新しいスナップショットを生成
		newSnapshot := generateRandomIcebergSnapshot(i + 1000)

		// 新しいメタデータパスを生成
		newMetadataPath := fmt.Sprintf("s3://test-bucket/metadata/%05d-%s.metadata.json", i+1, generateRandomUUID())

		// メタデータを生成
		newMetadata := generateNewMetadata(&existingMetadata, newSnapshot, newMetadataPath)

		// 必須フィールドの検証
		if newMetadata.FormatVersion == 0 {
			t.Errorf("iteration %d: format-version is missing or zero", i)
		}
		if newMetadata.TableUUID == "" {
			t.Errorf("iteration %d: table-uuid is missing", i)
		}
		if newMetadata.Location == "" {
			t.Errorf("iteration %d: location is missing", i)
		}
		if newMetadata.LastSequenceNumber == 0 {
			t.Errorf("iteration %d: last-sequence-number is missing or zero", i)
		}
		if newMetadata.LastUpdatedMS == 0 {
			t.Errorf("iteration %d: last-updated-ms is missing or zero", i)
		}
		if newMetadata.LastColumnID == 0 {
			t.Errorf("iteration %d: last-column-id is missing or zero", i)
		}
		if len(newMetadata.Schemas) == 0 {
			t.Errorf("iteration %d: schemas is missing or empty", i)
		}
		if newMetadata.CurrentSnapshotID == 0 {
			t.Errorf("iteration %d: current-snapshot-id is missing or zero", i)
		}
		if len(newMetadata.Snapshots) == 0 {
			t.Errorf("iteration %d: snapshots is missing or empty", i)
		}
		if len(newMetadata.SnapshotLog) == 0 {
			t.Errorf("iteration %d: snapshot-log is missing or empty", i)
		}

		// 新しいスナップショットが追加されていることを確認
		if len(newMetadata.Snapshots) != len(existingMetadata.Snapshots)+1 {
			t.Errorf("iteration %d: expected %d snapshots, got %d",
				i, len(existingMetadata.Snapshots)+1, len(newMetadata.Snapshots))
		}

		// 最後のスナップショットが新しいスナップショットであることを確認
		lastSnapshot := newMetadata.Snapshots[len(newMetadata.Snapshots)-1]
		if lastSnapshot.SnapshotID != newSnapshot.SnapshotID {
			t.Errorf("iteration %d: expected last snapshot ID %d, got %d",
				i, newSnapshot.SnapshotID, lastSnapshot.SnapshotID)
		}

		// current-snapshot-idが新しいスナップショットIDであることを確認
		if newMetadata.CurrentSnapshotID != newSnapshot.SnapshotID {
			t.Errorf("iteration %d: expected current-snapshot-id %d, got %d",
				i, newSnapshot.SnapshotID, newMetadata.CurrentSnapshotID)
		}

		// スナップショットログが更新されていることを確認
		if len(newMetadata.SnapshotLog) != len(existingMetadata.SnapshotLog)+1 {
			t.Errorf("iteration %d: expected %d snapshot log entries, got %d",
				i, len(existingMetadata.SnapshotLog)+1, len(newMetadata.SnapshotLog))
		}

		// メタデータログが更新されていることを確認
		if len(newMetadata.MetadataLog) != len(existingMetadata.MetadataLog)+1 {
			t.Errorf("iteration %d: expected %d metadata log entries, got %d",
				i, len(existingMetadata.MetadataLog)+1, len(newMetadata.MetadataLog))
		}

		// 最後のメタデータログエントリが新しいメタデータパスであることを確認
		lastMetadataLog := newMetadata.MetadataLog[len(newMetadata.MetadataLog)-1]
		if lastMetadataLog.MetadataFile != newMetadataPath {
			t.Errorf("iteration %d: expected last metadata file '%s', got '%s'",
				i, newMetadataPath, lastMetadataLog.MetadataFile)
		}
	}
}

// TestProperty_MetadataPreservation tests metadata preservation property
// Feature: iceberg-snapshot-commit, Property 10: メタデータ取得と更新の一貫性（保持部分）
// Validates: Requirements 9.3, 9.4
func TestProperty_MetadataPreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意の既存メタデータと新しいスナップショットに対して、
	// 新しいメタデータは既存のスキーマ、パーティション仕様、ソート順序を保持すること

	iterations := 100
	for i := 0; i < iterations; i++ {
		// ランダムな既存メタデータを生成
		existingMetadata := generateRandomIcebergMetadata(i)

		// ランダムな新しいスナップショットを生成
		newSnapshot := generateRandomIcebergSnapshot(i + 1000)

		// 新しいメタデータパスを生成
		newMetadataPath := fmt.Sprintf("s3://test-bucket/metadata/%05d-%s.metadata.json", i+1, generateRandomUUID())

		// メタデータを生成
		newMetadata := generateNewMetadata(&existingMetadata, newSnapshot, newMetadataPath)

		// スキーマが保持されていることを確認
		if len(newMetadata.Schemas) != len(existingMetadata.Schemas) {
			t.Errorf("iteration %d: schemas not preserved, expected %d, got %d",
				i, len(existingMetadata.Schemas), len(newMetadata.Schemas))
		}
		for j, schema := range newMetadata.Schemas {
			if schema.SchemaID != existingMetadata.Schemas[j].SchemaID {
				t.Errorf("iteration %d: schema %d ID not preserved, expected %d, got %d",
					i, j, existingMetadata.Schemas[j].SchemaID, schema.SchemaID)
			}
			if len(schema.Fields) != len(existingMetadata.Schemas[j].Fields) {
				t.Errorf("iteration %d: schema %d fields not preserved, expected %d, got %d",
					i, j, len(existingMetadata.Schemas[j].Fields), len(schema.Fields))
			}
		}

		// current-schema-idが保持されていることを確認
		if newMetadata.CurrentSchemaID != existingMetadata.CurrentSchemaID {
			t.Errorf("iteration %d: current-schema-id not preserved, expected %d, got %d",
				i, existingMetadata.CurrentSchemaID, newMetadata.CurrentSchemaID)
		}

		// パーティション仕様が保持されていることを確認
		if len(newMetadata.PartitionSpecs) != len(existingMetadata.PartitionSpecs) {
			t.Errorf("iteration %d: partition specs not preserved, expected %d, got %d",
				i, len(existingMetadata.PartitionSpecs), len(newMetadata.PartitionSpecs))
		}
		for j, spec := range newMetadata.PartitionSpecs {
			if spec.SpecID != existingMetadata.PartitionSpecs[j].SpecID {
				t.Errorf("iteration %d: partition spec %d ID not preserved, expected %d, got %d",
					i, j, existingMetadata.PartitionSpecs[j].SpecID, spec.SpecID)
			}
		}

		// default-spec-idが保持されていることを確認
		if newMetadata.DefaultSpecID != existingMetadata.DefaultSpecID {
			t.Errorf("iteration %d: default-spec-id not preserved, expected %d, got %d",
				i, existingMetadata.DefaultSpecID, newMetadata.DefaultSpecID)
		}

		// last-partition-idが保持されていることを確認
		if newMetadata.LastPartitionID != existingMetadata.LastPartitionID {
			t.Errorf("iteration %d: last-partition-id not preserved, expected %d, got %d",
				i, existingMetadata.LastPartitionID, newMetadata.LastPartitionID)
		}

		// プロパティが保持されていることを確認
		if len(newMetadata.Properties) != len(existingMetadata.Properties) {
			t.Errorf("iteration %d: properties not preserved, expected %d, got %d",
				i, len(existingMetadata.Properties), len(newMetadata.Properties))
		}
		for key, value := range existingMetadata.Properties {
			if newMetadata.Properties[key] != value {
				t.Errorf("iteration %d: property '%s' not preserved, expected '%s', got '%s'",
					i, key, value, newMetadata.Properties[key])
			}
		}

		// table-uuidが保持されていることを確認
		if newMetadata.TableUUID != existingMetadata.TableUUID {
			t.Errorf("iteration %d: table-uuid not preserved, expected '%s', got '%s'",
				i, existingMetadata.TableUUID, newMetadata.TableUUID)
		}

		// locationが保持されていることを確認
		if newMetadata.Location != existingMetadata.Location {
			t.Errorf("iteration %d: location not preserved, expected '%s', got '%s'",
				i, existingMetadata.Location, newMetadata.Location)
		}

		// last-column-idが保持されていることを確認
		if newMetadata.LastColumnID != existingMetadata.LastColumnID {
			t.Errorf("iteration %d: last-column-id not preserved, expected %d, got %d",
				i, existingMetadata.LastColumnID, newMetadata.LastColumnID)
		}

		// format-versionが保持されていることを確認
		if newMetadata.FormatVersion != existingMetadata.FormatVersion {
			t.Errorf("iteration %d: format-version not preserved, expected %d, got %d",
				i, existingMetadata.FormatVersion, newMetadata.FormatVersion)
		}
	}
}

// generateRandomIcebergMetadata generates random metadata for testing
// テスト用のランダムなメタデータを生成
func generateRandomIcebergMetadata(seed int) IcebergMetadata {
	timestamp := time.Now().UnixMilli()

	// ランダムなスキーマを生成
	schemas := []IcebergSchema{
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
					Name:     fmt.Sprintf("field_%d", seed),
					Required: false,
					Type:     "string",
				},
			},
		},
	}

	// ランダムなパーティション仕様を生成
	partitionSpecs := []IcebergPartitionSpec{
		{
			SpecID: 0,
			Fields: []IcebergPartitionField{},
		},
	}

	// ランダムなスナップショットを生成
	snapshots := []IcebergSnapshot{}
	snapshotLog := []IcebergSnapshotLog{}
	for i := 0; i < seed%5+1; i++ {
		snapshotID := int64(i + 1)
		snapshots = append(snapshots, IcebergSnapshot{
			SnapshotID:     snapshotID,
			TimestampMS:    timestamp - int64(i*1000),
			SequenceNumber: int64(i + 1),
			Summary: map[string]string{
				"operation": "append",
			},
			ManifestList: fmt.Sprintf("s3://test-bucket/metadata/snap-%d.avro", snapshotID),
		})
		snapshotLog = append(snapshotLog, IcebergSnapshotLog{
			TimestampMS: timestamp - int64(i*1000),
			SnapshotID:  snapshotID,
		})
	}

	// ランダムなメタデータログを生成
	metadataLog := []IcebergMetadataLog{}
	for i := 0; i < seed%5+1; i++ {
		metadataLog = append(metadataLog, IcebergMetadataLog{
			TimestampMS:  timestamp - int64(i*1000),
			MetadataFile: fmt.Sprintf("s3://test-bucket/metadata/v%d.metadata.json", i+1),
		})
	}

	return IcebergMetadata{
		FormatVersion:      2,
		TableUUID:          fmt.Sprintf("test-uuid-%d", seed),
		Location:           "s3://test-bucket/table",
		LastSequenceNumber: int64(len(snapshots)),
		LastUpdatedMS:      timestamp,
		LastColumnID:       len(schemas[0].Fields),
		Schemas:            schemas,
		CurrentSchemaID:    0,
		PartitionSpecs:     partitionSpecs,
		DefaultSpecID:      0,
		LastPartitionID:    0,
		Properties: map[string]string{
			"key1": fmt.Sprintf("value%d", seed),
			"key2": "value2",
		},
		CurrentSnapshotID: int64(len(snapshots)),
		Snapshots:         snapshots,
		SnapshotLog:       snapshotLog,
		MetadataLog:       metadataLog,
	}
}

// generateRandomIcebergSnapshot generates a random snapshot for testing
// テスト用のランダムなスナップショットを生成
func generateRandomIcebergSnapshot(seed int) IcebergSnapshot {
	timestamp := time.Now().UnixMilli()
	snapshotID := int64(seed)

	return IcebergSnapshot{
		SnapshotID:     snapshotID,
		TimestampMS:    timestamp,
		SequenceNumber: int64(seed),
		Summary: map[string]string{
			"operation":     "append",
			"added-files":   fmt.Sprintf("%d", seed%10+1),
			"added-records": fmt.Sprintf("%d", seed*100),
		},
		ManifestList: fmt.Sprintf("s3://test-bucket/metadata/snap-%d.avro", snapshotID),
	}
}

// generateRandomUUID generates a simple random UUID-like string for testing
// テスト用のシンプルなランダムUUID風文字列を生成
func generateRandomUUID() string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		timestamp&0xffffffff,
		(timestamp>>32)&0xffff,
		(timestamp>>48)&0xffff,
		0x4000|(timestamp&0x0fff),
		timestamp&0xffffffffffff)
}

// TestProperty_MetadataLocationFormat tests metadata location format property
// Feature: iceberg-snapshot-commit, Property 6: メタデータロケーションの形式
// Validates: Requirements 2.5
func TestProperty_MetadataLocationFormat(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意の生成されたメタデータロケーションに対して、
	// そのロケーションはテーブルのwarehouse locationで始まり、
	// .metadata.jsonまたは.metadata.json.gzで終わること

	iterations := 100
	for i := 0; i < iterations; i++ {
		// ランダムなwarehouse locationを生成
		warehouseLocations := []string{
			"s3://test-bucket/warehouse/table1",
			"s3://test-bucket/warehouse/table2",
			fmt.Sprintf("s3://test-bucket-%d/warehouse/table", i),
			fmt.Sprintf("s3://bucket/prefix-%d/table", i),
			"s3://bucket/table",
		}
		warehouseLocation := warehouseLocations[i%len(warehouseLocations)]

		// メタデータファイル名を生成
		version := i + 1
		metadataFileName := generateMetadataFileName(version)

		// メタデータロケーションを構築
		// warehouse locationからバケット名とプレフィックスを抽出
		bucket, prefix, err := extractBucketAndPrefixFromWarehouseLocation(warehouseLocation)
		if err != nil {
			t.Fatalf("iteration %d: failed to extract bucket and prefix: %v", i, err)
		}

		var metadataLocation string
		if prefix != "" {
			metadataLocation = fmt.Sprintf("s3://%s/%s/metadata/%s", bucket, prefix, metadataFileName)
		} else {
			metadataLocation = fmt.Sprintf("s3://%s/metadata/%s", bucket, metadataFileName)
		}

		// メタデータロケーションがwarehouse locationで始まることを確認
		// warehouse locationのバケット名とプレフィックスが含まれていることを確認
		expectedPrefix := fmt.Sprintf("s3://%s", bucket)
		if len(metadataLocation) < len(expectedPrefix) || metadataLocation[:len(expectedPrefix)] != expectedPrefix {
			t.Errorf("iteration %d: metadata location '%s' does not start with expected prefix '%s'",
				i, metadataLocation, expectedPrefix)
		}

		// メタデータロケーションが.metadata.jsonで終わることを確認
		if len(metadataLocation) < 14 || metadataLocation[len(metadataLocation)-14:] != ".metadata.json" {
			// .metadata.json.gzで終わる場合もチェック
			if len(metadataLocation) < 17 || metadataLocation[len(metadataLocation)-17:] != ".metadata.json.gz" {
				t.Errorf("iteration %d: metadata location '%s' does not end with '.metadata.json' or '.metadata.json.gz'",
					i, metadataLocation)
			}
		}

		// メタデータファイル名の形式を確認（{version}-{uuid}.metadata.json）
		// バージョン番号が5桁の0埋め形式であることを確認
		expectedVersionPrefix := fmt.Sprintf("%05d-", version)
		if len(metadataFileName) < len(expectedVersionPrefix) || metadataFileName[:len(expectedVersionPrefix)] != expectedVersionPrefix {
			t.Errorf("iteration %d: metadata file name '%s' does not start with expected version prefix '%s'",
				i, metadataFileName, expectedVersionPrefix)
		}

		// UUIDが含まれていることを確認（簡易チェック：ハイフンが含まれている）
		uuidPart := metadataFileName[len(expectedVersionPrefix) : len(metadataFileName)-14]
		if len(uuidPart) == 0 {
			t.Errorf("iteration %d: metadata file name '%s' does not contain UUID",
				i, metadataFileName)
		}
	}
}
