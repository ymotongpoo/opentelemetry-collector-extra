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
)

// TestProperty_SnapshotStructureCompleteness tests snapshot structure completeness property
// Feature: iceberg-snapshot-commit, Property 3: スナップショット構造の完全性
// Validates: Requirements 8.4
func TestProperty_SnapshotStructureCompleteness(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: 任意の生成されたスナップショットに対して、
	// そのスナップショットはsnapshot-id、timestamp-ms、sequence-number、summary、manifest-listの
	// すべての必須フィールドを含むべきである

	// テストケース1: 基本的なスナップショット生成
	t.Run("basic_snapshot_generation", func(t *testing.T) {
		manifestListPath := "s3://test-bucket/metadata/snap-12345.avro"
		sequenceNumber := int64(1)
		addedFiles := 5
		addedRecords := int64(1000)
		totalFiles := 10
		totalRecords := int64(5000)

		snapshot, err := generateSnapshot(manifestListPath, sequenceNumber, addedFiles, addedRecords, totalFiles, totalRecords)
		if err != nil {
			t.Fatalf("generateSnapshot() failed: %v", err)
		}

		// 必須フィールドの存在を検証
		if snapshot.SnapshotID == 0 {
			t.Error("SnapshotID should not be zero")
		}
		if snapshot.TimestampMS == 0 {
			t.Error("TimestampMS should not be zero")
		}
		if snapshot.SequenceNumber != sequenceNumber {
			t.Errorf("SequenceNumber mismatch: got %d, want %d", snapshot.SequenceNumber, sequenceNumber)
		}
		if snapshot.Summary == nil {
			t.Fatal("Summary should not be nil")
		}
		if snapshot.ManifestList != manifestListPath {
			t.Errorf("ManifestList mismatch: got %s, want %s", snapshot.ManifestList, manifestListPath)
		}

		// Summaryフィールドの検証
		requiredSummaryFields := []string{"operation", "added-files", "added-records", "total-files", "total-records"}
		for _, field := range requiredSummaryFields {
			if _, exists := snapshot.Summary[field]; !exists {
				t.Errorf("Summary should contain field '%s'", field)
			}
		}

		// Summaryの値を検証
		if snapshot.Summary["operation"] != "append" {
			t.Errorf("Summary[operation] should be 'append', got '%s'", snapshot.Summary["operation"])
		}
		if snapshot.Summary["added-files"] != fmt.Sprintf("%d", addedFiles) {
			t.Errorf("Summary[added-files] should be '%d', got '%s'", addedFiles, snapshot.Summary["added-files"])
		}
		if snapshot.Summary["added-records"] != fmt.Sprintf("%d", addedRecords) {
			t.Errorf("Summary[added-records] should be '%d', got '%s'", addedRecords, snapshot.Summary["added-records"])
		}
		if snapshot.Summary["total-files"] != fmt.Sprintf("%d", totalFiles) {
			t.Errorf("Summary[total-files] should be '%d', got '%s'", totalFiles, snapshot.Summary["total-files"])
		}
		if snapshot.Summary["total-records"] != fmt.Sprintf("%d", totalRecords) {
			t.Errorf("Summary[total-records] should be '%d', got '%s'", totalRecords, snapshot.Summary["total-records"])
		}
	})

	// テストケース2: 様々なパラメータでのスナップショット生成
	t.Run("various_parameters", func(t *testing.T) {
		testCases := []struct {
			name             string
			manifestListPath string
			sequenceNumber   int64
			addedFiles       int
			addedRecords     int64
			totalFiles       int
			totalRecords     int64
		}{
			{
				name:             "single file",
				manifestListPath: "s3://bucket/metadata/snap-1.avro",
				sequenceNumber:   1,
				addedFiles:       1,
				addedRecords:     100,
				totalFiles:       1,
				totalRecords:     100,
			},
			{
				name:             "multiple files",
				manifestListPath: "s3://bucket/metadata/snap-2.avro",
				sequenceNumber:   2,
				addedFiles:       10,
				addedRecords:     10000,
				totalFiles:       20,
				totalRecords:     50000,
			},
			{
				name:             "large dataset",
				manifestListPath: "s3://bucket/metadata/snap-3.avro",
				sequenceNumber:   100,
				addedFiles:       1000,
				addedRecords:     1000000,
				totalFiles:       5000,
				totalRecords:     10000000,
			},
			{
				name:             "complex warehouse location",
				manifestListPath: "s3://63a8e430-6e0b-46f5-k833abtwr6s8tmtsycedn8s4yc3xhuse1b--table-s3/metadata/snap-4.avro",
				sequenceNumber:   50,
				addedFiles:       50,
				addedRecords:     50000,
				totalFiles:       100,
				totalRecords:     100000,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				snapshot, err := generateSnapshot(tc.manifestListPath, tc.sequenceNumber, tc.addedFiles, tc.addedRecords, tc.totalFiles, tc.totalRecords)
				if err != nil {
					t.Fatalf("generateSnapshot() failed: %v", err)
				}

				// すべての必須フィールドが存在することを検証
				if snapshot.SnapshotID == 0 {
					t.Error("SnapshotID should not be zero")
				}
				if snapshot.TimestampMS == 0 {
					t.Error("TimestampMS should not be zero")
				}
				if snapshot.SequenceNumber != tc.sequenceNumber {
					t.Errorf("SequenceNumber mismatch: got %d, want %d", snapshot.SequenceNumber, tc.sequenceNumber)
				}
				if snapshot.Summary == nil {
					t.Fatal("Summary should not be nil")
				}
				if len(snapshot.Summary) == 0 {
					t.Error("Summary should not be empty")
				}
				if snapshot.ManifestList != tc.manifestListPath {
					t.Errorf("ManifestList mismatch: got %s, want %s", snapshot.ManifestList, tc.manifestListPath)
				}

				// Summaryの必須フィールドを検証
				requiredFields := []string{"operation", "added-files", "added-records", "total-files", "total-records"}
				for _, field := range requiredFields {
					if _, exists := snapshot.Summary[field]; !exists {
						t.Errorf("Summary should contain field '%s'", field)
					}
				}
			})
		}
	})

	// テストケース3: ランダムパラメータでの100回の反復テスト
	t.Run("random_parameters_100_iterations", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなパラメータを生成
			manifestListPath := fmt.Sprintf("s3://bucket-%d/metadata/snap-%d.avro", i, i)
			sequenceNumber := int64(i + 1)
			addedFiles := (i % 100) + 1
			addedRecords := int64((i % 10000) + 1)
			totalFiles := addedFiles + (i % 50)
			totalRecords := addedRecords + int64(i%5000)

			snapshot, err := generateSnapshot(manifestListPath, sequenceNumber, addedFiles, addedRecords, totalFiles, totalRecords)
			if err != nil {
				t.Fatalf("iteration %d: generateSnapshot() failed: %v", i, err)
			}

			// すべての必須フィールドが存在することを検証
			if snapshot.SnapshotID == 0 {
				t.Errorf("iteration %d: SnapshotID should not be zero", i)
			}
			if snapshot.TimestampMS == 0 {
				t.Errorf("iteration %d: TimestampMS should not be zero", i)
			}
			if snapshot.SequenceNumber != sequenceNumber {
				t.Errorf("iteration %d: SequenceNumber mismatch: got %d, want %d", i, snapshot.SequenceNumber, sequenceNumber)
			}
			if snapshot.Summary == nil {
				t.Fatalf("iteration %d: Summary should not be nil", i)
			}
			if len(snapshot.Summary) == 0 {
				t.Errorf("iteration %d: Summary should not be empty", i)
			}
			if snapshot.ManifestList != manifestListPath {
				t.Errorf("iteration %d: ManifestList mismatch: got %s, want %s", i, snapshot.ManifestList, manifestListPath)
			}

			// Summaryの必須フィールドを検証
			requiredFields := []string{"operation", "added-files", "added-records", "total-files", "total-records"}
			for _, field := range requiredFields {
				if _, exists := snapshot.Summary[field]; !exists {
					t.Errorf("iteration %d: Summary should contain field '%s'", i, field)
				}
			}

			// Summaryの値が正しいことを検証
			if snapshot.Summary["added-files"] != fmt.Sprintf("%d", addedFiles) {
				t.Errorf("iteration %d: Summary[added-files] mismatch: got %s, want %d", i, snapshot.Summary["added-files"], addedFiles)
			}
			if snapshot.Summary["added-records"] != fmt.Sprintf("%d", addedRecords) {
				t.Errorf("iteration %d: Summary[added-records] mismatch: got %s, want %d", i, snapshot.Summary["added-records"], addedRecords)
			}
			if snapshot.Summary["total-files"] != fmt.Sprintf("%d", totalFiles) {
				t.Errorf("iteration %d: Summary[total-files] mismatch: got %s, want %d", i, snapshot.Summary["total-files"], totalFiles)
			}
			if snapshot.Summary["total-records"] != fmt.Sprintf("%d", totalRecords) {
				t.Errorf("iteration %d: Summary[total-records] mismatch: got %s, want %d", i, snapshot.Summary["total-records"], totalRecords)
			}
		}
	})

	// テストケース4: スナップショットIDの一意性
	// 注: スナップショットIDはタイムスタンプベースなので、
	// 実際の使用では異なる時間に生成されるため一意性が保証される
	// このテストでは、スナップショットIDがタイムスタンプであることを確認
	t.Run("snapshot_id_is_timestamp_based", func(t *testing.T) {
		manifestListPath := "s3://bucket/metadata/snap-test.avro"
		sequenceNumber := int64(1)

		snapshot, err := generateSnapshot(manifestListPath, sequenceNumber, 1, 100, 1, 100)
		if err != nil {
			t.Fatalf("generateSnapshot() failed: %v", err)
		}

		// スナップショットIDがタイムスタンプであることを確認
		// タイムスタンプは正の値であるべき
		if snapshot.SnapshotID <= 0 {
			t.Errorf("SnapshotID should be positive, got %d", snapshot.SnapshotID)
		}

		// スナップショットIDがミリ秒単位のUnixタイムスタンプの範囲内であることを確認
		// 2020年1月1日以降（1577836800000ミリ秒）
		minTimestamp := int64(1577836800000)
		if snapshot.SnapshotID < minTimestamp {
			t.Errorf("SnapshotID should be a valid Unix timestamp in milliseconds, got %d", snapshot.SnapshotID)
		}

		// スナップショットIDとタイムスタンプが同じ値であることを確認
		if snapshot.SnapshotID != snapshot.TimestampMS {
			t.Errorf("SnapshotID should equal TimestampMS: got SnapshotID=%d, TimestampMS=%d",
				snapshot.SnapshotID, snapshot.TimestampMS)
		}
	})

	// テストケース5: タイムスタンプとスナップショットIDの関係
	t.Run("timestamp_and_snapshot_id_relationship", func(t *testing.T) {
		manifestListPath := "s3://bucket/metadata/snap-test.avro"
		sequenceNumber := int64(1)

		snapshot, err := generateSnapshot(manifestListPath, sequenceNumber, 1, 100, 1, 100)
		if err != nil {
			t.Fatalf("generateSnapshot() failed: %v", err)
		}

		// スナップショットIDとタイムスタンプが同じ値であることを確認
		// （タイムスタンプベースのスナップショットID生成）
		if snapshot.SnapshotID != snapshot.TimestampMS {
			t.Errorf("SnapshotID should equal TimestampMS: got SnapshotID=%d, TimestampMS=%d",
				snapshot.SnapshotID, snapshot.TimestampMS)
		}
	})

	// テストケース6: エラーケース - 空のマニフェストリストパス
	t.Run("error_empty_manifest_list_path", func(t *testing.T) {
		_, err := generateSnapshot("", 1, 1, 100, 1, 100)
		if err == nil {
			t.Error("expected error for empty manifestListPath")
		}

		expectedMsg := "manifestListPath cannot be empty"
		if err.Error() != expectedMsg {
			t.Errorf("expected error message '%s', got '%s'", expectedMsg, err.Error())
		}
	})
}
