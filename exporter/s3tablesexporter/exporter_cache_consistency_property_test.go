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
	"math/rand"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// TestProperty_CacheConsistency tests that cached table information is consistent
// Feature: remove-table-creation, Property 5: キャッシュの一貫性
// Validates: Requirements 3.1, 3.2
func TestProperty_CacheConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping property-based test in short mode")
	}

	// プロパティ: すべてのテーブル情報取得において、同じnamespaceとtableNameの組み合わせに対して、
	// キャッシュされた情報が一貫していること

	// テストケース1: 同じテーブルに対して複数回getTableInfoを呼び出し、一貫性を検証
	t.Run("multiple_calls_return_consistent_cached_info", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムなnamespaceとtableNameを生成
			namespace := generateRandomNamespace()
			tableName := generateRandomTableName()

			// ランダムなテーブル情報を生成
			expectedTableARN := fmt.Sprintf("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/%s/%s", namespace, tableName)
			expectedWarehouseLocation := fmt.Sprintf("s3://test-bucket/warehouse/%s/%s", namespace, tableName)
			expectedVersionToken := generateRandomVersionToken()

			// モックS3 Tablesクライアントを作成
			callCount := 0
			mockClient := &mockS3TablesClientForCache{
				getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
					callCount++
					return &s3tables.GetTableOutput{
						TableARN:          &expectedTableARN,
						WarehouseLocation: &expectedWarehouseLocation,
						VersionToken:      &expectedVersionToken,
					}, nil
				},
			}

			// エクスポーターを作成
			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      namespace,
				Tables: TableNamesConfig{
					Traces:  tableName,
					Metrics: tableName,
					Logs:    tableName,
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			exporter.s3TablesClient = mockClient

			// ランダムな回数（2-10回）getTableInfoを呼び出す
			numCalls := rand.Intn(9) + 2
			var tableInfos []*TableInfo

			for j := 0; j < numCalls; j++ {
				tableInfo, err := exporter.getTableInfo(context.Background(), namespace, tableName)
				if err != nil {
					t.Fatalf("iteration %d, call %d: getTableInfo() failed: %v", i, j, err)
				}
				tableInfos = append(tableInfos, tableInfo)
			}

			// GetTable APIは1回だけ呼び出されるべき（キャッシュが機能している）
			if callCount != 1 {
				t.Errorf("iteration %d: GetTable API was called %d times, expected 1 (cache should be used)", i, callCount)
			}

			// すべての呼び出しで同じ情報が返されることを検証
			for j := 1; j < len(tableInfos); j++ {
				if tableInfos[j].TableARN != tableInfos[0].TableARN {
					t.Errorf("iteration %d, call %d: TableARN mismatch: got %s, expected %s",
						i, j, tableInfos[j].TableARN, tableInfos[0].TableARN)
				}
				if tableInfos[j].WarehouseLocation != tableInfos[0].WarehouseLocation {
					t.Errorf("iteration %d, call %d: WarehouseLocation mismatch: got %s, expected %s",
						i, j, tableInfos[j].WarehouseLocation, tableInfos[0].WarehouseLocation)
				}
				if tableInfos[j].VersionToken != tableInfos[0].VersionToken {
					t.Errorf("iteration %d, call %d: VersionToken mismatch: got %s, expected %s",
						i, j, tableInfos[j].VersionToken, tableInfos[0].VersionToken)
				}
			}

			// 返された情報が期待値と一致することを検証
			if tableInfos[0].TableARN != expectedTableARN {
				t.Errorf("iteration %d: TableARN mismatch: got %s, expected %s",
					i, tableInfos[0].TableARN, expectedTableARN)
			}
			if tableInfos[0].WarehouseLocation != expectedWarehouseLocation {
				t.Errorf("iteration %d: WarehouseLocation mismatch: got %s, expected %s",
					i, tableInfos[0].WarehouseLocation, expectedWarehouseLocation)
			}
			if tableInfos[0].VersionToken != expectedVersionToken {
				t.Errorf("iteration %d: VersionToken mismatch: got %s, expected %s",
					i, tableInfos[0].VersionToken, expectedVersionToken)
			}
		}
	})

	// テストケース2: 異なるテーブルに対してキャッシュが独立していることを検証
	t.Run("different_tables_have_independent_cache", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// ランダムな数のテーブル（2-5個）を生成
			numTables := rand.Intn(4) + 2
			namespace := generateRandomNamespace()

			// 各テーブルの期待値を保存
			type tableExpectation struct {
				tableName         string
				tableARN          string
				warehouseLocation string
				versionToken      string
			}
			expectations := make([]tableExpectation, numTables)

			for j := 0; j < numTables; j++ {
				tableName := generateRandomTableName()
				expectations[j] = tableExpectation{
					tableName:         tableName,
					tableARN:          fmt.Sprintf("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/%s/%s", namespace, tableName),
					warehouseLocation: fmt.Sprintf("s3://test-bucket/warehouse/%s/%s", namespace, tableName),
					versionToken:      generateRandomVersionToken(),
				}
			}

			// モックS3 Tablesクライアントを作成
			callCounts := make(map[string]int)
			mockClient := &mockS3TablesClientForCache{
				getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
					tableName := *params.Name
					callCounts[tableName]++

					// 対応するテーブルの期待値を返す
					for _, exp := range expectations {
						if exp.tableName == tableName {
							return &s3tables.GetTableOutput{
								TableARN:          &exp.tableARN,
								WarehouseLocation: &exp.warehouseLocation,
								VersionToken:      &exp.versionToken,
							}, nil
						}
					}

					return nil, fmt.Errorf("table not found: %s", tableName)
				},
			}

			// エクスポーターを作成
			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      namespace,
				Tables: TableNamesConfig{
					Traces:  "dummy",
					Metrics: "dummy",
					Logs:    "dummy",
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			exporter.s3TablesClient = mockClient

			// 各テーブルに対してランダムな回数（2-5回）getTableInfoを呼び出す
			for _, exp := range expectations {
				numCalls := rand.Intn(4) + 2
				var tableInfos []*TableInfo

				for k := 0; k < numCalls; k++ {
					tableInfo, err := exporter.getTableInfo(context.Background(), namespace, exp.tableName)
					if err != nil {
						t.Fatalf("iteration %d, table %s, call %d: getTableInfo() failed: %v", i, exp.tableName, k, err)
					}
					tableInfos = append(tableInfos, tableInfo)
				}

				// GetTable APIは各テーブルに対して1回だけ呼び出されるべき
				if callCounts[exp.tableName] != 1 {
					t.Errorf("iteration %d, table %s: GetTable API was called %d times, expected 1",
						i, exp.tableName, callCounts[exp.tableName])
				}

				// すべての呼び出しで同じ情報が返されることを検証
				for k := 1; k < len(tableInfos); k++ {
					if tableInfos[k].TableARN != tableInfos[0].TableARN {
						t.Errorf("iteration %d, table %s, call %d: TableARN mismatch: got %s, expected %s",
							i, exp.tableName, k, tableInfos[k].TableARN, tableInfos[0].TableARN)
					}
					if tableInfos[k].WarehouseLocation != tableInfos[0].WarehouseLocation {
						t.Errorf("iteration %d, table %s, call %d: WarehouseLocation mismatch: got %s, expected %s",
							i, exp.tableName, k, tableInfos[k].WarehouseLocation, tableInfos[0].WarehouseLocation)
					}
					if tableInfos[k].VersionToken != tableInfos[0].VersionToken {
						t.Errorf("iteration %d, table %s, call %d: VersionToken mismatch: got %s, expected %s",
							i, exp.tableName, k, tableInfos[k].VersionToken, tableInfos[0].VersionToken)
					}
				}

				// 返された情報が期待値と一致することを検証
				if tableInfos[0].TableARN != exp.tableARN {
					t.Errorf("iteration %d, table %s: TableARN mismatch: got %s, expected %s",
						i, exp.tableName, tableInfos[0].TableARN, exp.tableARN)
				}
				if tableInfos[0].WarehouseLocation != exp.warehouseLocation {
					t.Errorf("iteration %d, table %s: WarehouseLocation mismatch: got %s, expected %s",
						i, exp.tableName, tableInfos[0].WarehouseLocation, exp.warehouseLocation)
				}
				if tableInfos[0].VersionToken != exp.versionToken {
					t.Errorf("iteration %d, table %s: VersionToken mismatch: got %s, expected %s",
						i, exp.tableName, tableInfos[0].VersionToken, exp.versionToken)
				}
			}
		}
	})

	// テストケース3: 異なるnamespaceで同じtableNameを持つテーブルのキャッシュが独立していることを検証
	t.Run("same_table_name_in_different_namespaces_have_independent_cache", func(t *testing.T) {
		iterations := 100
		for i := 0; i < iterations; i++ {
			// 同じテーブル名を使用
			tableName := generateRandomTableName()

			// ランダムな数のnamespace（2-4個）を生成
			numNamespaces := rand.Intn(3) + 2
			type namespaceExpectation struct {
				namespace         string
				tableARN          string
				warehouseLocation string
				versionToken      string
			}
			expectations := make([]namespaceExpectation, numNamespaces)

			for j := 0; j < numNamespaces; j++ {
				namespace := generateRandomNamespace()
				expectations[j] = namespaceExpectation{
					namespace:         namespace,
					tableARN:          fmt.Sprintf("arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/%s/%s", namespace, tableName),
					warehouseLocation: fmt.Sprintf("s3://test-bucket/warehouse/%s/%s", namespace, tableName),
					versionToken:      generateRandomVersionToken(),
				}
			}

			// モックS3 Tablesクライアントを作成
			callCounts := make(map[string]int)
			mockClient := &mockS3TablesClientForCache{
				getTableFunc: func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
					namespace := *params.Namespace
					cacheKey := fmt.Sprintf("%s.%s", namespace, tableName)
					callCounts[cacheKey]++

					// 対応するnamespaceの期待値を返す
					for _, exp := range expectations {
						if exp.namespace == namespace {
							return &s3tables.GetTableOutput{
								TableARN:          &exp.tableARN,
								WarehouseLocation: &exp.warehouseLocation,
								VersionToken:      &exp.versionToken,
							}, nil
						}
					}

					return nil, fmt.Errorf("table not found: %s.%s", namespace, tableName)
				},
			}

			// エクスポーターを作成
			cfg := &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket",
				Region:         "us-east-1",
				Namespace:      "dummy",
				Tables: TableNamesConfig{
					Traces:  tableName,
					Metrics: tableName,
					Logs:    tableName,
				},
			}
			set := exportertest.NewNopSettings(component.MustNewType("s3tables"))
			exporter, err := newS3TablesExporter(cfg, set)
			if err != nil {
				t.Fatalf("iteration %d: newS3TablesExporter() failed: %v", i, err)
			}

			exporter.s3TablesClient = mockClient

			// 各namespaceに対してランダムな回数（2-5回）getTableInfoを呼び出す
			for _, exp := range expectations {
				numCalls := rand.Intn(4) + 2
				var tableInfos []*TableInfo

				for k := 0; k < numCalls; k++ {
					tableInfo, err := exporter.getTableInfo(context.Background(), exp.namespace, tableName)
					if err != nil {
						t.Fatalf("iteration %d, namespace %s, call %d: getTableInfo() failed: %v", i, exp.namespace, k, err)
					}
					tableInfos = append(tableInfos, tableInfo)
				}

				cacheKey := fmt.Sprintf("%s.%s", exp.namespace, tableName)
				// GetTable APIは各namespace+tableNameの組み合わせに対して1回だけ呼び出されるべき
				if callCounts[cacheKey] != 1 {
					t.Errorf("iteration %d, namespace %s: GetTable API was called %d times, expected 1",
						i, exp.namespace, callCounts[cacheKey])
				}

				// すべての呼び出しで同じ情報が返されることを検証
				for k := 1; k < len(tableInfos); k++ {
					if tableInfos[k].TableARN != tableInfos[0].TableARN {
						t.Errorf("iteration %d, namespace %s, call %d: TableARN mismatch: got %s, expected %s",
							i, exp.namespace, k, tableInfos[k].TableARN, tableInfos[0].TableARN)
					}
					if tableInfos[k].WarehouseLocation != tableInfos[0].WarehouseLocation {
						t.Errorf("iteration %d, namespace %s, call %d: WarehouseLocation mismatch: got %s, expected %s",
							i, exp.namespace, k, tableInfos[k].WarehouseLocation, tableInfos[0].WarehouseLocation)
					}
					if tableInfos[k].VersionToken != tableInfos[0].VersionToken {
						t.Errorf("iteration %d, namespace %s, call %d: VersionToken mismatch: got %s, expected %s",
							i, exp.namespace, k, tableInfos[k].VersionToken, tableInfos[0].VersionToken)
					}
				}

				// 返された情報が期待値と一致することを検証
				if tableInfos[0].TableARN != exp.tableARN {
					t.Errorf("iteration %d, namespace %s: TableARN mismatch: got %s, expected %s",
						i, exp.namespace, tableInfos[0].TableARN, exp.tableARN)
				}
				if tableInfos[0].WarehouseLocation != exp.warehouseLocation {
					t.Errorf("iteration %d, namespace %s: WarehouseLocation mismatch: got %s, expected %s",
						i, exp.namespace, tableInfos[0].WarehouseLocation, exp.warehouseLocation)
				}
				if tableInfos[0].VersionToken != exp.versionToken {
					t.Errorf("iteration %d, namespace %s: VersionToken mismatch: got %s, expected %s",
						i, exp.namespace, tableInfos[0].VersionToken, exp.versionToken)
				}
			}
		}
	})
}

// mockS3TablesClientForCache is a mock S3 Tables client for cache consistency testing
// キャッシュ一貫性テスト用のモックS3 Tablesクライアント
type mockS3TablesClientForCache struct {
	getTableFunc func(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error)
}

func (m *mockS3TablesClientForCache) GetTable(ctx context.Context, params *s3tables.GetTableInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
	if m.getTableFunc != nil {
		return m.getTableFunc(ctx, params, optFns...)
	}
	return nil, fmt.Errorf("GetTable not implemented")
}

func (m *mockS3TablesClientForCache) GetNamespace(ctx context.Context, params *s3tables.GetNamespaceInput, optFns ...func(*s3tables.Options)) (*s3tables.GetNamespaceOutput, error) {
	return nil, fmt.Errorf("GetNamespace not implemented")
}

func (m *mockS3TablesClientForCache) GetTableMetadataLocation(ctx context.Context, params *s3tables.GetTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.GetTableMetadataLocationOutput, error) {
	return nil, fmt.Errorf("GetTableMetadataLocation not implemented")
}

func (m *mockS3TablesClientForCache) UpdateTableMetadataLocation(ctx context.Context, params *s3tables.UpdateTableMetadataLocationInput, optFns ...func(*s3tables.Options)) (*s3tables.UpdateTableMetadataLocationOutput, error) {
	return nil, fmt.Errorf("UpdateTableMetadataLocation not implemented")
}

// generateRandomVersionToken generates a random version token for testing
// テスト用のランダムなバージョントークンを生成
func generateRandomVersionToken() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	length := 32

	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}
