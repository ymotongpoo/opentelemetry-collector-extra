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
	"github.com/apache/iceberg-go"
)

// createMetricsSchema creates an Iceberg schema for metrics data
// メトリクスデータのIcebergスキーマを作成
// Schema:
//   - timestamp (timestamp with timezone) - メトリクスのタイムスタンプ
//   - resource_attributes (map<string, string>) - リソース属性
//   - metric_name (string) - メトリクス名
//   - metric_type (string) - メトリクスタイプ（gauge, sum, histogram, etc.）
//   - value (double) - メトリクス値
//   - attributes (map<string, string>) - メトリクス属性
func createMetricsSchema() *iceberg.Schema {
	return iceberg.NewSchema(
		0, // schema ID
		iceberg.NestedField{
			ID:       1,
			Name:     "timestamp",
			Type:     iceberg.PrimitiveTypes.TimestampTz,
			Required: true,
			Doc:      "メトリクスのタイムスタンプ",
		},
		iceberg.NestedField{
			ID:   2,
			Name: "resource_attributes",
			Type: &iceberg.MapType{
				KeyID:      3,
				KeyType:    iceberg.PrimitiveTypes.String,
				ValueID:    4,
				ValueType:  iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			},
			Required: false,
			Doc:      "リソース属性",
		},
		iceberg.NestedField{
			ID:       5,
			Name:     "metric_name",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
			Doc:      "メトリクス名",
		},
		iceberg.NestedField{
			ID:       6,
			Name:     "metric_type",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
			Doc:      "メトリクスタイプ（gauge, sum, histogram, etc.）",
		},
		iceberg.NestedField{
			ID:       7,
			Name:     "value",
			Type:     iceberg.PrimitiveTypes.Float64,
			Required: true,
			Doc:      "メトリクス値",
		},
		iceberg.NestedField{
			ID:   8,
			Name: "attributes",
			Type: &iceberg.MapType{
				KeyID:      9,
				KeyType:    iceberg.PrimitiveTypes.String,
				ValueID:    10,
				ValueType:  iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			},
			Required: false,
			Doc:      "メトリクス属性",
		},
	)
}

// createTracesSchema creates an Iceberg schema for traces data
// トレースデータのIcebergスキーマを作成
// Schema:
//   - trace_id (binary) - トレースID
//   - span_id (binary) - スパンID
//   - parent_span_id (binary) - 親スパンID
//   - name (string) - スパン名
//   - start_time (timestamp with timezone) - 開始時刻
//   - end_time (timestamp with timezone) - 終了時刻
//   - attributes (map<string, string>) - スパン属性
//   - resource_attributes (map<string, string>) - リソース属性
func createTracesSchema() *iceberg.Schema {
	return iceberg.NewSchema(
		0, // schema ID
		iceberg.NestedField{
			ID:       1,
			Name:     "trace_id",
			Type:     iceberg.PrimitiveTypes.Binary,
			Required: true,
			Doc:      "トレースID",
		},
		iceberg.NestedField{
			ID:       2,
			Name:     "span_id",
			Type:     iceberg.PrimitiveTypes.Binary,
			Required: true,
			Doc:      "スパンID",
		},
		iceberg.NestedField{
			ID:       3,
			Name:     "parent_span_id",
			Type:     iceberg.PrimitiveTypes.Binary,
			Required: false,
			Doc:      "親スパンID",
		},
		iceberg.NestedField{
			ID:       4,
			Name:     "name",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
			Doc:      "スパン名",
		},
		iceberg.NestedField{
			ID:       5,
			Name:     "start_time",
			Type:     iceberg.PrimitiveTypes.TimestampTz,
			Required: true,
			Doc:      "開始時刻",
		},
		iceberg.NestedField{
			ID:       6,
			Name:     "end_time",
			Type:     iceberg.PrimitiveTypes.TimestampTz,
			Required: true,
			Doc:      "終了時刻",
		},
		iceberg.NestedField{
			ID:   7,
			Name: "attributes",
			Type: &iceberg.MapType{
				KeyID:      8,
				KeyType:    iceberg.PrimitiveTypes.String,
				ValueID:    9,
				ValueType:  iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			},
			Required: false,
			Doc:      "スパン属性",
		},
		iceberg.NestedField{
			ID:   10,
			Name: "resource_attributes",
			Type: &iceberg.MapType{
				KeyID:      11,
				KeyType:    iceberg.PrimitiveTypes.String,
				ValueID:    12,
				ValueType:  iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			},
			Required: false,
			Doc:      "リソース属性",
		},
	)
}

// createLogsSchema creates an Iceberg schema for logs data
// ログデータのIcebergスキーマを作成
// Schema:
//   - timestamp (timestamp with timezone) - ログのタイムスタンプ
//   - severity (string) - ログレベル
//   - body (string) - ログメッセージ
//   - attributes (map<string, string>) - ログ属性
//   - resource_attributes (map<string, string>) - リソース属性
func createLogsSchema() *iceberg.Schema {
	return iceberg.NewSchema(
		0, // schema ID
		iceberg.NestedField{
			ID:       1,
			Name:     "timestamp",
			Type:     iceberg.PrimitiveTypes.TimestampTz,
			Required: true,
			Doc:      "ログのタイムスタンプ",
		},
		iceberg.NestedField{
			ID:       2,
			Name:     "severity",
			Type:     iceberg.PrimitiveTypes.String,
			Required: false,
			Doc:      "ログレベル",
		},
		iceberg.NestedField{
			ID:       3,
			Name:     "body",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
			Doc:      "ログメッセージ",
		},
		iceberg.NestedField{
			ID:   4,
			Name: "attributes",
			Type: &iceberg.MapType{
				KeyID:      5,
				KeyType:    iceberg.PrimitiveTypes.String,
				ValueID:    6,
				ValueType:  iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			},
			Required: false,
			Doc:      "ログ属性",
		},
		iceberg.NestedField{
			ID:   7,
			Name: "resource_attributes",
			Type: &iceberg.MapType{
				KeyID:      8,
				KeyType:    iceberg.PrimitiveTypes.String,
				ValueID:    9,
				ValueType:  iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			},
			Required: false,
			Doc:      "リソース属性",
		},
	)
}
