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

// createMetricsSchema creates an Iceberg schema JSON for metrics data
// メトリクスデータのIceberg形式スキーマJSONを作成
// Schema:
//   - timestamp (timestamp with timezone) - メトリクスのタイムスタンプ
//   - resource_attributes (map<string, string>) - リソース属性
//   - metric_name (string) - メトリクス名
//   - metric_type (string) - メトリクスタイプ（gauge, sum, histogram, etc.）
//   - value (double) - メトリクス値
//   - attributes (map<string, string>) - メトリクス属性
func createMetricsSchema() map[string]interface{} {
	// TODO: Iceberg形式のスキーマJSONを実装
	return map[string]interface{}{}
}

// createTracesSchema creates an Iceberg schema JSON for traces data
// トレースデータのIceberg形式スキーマJSONを作成
// Schema:
//   - trace_id (binary) - トレースID
//   - span_id (binary) - スパンID
//   - parent_span_id (binary) - 親スパンID
//   - name (string) - スパン名
//   - start_time (timestamp with timezone) - 開始時刻
//   - end_time (timestamp with timezone) - 終了時刻
//   - attributes (map<string, string>) - スパン属性
//   - resource_attributes (map<string, string>) - リソース属性
func createTracesSchema() map[string]interface{} {
	// TODO: Iceberg形式のスキーマJSONを実装
	return map[string]interface{}{}
}

// createLogsSchema creates an Iceberg schema JSON for logs data
// ログデータのIceberg形式スキーマJSONを作成
// Schema:
//   - timestamp (timestamp with timezone) - ログのタイムスタンプ
//   - severity (string) - ログレベル
//   - body (string) - ログメッセージ
//   - attributes (map<string, string>) - ログ属性
//   - resource_attributes (map<string, string>) - リソース属性
func createLogsSchema() map[string]interface{} {
	// TODO: Iceberg形式のスキーマJSONを実装
	return map[string]interface{}{}
}
