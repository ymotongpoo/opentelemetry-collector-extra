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
	"fmt"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

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
	ID       int         `json:"id"`
	Name     string      `json:"name"`
	Required bool        `json:"required"`
	Type     interface{} `json:"type"`
}

// IsPrimitiveType checks if the type is a primitive type (string)
// 型がプリミティブ型（文字列）かどうかをチェック
func (f *IcebergSchemaField) IsPrimitiveType() bool {
	_, ok := f.Type.(string)
	return ok
}

// IsMapType checks if the type is a map type
// 型がmap型かどうかをチェック
func (f *IcebergSchemaField) IsMapType() bool {
	if m, ok := f.Type.(map[string]interface{}); ok {
		typeVal, hasType := m["type"]
		return hasType && typeVal == "map"
	}
	return false
}

// GetPrimitiveType returns the primitive type string
// プリミティブ型の文字列を返す
func (f *IcebergSchemaField) GetPrimitiveType() (string, error) {
	if s, ok := f.Type.(string); ok {
		return s, nil
	}
	return "", fmt.Errorf("type is not a primitive type")
}

// GetMapType returns the map type structure
// map型の構造を返す
func (f *IcebergSchemaField) GetMapType() (map[string]interface{}, error) {
	if m, ok := f.Type.(map[string]interface{}); ok {
		return m, nil
	}
	return nil, fmt.Errorf("type is not a map type")
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

// createInitialIcebergMetadata creates an initial Iceberg metadata for a new table
// 新しいテーブル用の初期Icebergメタデータを作成
func createInitialIcebergMetadata(schema *IcebergSchema) *IcebergMetadata {
	return &IcebergMetadata{
		FormatVersion:      2,
		TableUUID:          uuid.New().String(),
		Location:           "", // S3 Tablesが自動的に設定
		LastSequenceNumber: 0,
		LastUpdatedMS:      0, // S3 Tablesが自動的に設定
		LastColumnID:       getLastColumnID(schema),
		Schemas:            []IcebergSchema{*schema},
		CurrentSchemaID:    0,
		PartitionSpecs:     []IcebergPartitionSpec{{SpecID: 0, Fields: []IcebergPartitionField{}}},
		DefaultSpecID:      0,
		LastPartitionID:    0,
		Properties:         map[string]string{},
		CurrentSnapshotID:  -1,
		Snapshots:          []IcebergSnapshot{},
		SnapshotLog:        []IcebergSnapshotLog{},
		MetadataLog:        []IcebergMetadataLog{},
	}
}

// getLastColumnID returns the last column ID from the schema
// スキーマから最後のカラムIDを取得
func getLastColumnID(schema *IcebergSchema) int {
	maxID := 0
	for _, field := range schema.Fields {
		if field.ID > maxID {
			maxID = field.ID
		}
		// map型の場合、key-idとvalue-idも考慮
		if field.IsMapType() {
			mapType, _ := field.GetMapType()
			if keyID, ok := mapType["key-id"].(int); ok && keyID > maxID {
				maxID = keyID
			}
			if keyID, ok := mapType["key-id"].(float64); ok && int(keyID) > maxID {
				maxID = int(keyID)
			}
			if valueID, ok := mapType["value-id"].(int); ok && valueID > maxID {
				maxID = valueID
			}
			if valueID, ok := mapType["value-id"].(float64); ok && int(valueID) > maxID {
				maxID = int(valueID)
			}
		}
	}
	return maxID
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

// extractBucketAndPrefixFromWarehouseLocation extracts the bucket name and prefix from a warehouse location
// Warehouse locationからバケット名とプレフィックスを抽出
// 形式: s3://bucket-name/prefix
func extractBucketAndPrefixFromWarehouseLocation(warehouseLocation string) (bucket, prefix string, err error) {
	// s3://形式の検証
	s3Pattern := regexp.MustCompile(`^s3://([^/]+)(?:/(.*))?$`)
	matches := s3Pattern.FindStringSubmatch(warehouseLocation)
	if len(matches) < 2 {
		return "", "", fmt.Errorf("invalid warehouse location format: %s (expected s3://bucket-name or s3://bucket-name/prefix)", warehouseLocation)
	}
	bucket = matches[1]
	if len(matches) > 2 {
		prefix = matches[2]
	}
	return bucket, prefix, nil
}

// generateMetadataFileName generates a metadata file name in Iceberg format
// Iceberg形式のメタデータファイル名を生成
// 形式: {version}-{uuid}.metadata.json
func generateMetadataFileName(version int) string {
	// UUIDを生成
	id := uuid.New().String()
	// ファイル名を生成
	return fmt.Sprintf("%05d-%s.metadata.json", version, id)
}

// generateManifestFileName generates a manifest file name in Iceberg format
// Iceberg形式のマニフェストファイル名を生成
// 形式: {snapshot-id}-{uuid}.manifest.json
func generateManifestFileName(snapshotID int64) string {
	// UUIDを生成
	id := uuid.New().String()
	// ファイル名を生成
	return fmt.Sprintf("%d-%s.manifest.json", snapshotID, id)
}

// uploadMetadata uploads metadata and manifest files to warehouse location
// メタデータファイルとマニフェストファイルをwarehouse locationにアップロード
//
// この関数は以下の処理を行います：
// 1. メタデータファイルをJSON形式でシリアライズ
// 2. warehouse location内のmetadataディレクトリにアップロード
// 3. ファイル名は{version}-{uuid}.metadata.json形式
// 4. マニフェストファイルも同様にアップロード
// 5. S3 PutObject APIを使用
func uploadMetadata(
	ctx context.Context,
	s3Client s3ClientInterface,
	warehouseLocation string,
	metadata *IcebergMetadata,
	manifest *IcebergManifest,
	version int,
) (metadataPath string, manifestPath string, err error) {
	// コンテキストキャンセルのチェック
	select {
	case <-ctx.Done():
		return "", "", fmt.Errorf("metadata upload cancelled: %w", ctx.Err())
	default:
	}

	// Warehouse locationからバケット名とプレフィックスを抽出
	bucket, prefix, err := extractBucketAndPrefixFromWarehouseLocation(warehouseLocation)
	if err != nil {
		return "", "", fmt.Errorf("failed to extract bucket and prefix from warehouse location: %w", err)
	}

	// メタデータファイル名を生成
	metadataFileName := generateMetadataFileName(version)
	var metadataKey string
	if prefix != "" {
		metadataKey = fmt.Sprintf("%s/metadata/%s", prefix, metadataFileName)
	} else {
		metadataKey = fmt.Sprintf("metadata/%s", metadataFileName)
	}

	// メタデータをJSON形式でシリアライズ
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal metadata to JSON: %w", err)
	}

	// メタデータファイルをS3にアップロード
	putMetadataInput := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(metadataKey),
		Body:   bytes.NewReader(metadataJSON),
	}

	_, err = s3Client.PutObject(ctx, putMetadataInput)
	if err != nil {
		return "", "", fmt.Errorf("failed to upload metadata file to S3: %w", err)
	}

	// メタデータファイルのS3パスを生成
	metadataPath = fmt.Sprintf("s3://%s/%s", bucket, metadataKey)

	// マニフェストファイル名を生成
	manifestFileName := generateManifestFileName(metadata.CurrentSnapshotID)
	var manifestKey string
	if prefix != "" {
		manifestKey = fmt.Sprintf("%s/metadata/%s", prefix, manifestFileName)
	} else {
		manifestKey = fmt.Sprintf("metadata/%s", manifestFileName)
	}

	// マニフェストをJSON形式でシリアライズ
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal manifest to JSON: %w", err)
	}

	// マニフェストファイルをS3にアップロード
	putManifestInput := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(manifestKey),
		Body:   bytes.NewReader(manifestJSON),
	}

	_, err = s3Client.PutObject(ctx, putManifestInput)
	if err != nil {
		return "", "", fmt.Errorf("failed to upload manifest file to S3: %w", err)
	}

	// マニフェストファイルのS3パスを生成
	manifestPath = fmt.Sprintf("s3://%s/%s", bucket, manifestKey)

	return metadataPath, manifestPath, nil
}


// GetTypeSerializer はフィールドの型に応じて適切なシリアライザーを返す
func (f *IcebergSchemaField) GetTypeSerializer() (IcebergTypeSerializer, error) {
	if f.IsPrimitiveType() {
		// プリミティブ型の場合
		primitiveType, _ := f.GetPrimitiveType()
		return NewPrimitiveTypeSerializer(primitiveType), nil
	}

	if f.IsMapType() {
		// map型の場合
		mapType, _ := f.GetMapType()

		// key-idを取得
		keyID, ok := mapType["key-id"].(int)
		if !ok {
			if keyIDFloat, ok := mapType["key-id"].(float64); ok {
				keyID = int(keyIDFloat)
			} else {
				return nil, fmt.Errorf("invalid key-id type: %T", mapType["key-id"])
			}
		}

		// keyを取得
		key := mapType["key"]

		// value-idを取得
		valueID, ok := mapType["value-id"].(int)
		if !ok {
			if valueIDFloat, ok := mapType["value-id"].(float64); ok {
				valueID = int(valueIDFloat)
			} else {
				return nil, fmt.Errorf("invalid value-id type: %T", mapType["value-id"])
			}
		}

		// valueを取得
		value := mapType["value"]

		// value-requiredを取得
		valueRequired, ok := mapType["value-required"].(bool)
		if !ok {
			valueRequired = false
		}

		return NewMapTypeSerializer(keyID, key, valueID, value, valueRequired), nil
	}

	if f.IsListType() {
		// list型の場合
		listType, _ := f.GetListType()

		// element-idを取得
		elementID, ok := listType["element-id"].(int)
		if !ok {
			if elementIDFloat, ok := listType["element-id"].(float64); ok {
				elementID = int(elementIDFloat)
			} else {
				return nil, fmt.Errorf("invalid element-id type: %T", listType["element-id"])
			}
		}

		// elementを取得
		element := listType["element"]

		// element-requiredを取得
		elementRequired, ok := listType["element-required"].(bool)
		if !ok {
			elementRequired = false
		}

		return NewListTypeSerializer(elementID, element, elementRequired), nil
	}

	if f.IsStructType() {
		// struct型の場合
		structType, _ := f.GetStructType()

		// fieldsを取得
		fieldsInterface, ok := structType["fields"]
		if !ok {
			return nil, fmt.Errorf("struct type missing 'fields' key")
		}

		// fieldsを[]IcebergSchemaFieldに変換
		fieldsSlice, ok := fieldsInterface.([]map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("struct 'fields' is not a slice of maps")
		}

		fields := make([]IcebergSchemaField, 0, len(fieldsSlice))
		for _, fieldMap := range fieldsSlice {
			field, err := convertToSchemaField(fieldMap)
			if err != nil {
				return nil, fmt.Errorf("failed to convert struct field: %w", err)
			}
			fields = append(fields, field)
		}

		return NewStructTypeSerializer(fields), nil
	}

	return nil, fmt.Errorf("unsupported type: %T", f.Type)
}

// IsListType はフィールドの型がlist型かどうかをチェックする
func (f *IcebergSchemaField) IsListType() bool {
	if m, ok := f.Type.(map[string]interface{}); ok {
		typeVal, hasType := m["type"]
		return hasType && typeVal == "list"
	}
	return false
}

// IsStructType はフィールドの型がstruct型かどうかをチェックする
func (f *IcebergSchemaField) IsStructType() bool {
	if m, ok := f.Type.(map[string]interface{}); ok {
		typeVal, hasType := m["type"]
		return hasType && typeVal == "struct"
	}
	return false
}

// GetListType はlist型の構造を返す
func (f *IcebergSchemaField) GetListType() (map[string]interface{}, error) {
	if m, ok := f.Type.(map[string]interface{}); ok {
		return m, nil
	}
	return nil, fmt.Errorf("type is not a list type")
}

// GetStructType はstruct型の構造を返す
func (f *IcebergSchemaField) GetStructType() (map[string]interface{}, error) {
	if m, ok := f.Type.(map[string]interface{}); ok {
		return m, nil
	}
	return nil, fmt.Errorf("type is not a struct type")
}
