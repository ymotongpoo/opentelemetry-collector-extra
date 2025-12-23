# S3 Tables Exporter - Schema Serialization Fix Proposal

## Executive Summary

The s3tablesexporter fails to create tables in AWS S3 Tables due to incorrect serialization of complex Iceberg schema types (map, list, struct). The current implementation serializes complex types as JSON strings, but the S3 Tables API expects structured objects following the Iceberg schema specification.

**Status**: Critical - Exporter is completely non-functional for all telemetry types
**Affected Versions**: main branch (commit 1331372 and earlier)
**Impact**: 100% of telemetry data (traces, metrics, logs) is rejected

---

## Problem Description

### Current Behavior

When creating tables with complex field types (map, list, struct), the exporter serializes the type definition as an escaped JSON string:

```json
{
  "Name": "attributes",
  "Type": "{\"key\":\"string\",\"key-id\":8,\"type\":\"map\",\"value\":\"string\",\"value-id\":9,\"value-required\":false}",
  "Required": false
}
```

The S3 Tables API rejects this with:

```
StatusCode: 400
BadRequestException: The specified metadata is not valid
```

### Expected Behavior

According to the Apache Iceberg specification, complex types should be structured objects:

```json
{
  "Name": "attributes",
  "Type": {
    "type": "map",
    "key-id": 8,
    "key": "string",
    "value-id": 9,
    "value": "string",
    "value-required": false
  },
  "Required": false
}
```

---

## Root Cause Analysis

### Location: `exporter.go` lines 410-432

```go
awsSchemaFields := make([]types.SchemaField, 0, len(customFields))
for _, customField := range customFields {
    // Typeをシリアライズ
    var typeBytes []byte
    if typeStr, ok := customField.Type.(string); ok {
        // プリミティブ型の場合、そのまま文字列として使用
        typeBytes = []byte(typeStr)
    } else {
        // 複雑型の場合、JSON形式にシリアライズ
        typeBytes, err = json.Marshal(customField.Type)  // ← PROBLEM: Creates escaped JSON string
        if err != nil {
            e.logger.Error("Failed to marshal field type",
                "namespace", namespace,
                "table", tableName,
                "field", customField.Name,
                "error", err)
            return nil, fmt.Errorf("failed to marshal field %s type: %w", customField.Name, err)
        }
    }
    typeStr := string(typeBytes)  // ← PROBLEM: Converts to string

    awsField := types.SchemaField{
        Name:     &customField.Name,
        Type:     &typeStr,  // ← PROBLEM: AWS SDK expects string, but API expects structured object
        Required: customField.Required,
    }
    awsSchemaFields = append(awsSchemaFields, awsField)
}
```

### The Core Issue

The AWS SDK's `types.SchemaField` struct defines `Type` as `*string`:

```go
type SchemaField struct {
    Name     *string
    Type     *string  // ← This is the constraint
    Required bool
}
```

However, the S3 Tables API expects the Iceberg schema format where complex types are structured JSON objects, not strings.

---

## Error Logs

### Full Error Output

```
time=2025-12-22T15:31:11.492+09:00 level=INFO msg="Starting upload to S3 Tables"
table_bucket_arn=arn:aws:s3tables:ap-northeast-1:863518440108:bucket/s3tables-exporter-e2e-test-1766384778
namespace=e2e_test_namespace_20251222
table=otel_metrics
data_type=metrics
size=2345

time=2025-12-22T15:31:12.256+09:00 level=ERROR msg="Failed to create table"
namespace=e2e_test_namespace_20251222
table=otel_metrics
schema_json="{
  \"fields\": [
    {
      \"id\": 1,
      \"name\": \"timestamp\",
      \"required\": true,
      \"type\": \"timestamptz\"
    },
    {
      \"id\": 2,
      \"name\": \"resource_attributes\",
      \"required\": false,
      \"type\": {
        \"key\": \"string\",
        \"key-id\": 3,
        \"type\": \"map\",
        \"value\": \"string\",
        \"value-id\": 4,
        \"value-required\": false
      }
    },
    ...
  ],
  \"type\": \"struct\"
}"
aws_schema_json="[
  {
    \"Name\": \"timestamp\",
    \"Type\": \"timestamptz\",
    \"Required\": true
  },
  {
    \"Name\": \"resource_attributes\",
    \"Type\": \"{\\\"key\\\":\\\"string\\\",\\\"key-id\\\":3,\\\"type\\\":\\\"map\\\",\\\"value\\\":\\\"string\\\",\\\"value-id\\\":4,\\\"value-required\\\":false}\",
    \"Required\": false
  },
  ...
]"
error="operation error S3Tables: CreateTable, https response error StatusCode: 400, RequestID: d6672b47-c496-4141-9206-f0595b957ee3, BadRequestException: The specified metadata is not valid"

time=2025-12-22T15:31:12.257+09:00 level=ERROR msg="Failed to get or create table"
namespace=e2e_test_namespace_20251222
table=otel_metrics
error="failed to create table: failed to create table e2e_test_namespace_20251222.otel_metrics: operation error S3Tables: CreateTable, https response error StatusCode: 400, RequestID: d6672b47-c496-4141-9206-f0595b957ee3, BadRequestException: The specified metadata is not valid"

2025-12-22T15:31:12.257+0900    error   internal/base_exporter.go:114   Exporting failed. Rejecting data.
{"otelcol.component.id": "s3tables",
"otelcol.component.kind": "exporter",
"otelcol.signal": "metrics",
"error": "failed to get or create table: failed to create table: failed to create table e2e_test_namespace_20251222.otel_metrics: operation error S3Tables: CreateTable, https response error StatusCode: 400, RequestID: d6672b47-c496-4141-9206-f0595b957ee3, BadRequestException: The specified metadata is not valid",
"rejected_items": 8}
```

### Key Observations

1. **Correct Iceberg Schema** (`schema_json`): Shows proper structured objects for map types
2. **Incorrect AWS Schema** (`aws_schema_json`): Shows escaped JSON strings for map types
3. **API Rejection**: S3 Tables API returns 400 Bad Request

---

## Proposed Solutions

### Solution 1: Custom JSON Marshaler (Recommended)

Implement a custom JSON marshaler for the schema that bypasses AWS SDK's type constraints.

#### Implementation Approach

1. Create a custom wrapper type that implements `json.Marshaler`
2. Override the JSON marshaling behavior to output structured objects
3. Use this wrapper when creating the `CreateTableInput`

#### Code Changes

**File: `exporter.go`**

Add a new type that wraps `types.IcebergMetadata`:

```go
// CustomIcebergMetadata wraps types.IcebergMetadata with custom JSON marshaling
type CustomIcebergMetadata struct {
 Schema *CustomIcebergSchema `json:"schema"`
}

// CustomIcebergSchema wraps types.IcebergSchema with custom JSON marshaling
type CustomIcebergSchema struct {
 Fields []CustomSchemaFieldForAPI `json:"fields"`
}

// CustomSchemaFieldForAPI represents a schema field with proper type handling
type CustomSchemaFieldForAPI struct {
 Name     string      `json:"name"`
 Type     interface{} `json:"type"` // Can be string or structured object
 Required bool        `json:"required"`
}

// MarshalJSON implements custom JSON marshaling for CustomIcebergMetadata
func (m *CustomIcebergMetadata) MarshalJSON() ([]byte, error) {
 return json.Marshal(struct {
  Schema *CustomIcebergSchema `json:"schema"`
 }{
  Schema: m.Schema,
 })
}
```

Modify the `createTable` function (lines 410-461):

```go
// Build custom schema fields with proper type handling
customSchemaFieldsForAPI := make([]CustomSchemaFieldForAPI, 0, len(customFields))
for _, customField := range customFields {
 apiField := CustomSchemaFieldForAPI{
  Name:     customField.Name,
  Type:     customField.Type, // Keep as interface{} - will be marshaled correctly
  Required: customField.Required,
 }
 customSchemaFieldsForAPI = append(customSchemaFieldsForAPI, apiField)
}

// Create custom metadata
customMetadata := &CustomIcebergMetadata{
 Schema: &CustomIcebergSchema{
  Fields: customSchemaFieldsForAPI,
 },
}

// Marshal to JSON
metadataJSON, err := json.Marshal(customMetadata)
if err != nil {
 return nil, fmt.Errorf("failed to marshal custom metadata: %w", err)
}

// Create table input with raw JSON
// Note: This requires using AWS SDK's lower-level API or custom HTTP client
createInput := &s3tables.CreateTableInput{
 TableBucketARN: &e.config.TableBucketArn,
 Namespace:      &namespace,
 Name:           &tableName,
 Format:         types.OpenTableFormatIceberg,
 // Use custom metadata that will be properly marshaled
}
```

**Challenge**: AWS SDK's `CreateTableInput.Metadata` expects `types.TableMetadata` interface, which is implemented by `types.TableMetadataMemberIceberg`. We need to either:

- Use AWS SDK's lower-level API with custom serialization
- Submit a patch to AWS SDK to support structured types
- Use direct HTTP API calls

### Solution 2: Direct HTTP API (Alternative)

Bypass AWS SDK entirely and make direct HTTP requests to S3 Tables API.

#### Implementation Approach

1. Create a custom HTTP client for S3 Tables API
2. Use AWS SigV4 signing for authentication
3. Construct the request body with proper Iceberg schema format
4. Handle responses manually

#### Code Changes

**File: `exporter.go`**

Add a new function:

```go
// createTableDirectAPI creates a table using direct HTTP API calls
func (e *s3TablesExporter) createTableDirectAPI(ctx context.Context, namespace, tableName string, schema map[string]interface{}) (*TableInfo, error) {
 // Convert to Iceberg schema
 icebergSchema, err := convertToIcebergSchema(schema)
 if err != nil {
  return nil, fmt.Errorf("failed to convert schema: %w", err)
 }

 // Serialize schema
 serializer := NewIcebergSchemaSerializer(icebergSchema)
 schemaData, err := serializer.SerializeForS3Tables()
 if err != nil {
  return nil, fmt.Errorf("failed to serialize schema: %w", err)
 }

 // Build request body
 requestBody := map[string]interface{}{
  "tableBucketARN": e.config.TableBucketArn,
  "namespace":      namespace,
  "name":           tableName,
  "format":         "ICEBERG",
  "metadata": map[string]interface{}{
   "iceberg": map[string]interface{}{
    "schema": schemaData,
   },
  },
 }

 bodyJSON, err := json.Marshal(requestBody)
 if err != nil {
  return nil, fmt.Errorf("failed to marshal request body: %w", err)
 }

 // Create HTTP request
 endpoint := fmt.Sprintf("https://s3tables.%s.amazonaws.com/tables", e.config.Region)
 req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(bodyJSON))
 if err != nil {
  return nil, fmt.Errorf("failed to create request: %w", err)
 }

 req.Header.Set("Content-Type", "application/json")

 // Sign request with AWS SigV4
 credentials, err := e.awsConfig.Credentials.Retrieve(ctx)
 if err != nil {
  return nil, fmt.Errorf("failed to retrieve credentials: %w", err)
 }

 signer := v4.NewSigner()
 err = signer.SignHTTP(ctx, credentials, req, "sha256", "s3tables", e.config.Region, time.Now())
 if err != nil {
  return nil, fmt.Errorf("failed to sign request: %w", err)
 }

 // Send request
 client := &http.Client{Timeout: 30 * time.Second}
 resp, err := client.Do(req)
 if err != nil {
  return nil, fmt.Errorf("failed to send request: %w", err)
 }
 defer resp.Body.Close()

 // Handle response
 if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
  body, _ := io.ReadAll(resp.Body)
  return nil, fmt.Errorf("API error: status=%d, body=%s", resp.StatusCode, string(body))
 }

 // Parse response and return TableInfo
 // ... (implementation details)
}
```

### Solution 3: AWS SDK Patch (Long-term)

Submit a patch to AWS SDK Go v2 to support structured types in `types.SchemaField.Type`.

#### Proposed AWS SDK Change

Change `types.SchemaField` definition:

```go
// Current
type SchemaField struct {
    Name     *string
    Type     *string  // ← Change this
    Required bool
}

// Proposed
type SchemaField struct {
    Name     *string
    Type     interface{} // ← Support both string and structured types
    Required bool
}
```

Add custom JSON marshaling:

```go
func (s SchemaField) MarshalJSON() ([]byte, error) {
    type Alias SchemaField
    return json.Marshal(&struct {
        Type interface{} `json:"type"`
        *Alias
    }{
        Type:  s.Type,
        Alias: (*Alias)(&s),
    })
}
```

---

## Recommended Action Plan

### Immediate (This Week)

1. **Implement Solution 2 (Direct HTTP API)** as a temporary workaround
   - Fastest path to working exporter
   - No dependency on AWS SDK changes
   - Can be implemented entirely within s3tablesexporter

2. **Document the workaround** in README with clear explanation

### Short-term (Next 2-4 Weeks)

1. **Submit issue to AWS SDK Go v2** repository
   - Reference this analysis
   - Propose Solution 3 changes
   - Provide test cases

2. **Implement Solution 1 (Custom Marshaler)** as intermediate solution
   - Cleaner than direct HTTP API
   - Easier to maintain
   - Can be replaced when AWS SDK is fixed

### Long-term (1-3 Months)

1. **Wait for AWS SDK fix** (if accepted)
2. **Migrate to official AWS SDK solution**
3. **Remove workarounds**

---

## Testing Recommendations

### Unit Tests

Add tests for schema serialization:

```go
func TestSchemaSerializationWithComplexTypes(t *testing.T) {
    schema := map[string]interface{}{
        "attributes": map[string]interface{}{
            "type": "map",
            "key": "string",
            "value": "string",
        },
    }

    // Test that complex types are NOT escaped as strings
    result := serializeSchema(schema)

    // Verify structure
    assert.NotContains(t, result, "\\\"") // No escaped quotes
    assert.Contains(t, result, `"type":"map"`) // Proper JSON structure
}
```

### Integration Tests

Test against actual S3 Tables API:

```go
func TestCreateTableWithMapFields(t *testing.T) {
    // Create table with map-type fields
    // Verify table creation succeeds
    // Verify schema is correctly stored
}
```

### E2E Tests

Full telemetry pipeline test:

```bash
# 1. Start collector with s3tablesexporter
# 2. Send traces/metrics/logs with attributes
# 3. Verify data in S3 Tables
# 4. Query data and verify schema
```

---

## References

- **Apache Iceberg Schema Spec**: <https://iceberg.apache.org/spec/#schemas>
- **AWS S3 Tables API**: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_CreateTable.html>
- **AWS SDK Go v2**: <https://github.com/aws/aws-sdk-go-v2>
- **Current Issue**: docs/s3tablesexporter-github-issue.md

---

## Appendix: Schema Comparison

### Iceberg Schema (Correct)

```json
{
  "type": "struct",
  "fields": [
    {
      "id": 1,
      "name": "timestamp",
      "required": true,
      "type": "timestamptz"
    },
    {
      "id": 2,
      "name": "resource_attributes",
      "required": false,
      "type": {
        "type": "map",
        "key-id": 3,
        "key": "string",
        "value-id": 4,
        "value": "string",
        "value-required": false
      }
    }
  ]
}
```

### Current AWS SDK Output (Incorrect)

```json
[
  {
    "Name": "timestamp",
    "Type": "timestamptz",
    "Required": true
  },
  {
    "Name": "resource_attributes",
    "Type": "{\"key\":\"string\",\"key-id\":3,\"type\":\"map\",\"value\":\"string\",\"value-id\":4,\"value-required\":false}",
    "Required": false
  }
]
```

### Expected AWS SDK Output (Correct)

```json
[
  {
    "Name": "timestamp",
    "Type": "timestamptz",
    "Required": true
  },
  {
    "Name": "resource_attributes",
    "Type": {
      "type": "map",
      "key-id": 3,
      "key": "string",
      "value-id": 4,
      "value": "string",
      "value-required": false
    },
    "Required": false
  }
]
```

---

**Report Generated**: 2025-12-22
**Environment**: OpenTelemetry Collector v0.141.0, s3tablesexporter main branch
**Tested Region**: ap-northeast-1
