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
	"math/rand"
	"testing"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		wantErr     bool
		expectedMsg string
	}{
		{
			name: "valid config with tables configuration",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			},
			wantErr: false,
		},
		{
			name: "missing table bucket ARN",
			config: &Config{
				TableBucketArn: "",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			},
			wantErr:     true,
			expectedMsg: "table_bucket_arn is required",
		},
		{
			name: "invalid ARN format - wrong service",
			config: &Config{
				TableBucketArn: "arn:aws:s3:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			},
			wantErr:     true,
			expectedMsg: "table_bucket_arn must follow the format arn:aws:s3tables:region:account-id:bucket/bucket-name",
		},
		{
			name: "invalid ARN format - short account ID",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:12345:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			},
			wantErr:     true,
			expectedMsg: "table_bucket_arn must follow the format arn:aws:s3tables:region:account-id:bucket/bucket-name",
		},
		{
			name: "invalid ARN format - bucket name with uppercase",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/MyBucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			},
			wantErr:     true,
			expectedMsg: "table_bucket_arn must follow the format arn:aws:s3tables:region:account-id:bucket/bucket-name",
		},
		{
			name: "missing region",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			},
			wantErr:     true,
			expectedMsg: "region is required",
		},
		{
			name: "missing namespace",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
			},
			wantErr:     true,
			expectedMsg: "namespace is required",
		},
		{
			name: "valid config with only traces",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "",
					Logs:    "",
				},
			},
			wantErr: false,
		},
		{
			name: "valid config with only metrics",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "",
					Metrics: "otel_metrics",
					Logs:    "",
				},
			},
			wantErr: false,
		},
		{
			name: "valid config with only logs",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "",
					Metrics: "",
					Logs:    "otel_logs",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid config with all tables empty",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "",
					Metrics: "",
					Logs:    "",
				},
			},
			wantErr:     true,
			expectedMsg: "at least one table name (traces, metrics, or logs) must be configured",
		},
		{
			name: "valid compression - snappy",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
				Compression: "snappy",
			},
			wantErr: false,
		},
		{
			name: "valid compression - gzip",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
				Compression: "gzip",
			},
			wantErr: false,
		},
		{
			name: "valid compression - zstd",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
				Compression: "zstd",
			},
			wantErr: false,
		},
		{
			name: "valid compression - none",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
				Compression: "none",
			},
			wantErr: false,
		},
		{
			name: "invalid compression format",
			config: &Config{
				TableBucketArn: "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
				Region:         "us-east-1",
				Namespace:      "test-namespace",
				Tables: TableNamesConfig{
					Traces:  "otel_traces",
					Metrics: "otel_metrics",
					Logs:    "otel_logs",
				},
				Compression: "invalid",
			},
			wantErr:     true,
			expectedMsg: "compression must be one of: none, snappy, gzip, zstd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err != nil && tt.expectedMsg != "" {
				if err.Error() != tt.expectedMsg {
					t.Errorf("Config.Validate() error message = %v, expected %v", err.Error(), tt.expectedMsg)
				}
			}
		})
	}
}

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	config, ok := cfg.(*Config)
	if !ok {
		t.Fatal("createDefaultConfig() did not return *Config")
	}

	if config.Region != "us-east-1" {
		t.Errorf("expected Region to be 'us-east-1', got %s", config.Region)
	}
	if config.Namespace != "default" {
		t.Errorf("expected Namespace to be 'default', got %s", config.Namespace)
	}

	// 新しい設定項目のデフォルト値を検証
	if config.Tables.Traces != "otel_traces" {
		t.Errorf("expected Tables.Traces to be 'otel_traces', got %s", config.Tables.Traces)
	}
	if config.Tables.Metrics != "otel_metrics" {
		t.Errorf("expected Tables.Metrics to be 'otel_metrics', got %s", config.Tables.Metrics)
	}
	if config.Tables.Logs != "otel_logs" {
		t.Errorf("expected Tables.Logs to be 'otel_logs', got %s", config.Tables.Logs)
	}
	if config.Compression != "snappy" {
		t.Errorf("expected Compression to be 'snappy', got %s", config.Compression)
	}
}

// generateValidARN は有効なTable Bucket ARNを生成
func generateValidARN(r *rand.Rand) string {
	// 有効なリージョンのリスト
	regions := []string{
		"us-east-1", "us-east-2", "us-west-1", "us-west-2",
		"eu-west-1", "eu-west-2", "eu-central-1",
		"ap-northeast-1", "ap-southeast-1", "ap-southeast-2",
	}
	region := regions[r.Intn(len(regions))]

	// 12桁のアカウントID
	accountID := fmt.Sprintf("%012d", r.Int63n(1000000000000))

	// バケット名（3-63文字、小文字英数字とハイフン、最初と最後は英数字）
	bucketNameLength := 3 + r.Intn(61) // 3-63文字
	bucketName := generateValidBucketName(r, bucketNameLength)

	return fmt.Sprintf("arn:aws:s3tables:%s:%s:bucket/%s", region, accountID, bucketName)
}

// generateValidBucketName は有効なバケット名を生成
func generateValidBucketName(r *rand.Rand, length int) string {
	if length < 3 {
		length = 3
	}
	if length > 63 {
		length = 63
	}

	chars := "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)

	// 最初の文字は英数字
	result[0] = chars[r.Intn(len(chars))]

	// 中間の文字は英数字またはハイフン
	charsWithHyphen := chars + "-"
	for i := 1; i < length-1; i++ {
		result[i] = charsWithHyphen[r.Intn(len(charsWithHyphen))]
	}

	// 最後の文字は英数字
	result[length-1] = chars[r.Intn(len(chars))]

	return string(result)
}

// TestProperty1_ValidARNAcceptance tests Property 1: Valid ARN acceptance
// Feature: s3tables-config-enhancement, Property 1: Valid ARN acceptance
// Validates: Requirements 1.3
func TestProperty1_ValidARNAcceptance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	// 100回のイテレーションで有効なARNを生成してテスト
	r := rand.New(rand.NewSource(rand.Int63()))
	for i := 0; i < 100; i++ {
		arn := generateValidARN(r)

		config := &Config{
			TableBucketArn: arn,
			Region:         "us-east-1",
			Namespace:      "default",
			Tables: TableNamesConfig{
				Traces:  "otel_traces",
				Metrics: "otel_metrics",
				Logs:    "otel_logs",
			},
		}

		err := config.Validate()
		if err != nil {
			t.Errorf("Property 1 failed for ARN %s: %v", arn, err)
		}
	}
}

// generateInvalidARN は無効なTable Bucket ARNを生成
func generateInvalidARN(r *rand.Rand) string {
	invalidPatterns := []func(*rand.Rand) string{
		// 間違ったプレフィックス
		func(r *rand.Rand) string {
			return fmt.Sprintf("arn:aws:s3:%s:%012d:bucket/%s",
				"us-east-1", r.Int63n(1000000000000), generateValidBucketName(r, 10))
		},
		// アカウントIDが12桁でない
		func(r *rand.Rand) string {
			return fmt.Sprintf("arn:aws:s3tables:%s:%d:bucket/%s",
				"us-east-1", r.Intn(100000), generateValidBucketName(r, 10))
		},
		// バケット名が短すぎる（2文字）
		func(r *rand.Rand) string {
			return fmt.Sprintf("arn:aws:s3tables:%s:%012d:bucket/ab",
				"us-east-1", r.Int63n(1000000000000))
		},
		// バケット名が長すぎる（65文字）
		func(r *rand.Rand) string {
			// 65文字の無効なバケット名を生成
			chars := "abcdefghijklmnopqrstuvwxyz0123456789-"
			longName := make([]byte, 65)
			for i := range longName {
				longName[i] = chars[r.Intn(len(chars))]
			}
			// 最初と最後を英数字にする
			alphanumeric := "abcdefghijklmnopqrstuvwxyz0123456789"
			longName[0] = alphanumeric[r.Intn(len(alphanumeric))]
			longName[64] = alphanumeric[r.Intn(len(alphanumeric))]
			return fmt.Sprintf("arn:aws:s3tables:%s:%012d:bucket/%s",
				"us-east-1", r.Int63n(1000000000000), string(longName))
		},
		// バケット名がハイフンで始まる
		func(r *rand.Rand) string {
			return fmt.Sprintf("arn:aws:s3tables:%s:%012d:bucket/-invalid",
				"us-east-1", r.Int63n(1000000000000))
		},
		// バケット名がハイフンで終わる
		func(r *rand.Rand) string {
			return fmt.Sprintf("arn:aws:s3tables:%s:%012d:bucket/invalid-",
				"us-east-1", r.Int63n(1000000000000))
		},
		// バケット名に大文字が含まれる
		func(r *rand.Rand) string {
			return fmt.Sprintf("arn:aws:s3tables:%s:%012d:bucket/InvalidName",
				"us-east-1", r.Int63n(1000000000000))
		},
		// リージョンに無効な文字が含まれる
		func(r *rand.Rand) string {
			return fmt.Sprintf("arn:aws:s3tables:%s:%012d:bucket/%s",
				"US_EAST_1", r.Int63n(1000000000000), generateValidBucketName(r, 10))
		},
	}

	pattern := invalidPatterns[r.Intn(len(invalidPatterns))]
	return pattern(r)
}

// TestProperty2_InvalidARNRejection tests Property 2: Invalid ARN rejection
// Feature: s3tables-config-enhancement, Property 2: Invalid ARN rejection
// Validates: Requirements 1.3, 3.2
func TestProperty2_InvalidARNRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	// 100回のイテレーションで無効なARNを生成してテスト
	r := rand.New(rand.NewSource(rand.Int63()))
	for i := 0; i < 100; i++ {
		arn := generateInvalidARN(r)

		config := &Config{
			TableBucketArn: arn,
			Region:         "us-east-1",
			Namespace:      "default",
			Tables: TableNamesConfig{
				Traces:  "otel_traces",
				Metrics: "otel_metrics",
				Logs:    "otel_logs",
			},
		}

		err := config.Validate()
		if err == nil {
			t.Errorf("Property 2 failed: expected error for invalid ARN %s, but got nil", arn)
		}
		// エラーメッセージが正しいことを確認
		expectedMsg := "table_bucket_arn must follow the format arn:aws:s3tables:region:account-id:bucket/bucket-name"
		if err != nil && err.Error() != expectedMsg {
			t.Errorf("Property 2 failed: expected error message '%s', got '%s'", expectedMsg, err.Error())
		}
	}
}

// TestProperty3_EmptyRequiredFieldRejection tests Property 3: Empty required field rejection
// Feature: s3tables-config-enhancement, Property 3: Empty required field rejection
// Validates: Requirements 1.2, 3.1, 3.3, 3.4, 3.5
func TestProperty3_EmptyRequiredFieldRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	r := rand.New(rand.NewSource(rand.Int63()))

	// 各必須フィールドが空の場合のテスト
	testCases := []struct {
		name          string
		setupConfig   func() *Config
		expectedError string
	}{
		{
			name: "empty table_bucket_arn",
			setupConfig: func() *Config {
				return &Config{
					TableBucketArn: "",
					Region:         "us-east-1",
					Namespace:      "default",
					Tables: TableNamesConfig{
						Traces:  "otel_traces",
						Metrics: "otel_metrics",
						Logs:    "otel_logs",
					},
				}
			},
			expectedError: "table_bucket_arn is required",
		},
		{
			name: "empty region",
			setupConfig: func() *Config {
				return &Config{
					TableBucketArn: generateValidARN(r),
					Region:         "",
					Namespace:      "default",
					Tables: TableNamesConfig{
						Traces:  "otel_traces",
						Metrics: "otel_metrics",
						Logs:    "otel_logs",
					},
				}
			},
			expectedError: "region is required",
		},
		{
			name: "empty namespace",
			setupConfig: func() *Config {
				return &Config{
					TableBucketArn: generateValidARN(r),
					Region:         "us-east-1",
					Namespace:      "",
					Tables: TableNamesConfig{
						Traces:  "otel_traces",
						Metrics: "otel_metrics",
						Logs:    "otel_logs",
					},
				}
			},
			expectedError: "namespace is required",
		},
		{
			name: "all tables empty",
			setupConfig: func() *Config {
				return &Config{
					TableBucketArn: generateValidARN(r),
					Region:         "us-east-1",
					Namespace:      "default",
					Tables: TableNamesConfig{
						Traces:  "",
						Metrics: "",
						Logs:    "",
					},
				}
			},
			expectedError: "at least one table name (traces, metrics, or logs) must be configured",
		},
	}

	// 各テストケースを100回実行
	for _, tc := range testCases {
		for i := 0; i < 100; i++ {
			config := tc.setupConfig()
			err := config.Validate()

			if err == nil {
				t.Errorf("Property 3 failed for %s (iteration %d): expected error, but got nil", tc.name, i)
			} else if err.Error() != tc.expectedError {
				t.Errorf("Property 3 failed for %s (iteration %d): expected error '%s', got '%s'",
					tc.name, i, tc.expectedError, err.Error())
			}
		}
	}
}

// TestProperty5_ConfigurationStorage tests Property 5: Configuration storage
// Feature: s3tables-config-enhancement, Property 5: Configuration storage
// Validates: Requirements 1.1
func TestProperty5_ConfigurationStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	// 100回のイテレーションで有効なARNを生成し、設定に保存されることを確認
	r := rand.New(rand.NewSource(rand.Int63()))
	for i := 0; i < 100; i++ {
		originalARN := generateValidARN(r)

		config := &Config{
			TableBucketArn: originalARN,
			Region:         "us-east-1",
			Namespace:      "default",
			Tables: TableNamesConfig{
				Traces:  "otel_traces",
				Metrics: "otel_metrics",
				Logs:    "otel_logs",
			},
		}

		// 設定されたARNが変更されずに保存されていることを確認
		if config.TableBucketArn != originalARN {
			t.Errorf("Property 5 failed (iteration %d): expected TableBucketArn to be '%s', got '%s'",
				i, originalARN, config.TableBucketArn)
		}
	}
}

// TestProperty6_PartialTableConfiguration tests Property 6: Partial table configuration acceptance
// Feature: s3tables-upload-implementation, Property 6: Partial table configuration acceptance
// Validates: Requirements 7.1, 7.2, 7.3, 7.4
func TestProperty6_PartialTableConfiguration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	r := rand.New(rand.NewSource(rand.Int63()))

	// 各テレメトリータイプのみが設定されている場合のテスト
	testCases := []struct {
		name   string
		tables TableNamesConfig
	}{
		{
			name: "only traces",
			tables: TableNamesConfig{
				Traces:  "otel_traces",
				Metrics: "",
				Logs:    "",
			},
		},
		{
			name: "only metrics",
			tables: TableNamesConfig{
				Traces:  "",
				Metrics: "otel_metrics",
				Logs:    "",
			},
		},
		{
			name: "only logs",
			tables: TableNamesConfig{
				Traces:  "",
				Metrics: "",
				Logs:    "otel_logs",
			},
		},
		{
			name: "traces and metrics",
			tables: TableNamesConfig{
				Traces:  "otel_traces",
				Metrics: "otel_metrics",
				Logs:    "",
			},
		},
		{
			name: "traces and logs",
			tables: TableNamesConfig{
				Traces:  "otel_traces",
				Metrics: "",
				Logs:    "otel_logs",
			},
		},
		{
			name: "metrics and logs",
			tables: TableNamesConfig{
				Traces:  "",
				Metrics: "otel_metrics",
				Logs:    "otel_logs",
			},
		},
	}

	// 各テストケースを100回実行
	for _, tc := range testCases {
		for i := 0; i < 100; i++ {
			config := &Config{
				TableBucketArn: generateValidARN(r),
				Region:         "us-east-1",
				Namespace:      "default",
				Tables:         tc.tables,
			}

			err := config.Validate()
			if err != nil {
				t.Errorf("Property 6 failed for %s (iteration %d): expected no error, but got '%s'",
					tc.name, i, err.Error())
			}
		}
	}
}

// TestProperty7_AllEmptyTableRejection tests Property 7: All empty table rejection
// Feature: s3tables-upload-implementation, Property 7: All empty table rejection
// Validates: Requirements 7.5
func TestProperty7_AllEmptyTableRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property-based test in short mode")
	}

	r := rand.New(rand.NewSource(rand.Int63()))

	// 100回のイテレーションですべてのテーブル名が空の場合にエラーが返されることを確認
	for i := 0; i < 100; i++ {
		config := &Config{
			TableBucketArn: generateValidARN(r),
			Region:         "us-east-1",
			Namespace:      "default",
			Tables: TableNamesConfig{
				Traces:  "",
				Metrics: "",
				Logs:    "",
			},
		}

		err := config.Validate()
		if err == nil {
			t.Errorf("Property 7 failed (iteration %d): expected error for all empty tables, but got nil", i)
		}

		expectedMsg := "at least one table name (traces, metrics, or logs) must be configured"
		if err != nil && err.Error() != expectedMsg {
			t.Errorf("Property 7 failed (iteration %d): expected error message '%s', got '%s'",
				i, expectedMsg, err.Error())
		}
	}
}
