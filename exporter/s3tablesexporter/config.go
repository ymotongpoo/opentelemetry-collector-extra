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

// Package s3tablesexporter provides an OpenTelemetry Collector exporter
// that converts telemetry data to Parquet format and stores it in Amazon S3 Tables.
package s3tablesexporter

import (
	"fmt"
	"regexp"

	"go.opentelemetry.io/collector/component"
)

// TableNamesConfig defines the table names for each telemetry data type.
type TableNamesConfig struct {
	Traces  string `mapstructure:"traces"`
	Metrics string `mapstructure:"metrics"`
	Logs    string `mapstructure:"logs"`
}

// Config defines the configuration for the S3 Tables exporter.
type Config struct {
	TableBucketArn string           `mapstructure:"table_bucket_arn"`
	Region         string           `mapstructure:"region"`
	Namespace      string           `mapstructure:"namespace"`
	Tables         TableNamesConfig `mapstructure:"tables"`
	Compression    string           `mapstructure:"compression"`
}

// Validate checks that the configuration is valid.
func (cfg *Config) Validate() error {
	// TableBucketArnの検証
	if cfg.TableBucketArn == "" {
		return fmt.Errorf("table_bucket_arn is required")
	}

	// ARN形式の検証
	// 形式: arn:aws:s3tables:region:account-id:bucket/bucket-name
	arnPattern := `^arn:aws:s3tables:[a-z0-9-]+:\d{12}:bucket/[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`
	matched, err := regexp.MatchString(arnPattern, cfg.TableBucketArn)
	if err != nil {
		return fmt.Errorf("failed to validate table_bucket_arn: %w", err)
	}
	if !matched {
		return fmt.Errorf("table_bucket_arn must follow the format arn:aws:s3tables:region:account-id:bucket/bucket-name")
	}

	if cfg.Region == "" {
		return fmt.Errorf("region is required")
	}
	if cfg.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	// TableNamesConfigの検証
	if cfg.Tables.Traces == "" {
		return fmt.Errorf("tables.traces is required")
	}
	if cfg.Tables.Metrics == "" {
		return fmt.Errorf("tables.metrics is required")
	}
	if cfg.Tables.Logs == "" {
		return fmt.Errorf("tables.logs is required")
	}

	// Compression形式の検証
	if cfg.Compression != "" {
		validCompressions := map[string]bool{
			"none":   true,
			"snappy": true,
			"gzip":   true,
			"zstd":   true,
		}
		if !validCompressions[cfg.Compression] {
			return fmt.Errorf("compression must be one of: none, snappy, gzip, zstd")
		}
	}

	return nil
}

// createDefaultConfig creates the default configuration for the S3 Tables exporter.
func createDefaultConfig() component.Config {
	return &Config{
		TableBucketArn: "",
		Region:         "us-east-1",
		Namespace:      "default",
		Tables: TableNamesConfig{
			Traces:  "otel_traces",
			Metrics: "otel_metrics",
			Logs:    "otel_logs",
		},
		Compression: "snappy",
	}
}