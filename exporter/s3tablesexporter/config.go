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

	"go.opentelemetry.io/collector/component"
)

// Config defines the configuration for the S3 Tables exporter.
type Config struct {
	TableBucketArn string `mapstructure:"table_bucket_arn"`
	Region         string `mapstructure:"region"`
	Namespace      string `mapstructure:"namespace"`
	TableName      string `mapstructure:"table_name"`
}

// Validate checks that the configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Region == "" {
		return fmt.Errorf("region is required")
	}
	if cfg.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if cfg.TableName == "" {
		return fmt.Errorf("table_name is required")
	}
	return nil
}

// createDefaultConfig creates the default configuration for the S3 Tables exporter.
func createDefaultConfig() component.Config {
	return &Config{
		TableBucketArn: "",
		Region:         "us-east-1",
		Namespace:      "default",
		TableName:      "otel-data",
	}
}