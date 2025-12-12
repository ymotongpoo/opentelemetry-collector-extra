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
	"testing"

)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				Region:    "us-east-1",
				Namespace: "test-namespace",
				TableName: "test-table",
			},
			wantErr: false,
		},
		{
			name: "missing region",
			config: &Config{
				Namespace: "test-namespace",
				TableName: "test-table",
			},
			wantErr: true,
		},
		{
			name: "missing namespace",
			config: &Config{
				Region:    "us-east-1",
				TableName: "test-table",
			},
			wantErr: true,
		},
		{
			name: "missing table name",
			config: &Config{
				Region:    "us-east-1",
				Namespace: "test-namespace",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
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
	if config.TableName != "otel-data" {
		t.Errorf("expected TableName to be 'otel-data', got %s", config.TableName)
	}
}