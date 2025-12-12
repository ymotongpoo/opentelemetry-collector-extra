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
	"testing"

	"go.opentelemetry.io/collector/component"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	
	if factory.Type().String() != typeStr {
		t.Errorf("expected type %s, got %s", typeStr, factory.Type().String())
	}

	cfg := factory.CreateDefaultConfig()
	if cfg == nil {
		t.Fatal("CreateDefaultConfig() returned nil")
	}

	if err := componenttest.CheckConfigStruct(cfg); err != nil {
		t.Errorf("invalid default config: %v", err)
	}
}

func TestCreateMetricsExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))

	exporter, err := factory.CreateMetrics(context.Background(), set, cfg)
	if err != nil {
		t.Fatalf("CreateMetrics() failed: %v", err)
	}
	if exporter == nil {
		t.Fatal("CreateMetrics() returned nil exporter")
	}
}

func TestCreateTracesExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))

	exporter, err := factory.CreateTraces(context.Background(), set, cfg)
	if err != nil {
		t.Fatalf("CreateTraces() failed: %v", err)
	}
	if exporter == nil {
		t.Fatal("CreateTraces() returned nil exporter")
	}
}

func TestCreateLogsExporter(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	set := exportertest.NewNopSettings(component.MustNewType("s3tables"))

	exporter, err := factory.CreateLogs(context.Background(), set, cfg)
	if err != nil {
		t.Fatalf("CreateLogs() failed: %v", err)
	}
	if exporter == nil {
		t.Fatal("CreateLogs() returned nil exporter")
	}
}