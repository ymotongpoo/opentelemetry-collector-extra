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
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestConvertMetricsToParquet(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("test-metric")
	metric.SetUnit("bytes")

	gauge := metric.SetEmptyGauge()
	dp := gauge.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetDoubleValue(42.0)
	dp.Attributes().PutStr("key", "value")

	data, err := convertMetricsToParquet(md)
	if err != nil {
		t.Fatalf("convertMetricsToParquet() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("convertMetricsToParquet() returned empty data")
	}
}

func TestConvertTracesToParquet(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-service")

	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("test-span")
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(time.Second)))
	span.Attributes().PutStr("key", "value")

	data, err := convertTracesToParquet(td)
	if err != nil {
		t.Fatalf("convertTracesToParquet() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("convertTracesToParquet() returned empty data")
	}
}

func TestConvertLogsToParquet(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")

	sl := rl.ScopeLogs().AppendEmpty()
	log := sl.LogRecords().AppendEmpty()
	log.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	log.SetSeverityText("INFO")
	log.Body().SetStr("test log message")
	log.Attributes().PutStr("key", "value")

	data, err := convertLogsToParquet(ld)
	if err != nil {
		t.Fatalf("convertLogsToParquet() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("convertLogsToParquet() returned empty data")
	}
}

func TestWriteParquet(t *testing.T) {
	records := []MetricRecord{
		{
			Timestamp:  time.Now().UnixNano(),
			Name:       "test-metric",
			Type:       "gauge",
			Value:      42.0,
			Unit:       "bytes",
			Attributes: "value",
			ResourceID: "test-service",
		},
	}

	data, err := writeParquet(records)
	if err != nil {
		t.Fatalf("writeParquet() failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("writeParquet() returned empty data")
	}
}

func TestWriteParquetEmpty(t *testing.T) {
	var records []MetricRecord

	data, err := writeParquet(records)
	if err != nil {
		t.Fatalf("writeParquet() with empty records failed: %v", err)
	}
	if len(data) != 0 {
		t.Error("writeParquet() should return empty data for empty records")
	}
}
