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

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// MetricRecord represents a metric data point in Parquet format.
type MetricRecord struct {
	Timestamp   int64             `parquet:"timestamp"`
	Name        string            `parquet:"name"`
	Type        string            `parquet:"type"`
	Value       float64           `parquet:"value"`
	Unit        string            `parquet:"unit"`
	Attributes  map[string]any `parquet:"attributes"`
	ResourceID  string            `parquet:"resource_id"`
}

// TraceRecord represents a trace span in Parquet format.
type TraceRecord struct {
	TraceID     string            `parquet:"trace_id"`
	SpanID      string            `parquet:"span_id"`
	ParentID    string            `parquet:"parent_id"`
	Name        string            `parquet:"name"`
	StartTime   int64             `parquet:"start_time"`
	EndTime     int64             `parquet:"end_time"`
	Duration    int64             `parquet:"duration"`
	Status      string            `parquet:"status"`
	Attributes  map[string]any `parquet:"attributes"`
	ResourceID  string            `parquet:"resource_id"`
}

// LogRecord represents a log entry in Parquet format.
type LogRecord struct {
	Timestamp   int64             `parquet:"timestamp"`
	Severity    string            `parquet:"severity"`
	Body        string            `parquet:"body"`
	TraceID     string            `parquet:"trace_id"`
	SpanID      string            `parquet:"span_id"`
	Attributes  map[string]any `parquet:"attributes"`
	ResourceID  string            `parquet:"resource_id"`
}

// convertMetricsToParquet converts OpenTelemetry metrics to Parquet format.
func convertMetricsToParquet(md pmetric.Metrics) ([]byte, error) {
	var records []MetricRecord
	
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resourceID := "unknown"
		if svcName, ok := rm.Resource().Attributes().AsRaw()["service.name"]; ok {
			resourceID = svcName.(string)
		}
		
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			
			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)
				
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					for l := 0; l < metric.Gauge().DataPoints().Len(); l++ {
						dp := metric.Gauge().DataPoints().At(l)
						records = append(records, MetricRecord{
							Timestamp:  dp.Timestamp().AsTime().UnixNano(),
							Name:       metric.Name(),
							Type:       "gauge",
							Value:      dp.DoubleValue(),
							Unit:       metric.Unit(),
							Attributes: dp.Attributes().AsRaw(),
							ResourceID: resourceID,
						})
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < metric.Sum().DataPoints().Len(); l++ {
						dp := metric.Sum().DataPoints().At(l)
						records = append(records, MetricRecord{
							Timestamp:  dp.Timestamp().AsTime().UnixNano(),
							Name:       metric.Name(),
							Type:       "sum",
							Value:      dp.DoubleValue(),
							Unit:       metric.Unit(),
							Attributes: dp.Attributes().AsRaw(),
							ResourceID: resourceID,
						})
					}
				}
			}
		}
	}
	
	return writeParquet(records)
}

// convertTracesToParquet converts OpenTelemetry traces to Parquet format.
func convertTracesToParquet(td ptrace.Traces) ([]byte, error) {
	var records []TraceRecord
	
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resourceID := "unknown"
		if svcName, ok := rs.Resource().Attributes().AsRaw()["service.name"]; ok {
			resourceID = svcName.(string)
		}
		
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				
				records = append(records, TraceRecord{
					TraceID:    span.TraceID().String(),
					SpanID:     span.SpanID().String(),
					ParentID:   span.ParentSpanID().String(),
					Name:       span.Name(),
					StartTime:  span.StartTimestamp().AsTime().UnixNano(),
					EndTime:    span.EndTimestamp().AsTime().UnixNano(),
					Duration:   span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Nanoseconds(),
					Status:     span.Status().Code().String(),
					Attributes: span.Attributes().AsRaw(),
					ResourceID: resourceID,
				})
			}
		}
	}
	
	return writeParquet(records)
}

// convertLogsToParquet converts OpenTelemetry logs to Parquet format.
func convertLogsToParquet(ld plog.Logs) ([]byte, error) {
	var records []LogRecord
	
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceID := "unknown"
		if svcName, ok := rl.Resource().Attributes().AsRaw()["service.name"]; ok {
			resourceID = svcName.(string)
		}
		
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			
			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)
				
				records = append(records, LogRecord{
					Timestamp:  log.Timestamp().AsTime().UnixNano(),
					Severity:   log.SeverityText(),
					Body:       log.Body().AsString(),
					TraceID:    log.TraceID().String(),
					SpanID:     log.SpanID().String(),
					Attributes: log.Attributes().AsRaw(),
					ResourceID: resourceID,
				})
			}
		}
	}
	
	return writeParquet(records)
}

// writeParquet writes records to Parquet format using generic type T.
func writeParquet[T any](records []T) ([]byte, error) {
	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf)
	
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	
	if err := writer.Close(); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}