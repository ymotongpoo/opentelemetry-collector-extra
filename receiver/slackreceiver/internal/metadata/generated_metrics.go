// Code generated by mdatagen. DO NOT EDIT.

package metadata

import (
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
)

type metricSlackMessagesCount struct {
	data     pmetric.Metric // data buffer for generated metric.
	config   MetricConfig   // metric config provided by user.
	capacity int            // max observed number of data points added to the metric.
}

// init fills slack.messages.count metric with initial data.
func (m *metricSlackMessagesCount) init() {
	m.data.SetName("slack.messages.count")
	m.data.SetDescription("The number of messages sent to the channel")
	m.data.SetUnit("{messages}")
	m.data.SetEmptySum()
	m.data.Sum().SetIsMonotonic(true)
	m.data.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	m.data.Sum().DataPoints().EnsureCapacity(m.capacity)
}

func (m *metricSlackMessagesCount) recordDataPoint(start pcommon.Timestamp, ts pcommon.Timestamp, val int64, slackChannelIDAttributeValue string) {
	if !m.config.Enabled {
		return
	}
	dp := m.data.Sum().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(start)
	dp.SetTimestamp(ts)
	dp.SetIntValue(val)
	dp.Attributes().PutStr("slack.channel.id", slackChannelIDAttributeValue)
}

// updateCapacity saves max length of data point slices that will be used for the slice capacity.
func (m *metricSlackMessagesCount) updateCapacity() {
	if m.data.Sum().DataPoints().Len() > m.capacity {
		m.capacity = m.data.Sum().DataPoints().Len()
	}
}

// emit appends recorded metric data to a metrics slice and prepares it for recording another set of data points.
func (m *metricSlackMessagesCount) emit(metrics pmetric.MetricSlice) {
	if m.config.Enabled && m.data.Sum().DataPoints().Len() > 0 {
		m.updateCapacity()
		m.data.MoveTo(metrics.AppendEmpty())
		m.init()
	}
}

func newMetricSlackMessagesCount(cfg MetricConfig) metricSlackMessagesCount {
	m := metricSlackMessagesCount{config: cfg}
	if cfg.Enabled {
		m.data = pmetric.NewMetric()
		m.init()
	}
	return m
}

type metricSlackMessagesLength struct {
	data     pmetric.Metric // data buffer for generated metric.
	config   MetricConfig   // metric config provided by user.
	capacity int            // max observed number of data points added to the metric.
}

// init fills slack.messages.length metric with initial data.
func (m *metricSlackMessagesLength) init() {
	m.data.SetName("slack.messages.length")
	m.data.SetDescription("The length of message sent to the channel")
	m.data.SetUnit("{characters}")
	m.data.SetEmptySum()
	m.data.Sum().SetIsMonotonic(true)
	m.data.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	m.data.Sum().DataPoints().EnsureCapacity(m.capacity)
}

func (m *metricSlackMessagesLength) recordDataPoint(start pcommon.Timestamp, ts pcommon.Timestamp, val int64, slackChannelIDAttributeValue string) {
	if !m.config.Enabled {
		return
	}
	dp := m.data.Sum().DataPoints().AppendEmpty()
	dp.SetStartTimestamp(start)
	dp.SetTimestamp(ts)
	dp.SetIntValue(val)
	dp.Attributes().PutStr("slack.channel.id", slackChannelIDAttributeValue)
}

// updateCapacity saves max length of data point slices that will be used for the slice capacity.
func (m *metricSlackMessagesLength) updateCapacity() {
	if m.data.Sum().DataPoints().Len() > m.capacity {
		m.capacity = m.data.Sum().DataPoints().Len()
	}
}

// emit appends recorded metric data to a metrics slice and prepares it for recording another set of data points.
func (m *metricSlackMessagesLength) emit(metrics pmetric.MetricSlice) {
	if m.config.Enabled && m.data.Sum().DataPoints().Len() > 0 {
		m.updateCapacity()
		m.data.MoveTo(metrics.AppendEmpty())
		m.init()
	}
}

func newMetricSlackMessagesLength(cfg MetricConfig) metricSlackMessagesLength {
	m := metricSlackMessagesLength{config: cfg}
	if cfg.Enabled {
		m.data = pmetric.NewMetric()
		m.init()
	}
	return m
}

// MetricsBuilder provides an interface for scrapers to report metrics while taking care of all the transformations
// required to produce metric representation defined in metadata and user config.
type MetricsBuilder struct {
	config                    MetricsBuilderConfig // config of the metrics builder.
	startTime                 pcommon.Timestamp    // start time that will be applied to all recorded data points.
	metricsCapacity           int                  // maximum observed number of metrics per resource.
	metricsBuffer             pmetric.Metrics      // accumulates metrics data before emitting.
	buildInfo                 component.BuildInfo  // contains version information.
	metricSlackMessagesCount  metricSlackMessagesCount
	metricSlackMessagesLength metricSlackMessagesLength
}

// metricBuilderOption applies changes to default metrics builder.
type metricBuilderOption func(*MetricsBuilder)

// WithStartTime sets startTime on the metrics builder.
func WithStartTime(startTime pcommon.Timestamp) metricBuilderOption {
	return func(mb *MetricsBuilder) {
		mb.startTime = startTime
	}
}

func NewMetricsBuilder(mbc MetricsBuilderConfig, settings receiver.CreateSettings, options ...metricBuilderOption) *MetricsBuilder {
	mb := &MetricsBuilder{
		config:                    mbc,
		startTime:                 pcommon.NewTimestampFromTime(time.Now()),
		metricsBuffer:             pmetric.NewMetrics(),
		buildInfo:                 settings.BuildInfo,
		metricSlackMessagesCount:  newMetricSlackMessagesCount(mbc.Metrics.SlackMessagesCount),
		metricSlackMessagesLength: newMetricSlackMessagesLength(mbc.Metrics.SlackMessagesLength),
	}
	for _, op := range options {
		op(mb)
	}
	return mb
}

// updateCapacity updates max length of metrics and resource attributes that will be used for the slice capacity.
func (mb *MetricsBuilder) updateCapacity(rm pmetric.ResourceMetrics) {
	if mb.metricsCapacity < rm.ScopeMetrics().At(0).Metrics().Len() {
		mb.metricsCapacity = rm.ScopeMetrics().At(0).Metrics().Len()
	}
}

// ResourceMetricsOption applies changes to provided resource metrics.
type ResourceMetricsOption func(pmetric.ResourceMetrics)

// WithResource sets the provided resource on the emitted ResourceMetrics.
// It's recommended to use ResourceBuilder to create the resource.
func WithResource(res pcommon.Resource) ResourceMetricsOption {
	return func(rm pmetric.ResourceMetrics) {
		res.CopyTo(rm.Resource())
	}
}

// WithStartTimeOverride overrides start time for all the resource metrics data points.
// This option should be only used if different start time has to be set on metrics coming from different resources.
func WithStartTimeOverride(start pcommon.Timestamp) ResourceMetricsOption {
	return func(rm pmetric.ResourceMetrics) {
		var dps pmetric.NumberDataPointSlice
		metrics := rm.ScopeMetrics().At(0).Metrics()
		for i := 0; i < metrics.Len(); i++ {
			switch metrics.At(i).Type() {
			case pmetric.MetricTypeGauge:
				dps = metrics.At(i).Gauge().DataPoints()
			case pmetric.MetricTypeSum:
				dps = metrics.At(i).Sum().DataPoints()
			}
			for j := 0; j < dps.Len(); j++ {
				dps.At(j).SetStartTimestamp(start)
			}
		}
	}
}

// EmitForResource saves all the generated metrics under a new resource and updates the internal state to be ready for
// recording another set of data points as part of another resource. This function can be helpful when one scraper
// needs to emit metrics from several resources. Otherwise calling this function is not required,
// just `Emit` function can be called instead.
// Resource attributes should be provided as ResourceMetricsOption arguments.
func (mb *MetricsBuilder) EmitForResource(rmo ...ResourceMetricsOption) {
	rm := pmetric.NewResourceMetrics()
	ils := rm.ScopeMetrics().AppendEmpty()
	ils.Scope().SetName("otelcol/slackreceiver")
	ils.Scope().SetVersion(mb.buildInfo.Version)
	ils.Metrics().EnsureCapacity(mb.metricsCapacity)
	mb.metricSlackMessagesCount.emit(ils.Metrics())
	mb.metricSlackMessagesLength.emit(ils.Metrics())

	for _, op := range rmo {
		op(rm)
	}
	if ils.Metrics().Len() > 0 {
		mb.updateCapacity(rm)
		rm.MoveTo(mb.metricsBuffer.ResourceMetrics().AppendEmpty())
	}
}

// Emit returns all the metrics accumulated by the metrics builder and updates the internal state to be ready for
// recording another set of metrics. This function will be responsible for applying all the transformations required to
// produce metric representation defined in metadata and user config, e.g. delta or cumulative.
func (mb *MetricsBuilder) Emit(rmo ...ResourceMetricsOption) pmetric.Metrics {
	mb.EmitForResource(rmo...)
	metrics := mb.metricsBuffer
	mb.metricsBuffer = pmetric.NewMetrics()
	return metrics
}

// RecordSlackMessagesCountDataPoint adds a data point to slack.messages.count metric.
func (mb *MetricsBuilder) RecordSlackMessagesCountDataPoint(ts pcommon.Timestamp, val int64, slackChannelIDAttributeValue string) {
	mb.metricSlackMessagesCount.recordDataPoint(mb.startTime, ts, val, slackChannelIDAttributeValue)
}

// RecordSlackMessagesLengthDataPoint adds a data point to slack.messages.length metric.
func (mb *MetricsBuilder) RecordSlackMessagesLengthDataPoint(ts pcommon.Timestamp, val int64, slackChannelIDAttributeValue string) {
	mb.metricSlackMessagesLength.recordDataPoint(mb.startTime, ts, val, slackChannelIDAttributeValue)
}

// Reset resets metrics builder to its initial state. It should be used when external metrics source is restarted,
// and metrics builder should update its startTime and reset it's internal state accordingly.
func (mb *MetricsBuilder) Reset(options ...metricBuilderOption) {
	mb.startTime = pcommon.NewTimestampFromTime(time.Now())
	for _, op := range options {
		op(mb)
	}
}
