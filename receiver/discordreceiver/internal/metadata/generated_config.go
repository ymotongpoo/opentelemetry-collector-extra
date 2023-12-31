// Code generated by mdatagen. DO NOT EDIT.

package metadata

import "go.opentelemetry.io/collector/confmap"

// MetricConfig provides common config for a particular metric.
type MetricConfig struct {
	Enabled bool `mapstructure:"enabled"`

	enabledSetByUser bool
}

func (ms *MetricConfig) Unmarshal(parser *confmap.Conf) error {
	if parser == nil {
		return nil
	}
	err := parser.Unmarshal(ms, confmap.WithErrorUnused())
	if err != nil {
		return err
	}
	ms.enabledSetByUser = parser.IsSet("enabled")
	return nil
}

// MetricsConfig provides config for discord metrics.
type MetricsConfig struct {
	DiscordMessagesCount  MetricConfig `mapstructure:"discord.messages.count"`
	DiscordMessagesLength MetricConfig `mapstructure:"discord.messages.length"`
}

func DefaultMetricsConfig() MetricsConfig {
	return MetricsConfig{
		DiscordMessagesCount: MetricConfig{
			Enabled: true,
		},
		DiscordMessagesLength: MetricConfig{
			Enabled: true,
		},
	}
}

// MetricsBuilderConfig is a configuration for discord metrics builder.
type MetricsBuilderConfig struct {
	Metrics MetricsConfig `mapstructure:"metrics"`
}

func DefaultMetricsBuilderConfig() MetricsBuilderConfig {
	return MetricsBuilderConfig{
		Metrics: DefaultMetricsConfig(),
	}
}
