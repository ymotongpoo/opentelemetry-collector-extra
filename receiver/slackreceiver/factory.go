// Copyright 2023 Yoshi Yamaguchi
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

package slackreceiver

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	metadata "github.com/ymotongpoo/opentelemetry-collector-extra/receiver/slackreceiver/internal/metadata"
)

// NewFactory returns a new factory for the Slack receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(metadata.Type),
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
	)
}

func createMetricsReceiver(
	_ context.Context,
	settings receiver.Settings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	if consumer == nil {
		return nil, fmt.Errorf("nil next consumer")
	}

	c := cfg.(*Config)
	err := c.Validate()
	if err != nil {
		return nil, err
	}
	return newSlackReceiver(c, settings, consumer)
}