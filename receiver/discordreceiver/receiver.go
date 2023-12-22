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

package discordreceiver

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
)

type discordReceiver struct {
	metricsConsumer consumer.Metrics
	logsConsumer    consumer.Logs
	settings        receiver.CreateSettings
	cancel          context.CancelFunc
	config          *Config
	dh              *discordHandler
	obsrecv         *receiverhelper.ObsReport
}

func newDiscordMetricsReceiver(
	config *Config,
	settings receiver.CreateSettings,
	consumer consumer.Metrics,
) (*discordReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "event",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, err
	}
	return &discordReceiver{
		metricsConsumer: consumer,
		settings:        settings,
		config:          config,
		obsrecv:         obsrecv,
	}, nil
}

func newDiscordLogsReceiver(
	config *Config,
	settings receiver.CreateSettings,
	consumer consumer.Logs,
) (*discordReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "event",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, err
	}
	return &discordReceiver{
		logsConsumer: consumer,
		settings:     settings,
		config:       config,
		obsrecv:      obsrecv,
	}, nil
}

func (r *discordReceiver) Start(ctx context.Context, _ component.Host) error {
	ctx, r.cancel = context.WithCancel(ctx)

	var err error
	r.dh, err = newDiscordHandler(r.metricsConsumer, r.logsConsumer, r.config, r.settings, r.obsrecv)
	if err != nil {
		return err
	}
	if r.dh == nil {
		return errors.New("failed to create discord handler")
	}
	if err := r.dh.run(ctx); err != nil {
		return err
	}
	return nil
}

func (r *discordReceiver) Shutdown(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}
