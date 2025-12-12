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
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
)

type slackReceiver struct {
	consumer consumer.Metrics
	settings receiver.Settings
	cancel   context.CancelFunc
	config   *Config
	sh       *slackHandler
	obsrecv  *receiverhelper.ObsReport
}

func newSlackReceiver(config *Config, settings receiver.Settings, consumer consumer.Metrics) (*slackReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "event",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, err
	}
	return &slackReceiver{
		consumer: consumer,
		settings: settings,
		config:   config,
		obsrecv:  obsrecv,
	}, nil
}

func (r *slackReceiver) Start(ctx context.Context, _ component.Host) error {
	ctx, r.cancel = context.WithCancel(ctx)

	var err error
	r.sh, err = newSlackHandler(r.consumer, r.config, r.settings, r.obsrecv)
	if err != nil {
		return err
	}
	if r.sh == nil {
		return errors.New("failed to create slack handler")
	}
	if err := r.sh.run(ctx); err != nil {
		return err
	}
	return nil
}

func (r *slackReceiver) Shutdown(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}
