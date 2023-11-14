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
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	metadata "github.com/ymotongpoo/opentelemetry-collector-extra/receiver/slackreceiver/internal/metadata"
)

type slackHandler struct {
	client   *socketmode.Client
	consumer consumer.Metrics
	cancel   context.CancelFunc
	settings receiver.CreateSettings
	config   *Config
	obsrecv  *receiverhelper.ObsReport
	mb       *metadata.MetricsBuilder
	meCh     chan *slackevents.MessageEvent
}

var errSlackClient = errors.New("failed to create slack client")

func newSlackHandler(consumer consumer.Metrics, cfg *Config, settings receiver.CreateSettings, obsrecv *receiverhelper.ObsReport) (*slackHandler, error) {
	api := slack.New(cfg.BotToken, slack.OptionAppLevelToken(cfg.AppToken))
	cli := socketmode.New(api)
	if cli == nil {
		return nil, errSlackClient
	}

	sh := &slackHandler{
		client:   cli,
		consumer: consumer,
		settings: settings,
		config:   cfg,
		obsrecv:  obsrecv,
		mb:       metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, settings),
		meCh:     make(chan *slackevents.MessageEvent, 1000),
	}
	return sh, nil
}

const (
	dataFormat = "slack"
)

func (sh *slackHandler) run(ctx context.Context) error {
	sh.settings.Logger.Info("start slack handler")
	ctx, sh.cancel = context.WithCancel(ctx)
	d, err := time.ParseDuration(sh.config.BufferInterval)
	if err != nil {
		return err
	}
	go sh.eventsLoop()

	ticker := time.NewTicker(d)
TICK:
	for {
		select {
		case e := <-sh.meCh:
			sh.messageEventToMetrics(e)
		case <-ticker.C:
			metrics := sh.mb.Emit()
			sh.obsrecv.StartMetricsOp(ctx)
			err := sh.consumer.ConsumeMetrics(ctx, metrics)
			sh.obsrecv.EndMetricsOp(ctx, dataFormat, metrics.DataPointCount(), err)
		case <-ctx.Done():
			break TICK
		}
	}

	return nil
}

func (sh *slackHandler) eventsLoop() {
	sh.settings.Logger.Info("start events loop")
	for env := range sh.client.Events {
		switch env.Type {
		case socketmode.EventTypeEventsAPI:
			sh.client.Ack(*env.Request)
			payload, _ := env.Data.(slackevents.EventsAPIEvent)
			switch e := payload.InnerEvent.Data.(type) {
			case *slackevents.MessageEvent:
				sh.settings.Logger.Info(e.Text)
				sh.meCh <- e
			default:
				sh.settings.Logger.Info(payload.InnerEvent.Type)
			}
		default:
			sh.settings.Logger.Info(string(env.Type))
		}
	}
	if err := sh.client.Run(); err != nil {
		return
	}
}

func (sh *slackHandler) messageEventToMetrics(e *slackevents.MessageEvent) {
	now := pcommon.NewTimestampFromTime(time.Now())
	channelID := e.Channel
	matched := false
	switch {
	case sh.config.ServerWide:
		channelID = "@all@"
		matched = true
	case len(sh.config.Channels) == 0:
		matched = true
	default:
		for _, ch := range sh.config.Channels {
			if ch == channelID {
				matched = true
				break
			}
		}
	}
	if !matched {
		return
	}
	sh.mb.RecordSlackMessagesCountDataPoint(now, 1, channelID)
	sh.mb.RecordSlackMessagesLengthDataPoint(now, int64(len(e.Text)), channelID)
}
