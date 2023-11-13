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
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"

	"github.com/bwmarrin/discordgo"
	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/discordreceiver/internal/metadata"
)

type discordHandler struct {
	session  *discordgo.Session
	consumer consumer.Metrics
	cancel   context.CancelFunc
	config   *Config
	obsrecv  *receiverhelper.ObsReport
	mb       *metadata.MetricsBuilder
	mcCh     chan messageCreateEvent
}

func newDiscordHandler(consumer consumer.Metrics, cfg *Config, settings receiver.CreateSettings, obsrecv *receiverhelper.ObsReport) (*discordHandler, error) {
	s, err := discordgo.New("Bot " + cfg.Token)
	if err != nil {
		return nil, err
	}

	dh := &discordHandler{
		session:  s,
		consumer: consumer,
		config:   cfg,
		obsrecv:  obsrecv,
		mb:       metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, settings),
		mcCh:     make(chan messageCreateEvent, 1000),
	}
	return dh, nil
}

const (
	dataFormat = "discord"
)

type messageCreateEvent struct {
	s *discordgo.Session
	m *discordgo.MessageCreate
}

func (dh *discordHandler) messageCreateFunc(ctx context.Context) func(s *discordgo.Session, m *discordgo.MessageCreate) {
	return func(s *discordgo.Session, m *discordgo.MessageCreate) {
		dh.mcCh <- messageCreateEvent{
			s: s,
			m: m,
		}
	}
}

func (dh *discordHandler) run(ctx context.Context) error {
	dh.session.AddHandler(dh.messageCreateFunc(ctx))
	if err := dh.session.Open(); err != nil {
		return err
	}
	defer dh.session.Close()

	d, err := time.ParseDuration(dh.config.BufferInterval)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(d)
TICK:
	for {
		select {
		case e := <-dh.mcCh:
			dh.messageCreateToMetrics(e)
		case <-ticker.C:
			metrics := dh.mb.Emit()
			dh.obsrecv.StartMetricsOp(ctx)
			err := dh.consumer.ConsumeMetrics(ctx, metrics)
			dh.obsrecv.EndMetricsOp(ctx, dataFormat, metrics.DataPointCount(), err)
		case <-ctx.Done():
			break TICK
		}
	}

	return nil
}

func (dh *discordHandler) messageCreateToMetrics(e messageCreateEvent) {
	now := pcommon.NewTimestampFromTime(time.Now())
	dh.mb.RecordDiscordMessagesCountDataPoint(now, 1, e.m.ChannelID)
	dh.mb.RecordDiscordMessagesLengthDataPoint(now, int64(len(e.m.Content)), e.m.ChannelID)
}
