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

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/bwmarrin/discordgo"
)

type discordHandler struct {
	session  *discordgo.Session
	consumer consumer.Metrics
	cancel   context.CancelFunc
	config   *Config
	unm      *discordUnmarshaler
}

func newDiscordHandler(consumer consumer.Metrics, cfg *Config, settings receiver.CreateSettings) (*discordHandler, error) {
	s, err := discordgo.New("Bot " + cfg.Token)
	if err != nil {
		return nil, err
	}

	dh := &discordHandler{
		session:  s,
		consumer: consumer,
		config:   cfg,
		unm:      newDiscordUnmarshaler(cfg.MetricsBuilderConfig, settings),
	}
	return dh, nil
}

func (dh *discordHandler) run(ctx context.Context) error {
	dh.session.AddHandler(dh.messageCreateFunc(ctx))
	if err := dh.session.Open(); err != nil {
		dh.unm.logger.Error(err.Error())
		return err
	}
	defer dh.session.Close()
	dh.unm.logger.Info("Discord receiver started")

	<-ctx.Done()
	dh.cancel()
	dh.unm.logger.Info("Discord receiver stopped")

	return nil
}

func (dh *discordHandler) messageCreateFunc(ctx context.Context) func(s *discordgo.Session, m *discordgo.MessageCreate) {
	handler := func(s *discordgo.Session, m *discordgo.MessageCreate) {
		metrics, err := dh.unm.UnmarshalMessageCreateMetrics(m)
		if err != nil {
			return
		}
		dh.consumer.ConsumeMetrics(ctx, metrics)
	}
	return handler
}
