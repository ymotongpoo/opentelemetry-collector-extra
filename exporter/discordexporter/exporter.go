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

package discordexporter

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/bwmarrin/discordgo"
)

type discordExporter struct {
	config    *Config
	session   *discordgo.Session
	marshaler pmetric.JSONMarshaler
}

func newDiscordExporter(cfg *Config) (*discordExporter, error) {
	s, err := discordgo.New("Bot " + cfg.Token)
	if err != nil {
		return nil, err
	}
	return &discordExporter{
		session: s,
		config:  cfg,
	}, nil
}

func (d *discordExporter) Shutdown(context.Context) error {
	return d.session.Close()
}

func (d *discordExporter) pushMetrics(_ context.Context, md pmetric.Metrics) error {
	buf, err := d.marshaler.MarshalMetrics(md)
	if err != nil {
		return err
	}

	_, err = d.session.ChannelMessageSend(d.config.Channel, string(buf))
	if err != nil {
		return err
	}
	return nil
}
