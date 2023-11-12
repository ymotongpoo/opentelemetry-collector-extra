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
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/bwmarrin/discordgo"
	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/discordreceiver/internal/metadata"
)

type discordUnmarshaler struct {
	mb     *metadata.MetricsBuilder
	logger *zap.Logger
}

func newDiscordUnmarshaler(mbc metadata.MetricsBuilderConfig, params receiver.CreateSettings) *discordUnmarshaler {
	return &discordUnmarshaler{
		mb:     metadata.NewMetricsBuilder(mbc, params),
		logger: params.Logger,
	}
}

func (du *discordUnmarshaler) UnmarshalMessageCreateMetrics(m *discordgo.MessageCreate) (pmetric.Metrics, error) {
	now := pcommon.NewTimestampFromTime(time.Now())
	du.mb.RecordDiscordMessagesCountDataPoint(now, 1)
	du.mb.RecordDiscordMessagesLengthDataPoint(now, int64(len(m.Content)))

	rb := du.mb.NewResourceBuilder()
	rb.SetDiscordChannelID(m.ChannelID)
	du.mb.Emit(metadata.WithResource(rb.Emit()))

	return du.mb.Emit(), nil
}
