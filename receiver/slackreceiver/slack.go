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

	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
	"go.uber.org/zap"
)

type socketClient interface {
	RunContext(context.Context) error
	AckCtx(context.Context, string, any) error
	events() <-chan socketmode.Event
}

type socketAdapter struct{ *socketmode.Client }

func (c *socketAdapter) events() <-chan socketmode.Event { return c.Events }

func (r *slackReceiver) eventsLoop(ctx context.Context, ready chan<- error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-r.client.events():
			if !ok {
				return errors.New("Slack event channel closed unexpectedly")
			}
			if event.Type == socketmode.EventTypeConnected {
				select {
				case ready <- nil:
				default:
				}
			}
			if event.Type == socketmode.EventTypeEventsAPI {
				if payload, ok := event.Data.(slackevents.EventsAPIEvent); ok {
					if message, ok := payload.InnerEvent.Data.(*slackevents.MessageEvent); ok {
						team := payload.TeamID
						if team == "" {
							team = r.teamID
						}
						r.messageEvent(team, message)
					}
				}
			}
			// Acknowledge after recording, including ignored and duplicate events.
			// Keep the ack independent of downstream metric delivery.
			if event.Request != nil && event.Request.EnvelopeID != "" {
				ackCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				err := r.client.AckCtx(ackCtx, event.Request.EnvelopeID, nil)
				cancel()
				if err != nil && ctx.Err() == nil {
					r.settings.Logger.Warn("Failed to acknowledge Slack event", zap.Error(err))
				}
			}
		}
	}
}

func (r *slackReceiver) messageEvent(team string, event *slackevents.MessageEvent) {
	if event == nil {
		return
	}
	switch event.SubType {
	case "", "file_share", "me_message", "thread_broadcast", "bot_message":
	default:
		return // Edits, deletes, and channel administration do not add messages.
	}
	if !r.config.IncludeBots && (event.BotID != "" || event.SubType == "bot_message") {
		return
	}
	id := ""
	if event.TimeStamp != "" {
		id = team + ":" + event.Channel + ":" + event.TimeStamp
	}
	r.stats.record(r.config, team, event.Channel, id, event.Text)
}
