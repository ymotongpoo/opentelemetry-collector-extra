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
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/slackreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type slackReceiver struct {
	consumer     consumer.Metrics
	settings     receiver.Settings
	config       *Config
	obsrecv      *receiverhelper.ObsReport
	mb           *metadata.MetricsBuilder
	stats        messageStats
	client       socketClient
	authTest     func(context.Context) (*slack.AuthTestResponse, error)
	teamID       string
	cancel       context.CancelFunc
	done         chan struct{}
	shutdownOnce sync.Once
	shutdownErr  error
}

func newSlackReceiver(cfg *Config, settings receiver.Settings, next consumer.Metrics) (*slackReceiver, error) {
	obs, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID: settings.ID, Transport: "websocket", ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, err
	}
	api := slack.New(string(cfg.BotToken), slack.OptionAppLevelToken(string(cfg.AppToken)),
		slack.OptionHTTPClient(&http.Client{Timeout: 10 * time.Second}))
	return &slackReceiver{consumer: next, settings: settings, config: cfg, obsrecv: obs,
		mb:     metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, settings),
		client: &socketAdapter{socketmode.New(api)}, authTest: api.AuthTestContext}, nil
}

func (r *slackReceiver) Start(ctx context.Context, host component.Host) error {
	if r.cancel != nil {
		return errors.New("slack receiver already started")
	}
	startupCtx, startupCancel := context.WithTimeout(ctx, 15*time.Second)
	defer startupCancel()
	auth, err := r.authTest(startupCtx)
	if err != nil {
		return fmt.Errorf("Slack authentication failed: %w", err)
	}
	r.teamID = auth.TeamID
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	ready := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		err := r.client.RunContext(runCtx)
		if runCtx.Err() == nil {
			if err == nil {
				err = errors.New("Slack Socket Mode stopped unexpectedly")
			}
			select {
			case ready <- err:
			default:
			}
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(err))
			r.settings.Logger.Error("Slack Socket Mode stopped", zap.Error(err))
		}
		cancel()
	}()
	go func() {
		defer wg.Done()
		err := r.eventsLoop(runCtx, ready)
		if runCtx.Err() == nil {
			select {
			case ready <- err:
			default:
			}
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(err))
			r.settings.Logger.Error("Slack event loop stopped", zap.Error(err))
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(r.config.BufferInterval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				if err := r.flush(runCtx); err != nil && runCtx.Err() == nil {
					r.settings.Logger.Error("Failed to emit Slack metrics", zap.Error(err))
				}
			}
		}
	}()
	go func() { wg.Wait(); close(r.done) }()
	select {
	case err = <-ready:
	case <-startupCtx.Done():
		err = startupCtx.Err()
	}
	if err != nil {
		cancel()
		<-r.done
		return fmt.Errorf("Slack Socket Mode startup failed: %w", err)
	}
	return nil
}

func (r *slackReceiver) Shutdown(ctx context.Context) error {
	r.shutdownOnce.Do(func() {
		r.stats.stop()
		if r.cancel == nil {
			return
		}
		r.cancel()
		select {
		case <-r.done:
			r.shutdownErr = r.flush(ctx)
		case <-ctx.Done():
			r.shutdownErr = ctx.Err()
		}
	})
	return r.shutdownErr
}
