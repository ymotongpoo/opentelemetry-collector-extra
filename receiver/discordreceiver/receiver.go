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
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
	"github.com/ymotongpoo/opentelemetry-collector-extra/receiver/discordreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type discordReceiver struct {
	disconnected            chan struct{}
	removeDisconnectHandler func()
	host                    component.Host
	consumer                consumer.Metrics
	settings                receiver.Settings
	config                  *Config
	obsrecv                 *receiverhelper.ObsReport
	mb                      *metadata.MetricsBuilder
	stats                   messageStats
	session                 gatewaySession
	removeHandler           func()
	cancel                  context.CancelFunc
	done                    chan struct{}
	shutdownOnce            sync.Once
	shutdownErr             error
}

func newDiscordReceiver(cfg *Config, settings receiver.Settings, next consumer.Metrics) (*discordReceiver, error) {
	obs, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID: settings.ID, Transport: "websocket", ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, err
	}
	session, err := newGatewaySession(cfg)
	if err != nil {
		return nil, err
	}
	return &discordReceiver{consumer: next, settings: settings, config: cfg, obsrecv: obs,
		mb: metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, settings), session: session, disconnected: make(chan struct{}, 1)}, nil
}

func (r *discordReceiver) Start(ctx context.Context, host component.Host) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.cancel != nil {
		return errors.New("discord receiver already started")
	}
	r.host = host
	r.removeHandler = r.session.AddHandler(r.messageCreate)
	r.removeDisconnectHandler = r.session.AddHandler(func(_ *discordgo.Session, _ *discordgo.Disconnect) {
		select {
		case r.disconnected <- struct{}{}:
		default:
		}
	})
	if err := r.session.Open(ctx); err != nil {
		r.removeHandler()
		r.removeDisconnectHandler()
		_ = r.session.Close()
		return err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(r.config.BufferInterval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-r.disconnected:
				if !r.reconnect(runCtx) {
					return
				}
			case <-ticker.C:
				if err := r.flush(runCtx); err != nil && runCtx.Err() == nil {
					r.settings.Logger.Error("Failed to emit Discord metrics", zap.Error(err))
				}
			}
		}
	}()
	return nil
}

func (r *discordReceiver) Shutdown(ctx context.Context) error {
	r.shutdownOnce.Do(func() {
		r.stats.stop()
		if r.cancel == nil {
			return
		}
		r.cancel()
		if r.removeHandler != nil {
			r.removeHandler()
		}
		if r.removeDisconnectHandler != nil {
			r.removeDisconnectHandler()
		}
		closeErr := r.session.Close()
		select {
		case <-r.done:
			r.shutdownErr = errors.Join(closeErr, r.flush(ctx))
		case <-ctx.Done():
			r.shutdownErr = errors.Join(closeErr, ctx.Err())
		}
	})
	return r.shutdownErr
}

// reconnect runs on the receiver worker, so Shutdown can cancel and join it.
func (r *discordReceiver) reconnect(ctx context.Context) bool {
	delay := time.Second
	for {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return false
		}
		err := r.session.Open(ctx)
		if err == nil || errors.Is(err, discordgo.ErrWSAlreadyOpen) {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		var closeErr *websocket.CloseError
		var restErr *discordgo.RESTError
		fatal := errors.As(err, &closeErr) && (closeErr.Code == 4004 || closeErr.Code >= 4010 && closeErr.Code <= 4014)
		if errors.As(err, &restErr) && restErr.Response != nil && (restErr.Response.StatusCode == 401 || restErr.Response.StatusCode == 403) {
			fatal = true
		}
		if fatal {
			componentstatus.ReportStatus(r.host, componentstatus.NewFatalErrorEvent(err))
			r.settings.Logger.Error("Discord Gateway rejected connection", zap.Error(err))
			return false
		}
		r.settings.Logger.Warn("Discord reconnect failed", zap.Error(err))
		if delay < 30*time.Second {
			delay = min(2*delay, 30*time.Second)
		}
	}
}
