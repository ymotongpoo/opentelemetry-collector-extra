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
	"net"
	"net/http"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

type gatewaySession interface {
	AddHandler(any) func()
	Open(context.Context) error
	Close() error
}

type discordGateway struct {
	*discordgo.Session
	nextOpen time.Time
}

type startupTransport struct{ ctx context.Context }

func (t startupTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return http.DefaultTransport.RoundTrip(request.WithContext(t.ctx))
}

func newGatewaySession(cfg *Config) (*discordGateway, error) {
	session, err := discordgo.New("Bot " + string(cfg.Token))
	if err != nil {
		return nil, err
	}
	session.Identify.Intents = discordgo.IntentsGuildMessages | discordgo.IntentMessageContent
	// Own reconnects: DiscordGo's built-in retry sleeps cannot be canceled.
	session.ShouldReconnectOnError = false
	session.ShouldRetryOnRateLimit = false
	session.MaxRestRetries = 0
	session.SyncEvents = true
	return &discordGateway{Session: session}, nil
}

// Open bounds both gateway discovery and the WebSocket HELLO/READY exchange.
// DiscordGo accepts no context here, so cancellation closes the underlying socket.
func (g *discordGateway) Open(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// Wait before entering DiscordGo's non-cancellable REST bucket lock.
	delay := max(time.Until(g.nextOpen), g.Ratelimiter.GetWaitTime(g.Ratelimiter.GetBucket(discordgo.EndpointGateway), 1))
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.Client = &http.Client{Timeout: 10 * time.Second, Transport: startupTransport{ctx}}
	var conn net.Conn
	var stop func() bool
	g.Dialer = &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 10 * time.Second,
		NetDialContext: func(_ context.Context, network, address string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			conn = c
			stop = context.AfterFunc(ctx, func() { _ = c.Close() })
			if deadline, ok := ctx.Deadline(); ok {
				_ = c.SetDeadline(deadline)
			}
			return c, nil
		},
	}
	err := g.Session.Open()
	var limited *discordgo.RateLimitError
	if errors.As(err, &limited) {
		g.nextOpen = time.Now().Add(limited.RetryAfter)
	}
	if stop != nil {
		stop()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil && conn != nil {
		_ = conn.SetDeadline(time.Time{})
	}
	return err
}

func (r *discordReceiver) messageCreate(_ *discordgo.Session, event *discordgo.MessageCreate) {
	if event == nil || event.Message == nil || event.GuildID == "" {
		return
	}
	if !r.config.IncludeBots && (event.WebhookID != "" || (event.Author != nil && event.Author.Bot)) {
		return
	}
	if event.Type != discordgo.MessageTypeDefault && event.Type != discordgo.MessageTypeReply {
		return
	}
	r.stats.record(r.config, event.GuildID, event.ChannelID, event.ID, event.Content)
}
