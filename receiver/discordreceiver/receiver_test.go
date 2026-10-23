// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package discordreceiver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

type fakeGateway struct {
	handler         any
	openErr         error
	closed, removed bool
}

func (f *fakeGateway) AddHandler(h any) func() {
	if _, ok := h.(func(*discordgo.Session, *discordgo.MessageCreate)); ok {
		f.handler = h
	}
	return func() { f.removed = true }
}
func (f *fakeGateway) Open(context.Context) error { return f.openErr }
func (f *fakeGateway) Close() error               { f.closed = true; return nil }

func TestLifecycle(t *testing.T) {
	sink := &consumertest.MetricsSink{}
	r := testReceiver(t, sink)
	gateway := &fakeGateway{}
	r.session = gateway
	r.config.BufferInterval = 5 * time.Millisecond
	startCtx, cancel := context.WithCancel(context.Background())
	require.NoError(t, r.Start(startCtx, nil))
	cancel() // The startup context must not own the receiver's lifetime.
	handler := gateway.handler.(func(*discordgo.Session, *discordgo.MessageCreate))
	handler(nil, &discordgo.MessageCreate{Message: &discordgo.Message{ID: "1", GuildID: "G", ChannelID: "C", Content: "hi"}})
	require.Eventually(t, func() bool { return len(sink.AllMetrics()) > 0 }, time.Second, time.Millisecond)
	handler(nil, &discordgo.MessageCreate{Message: &discordgo.Message{ID: "2", GuildID: "G", ChannelID: "C", Content: "bye"}})
	require.NoError(t, r.Shutdown(context.Background()))
	require.True(t, gateway.closed)
	require.True(t, gateway.removed)
	count := len(sink.AllMetrics())
	require.NoError(t, r.Shutdown(context.Background()))
	require.Len(t, sink.AllMetrics(), count)
	last := sink.AllMetrics()[count-1]
	require.Equal(t, int64(2), last.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).IntValue())
	require.NoError(t, testReceiver(t, sink).Shutdown(context.Background()))
}

func TestStartupFailureAndIntents(t *testing.T) {
	r := testReceiver(t, &consumertest.MetricsSink{})
	s := r.session.(*discordGateway).Session
	require.NotZero(t, s.Identify.Intents&discordgo.IntentMessageContent)
	require.NotZero(t, s.Identify.Intents&discordgo.IntentsGuildMessages)
	gateway := &fakeGateway{openErr: errors.New("invalid token")}
	r.session = gateway
	require.Error(t, r.Start(context.Background(), nil))
	require.True(t, gateway.closed)
	require.True(t, gateway.removed)
	require.NoError(t, r.Shutdown(context.Background()))
}

func TestMessageFiltering(t *testing.T) {
	r := testReceiver(t, &consumertest.MetricsSink{})
	r.messageCreate(nil, nil)
	r.messageCreate(nil, &discordgo.MessageCreate{})
	for _, m := range []*discordgo.Message{
		{GuildID: "G", ChannelID: "C", Author: &discordgo.User{Bot: true}},
		{GuildID: "G", ChannelID: "C", WebhookID: "W"},
		{ChannelID: "DM"},
		{GuildID: "G", ChannelID: "C", Type: discordgo.MessageTypeChannelPinnedMessage},
	} {
		r.messageCreate(nil, &discordgo.MessageCreate{Message: m})
	}
	require.Empty(t, r.stats.totals)
	r.config.IncludeBots = true
	r.messageCreate(nil, &discordgo.MessageCreate{Message: &discordgo.Message{GuildID: "G", ChannelID: "C", Author: &discordgo.User{Bot: true}}})
	r.messageCreate(nil, &discordgo.MessageCreate{Message: &discordgo.Message{GuildID: "G", ChannelID: "C", Type: discordgo.MessageTypeReply}})
	require.Equal(t, int64(2), r.stats.totals["G"]["C"].count)
}

type reconnectGateway struct {
	fakeGateway
	attempts     int
	reconnecting chan struct{}
}

func (f *reconnectGateway) Open(ctx context.Context) error {
	f.attempts++
	if f.attempts == 1 {
		return nil
	}
	close(f.reconnecting)
	<-ctx.Done()
	return ctx.Err()
}

func TestShutdownCancelsReconnect(t *testing.T) {
	r := testReceiver(t, &consumertest.MetricsSink{})
	gateway := &reconnectGateway{reconnecting: make(chan struct{})}
	r.session = gateway
	require.NoError(t, r.Start(context.Background(), nil))
	r.disconnected <- struct{}{}
	select {
	case <-gateway.reconnecting:
	case <-time.After(3 * time.Second):
		t.Fatal("reconnect did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, r.Shutdown(ctx))
	require.True(t, gateway.closed)
	select {
	case <-r.done:
	default:
		t.Fatal("reconnect worker still running")
	}
}
