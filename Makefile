OCB_VERSION := v0.163.0
VERSION ?= 0.3.1
ARCH ?= $(shell dpkg --print-architecture)
export GOTOOLCHAIN := go1.27.3

.PHONY: build test vet generate validate collector-build deb
collector-build:
	./scripts/build-collector.sh "$(VERSION)" "$(ARCH)"

deb:
	./scripts/build-deb.sh "$(VERSION)" "$(ARCH)"

build:
	go run go.opentelemetry.io/collector/cmd/builder@$(OCB_VERSION) --config=otelcol-builder.yaml

test:
	./scripts/check-modules.sh test -race -timeout=5m

vet:
	./scripts/check-modules.sh vet

generate:
	./scripts/generate-metadata.sh

validate:
	./otelcol-extra validate --config=examples/smartheim.yaml
	DISCORD_BOT_TOKEN=test SLACK_BOT_TOKEN=xoxb-test SLACK_APP_TOKEN=xapp-test ./otelcol-extra validate --config=otel-config.yaml
	DISCORD_BOT_TOKEN=test SLACK_BOT_TOKEN=xoxb-test DISCORD_CHANNEL_ID=123456789012345678 SLACK_CHANNEL_ID=C0123456789 ./otelcol-extra validate --config=examples/export-metrics.yaml
