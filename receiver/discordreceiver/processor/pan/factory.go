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

package panprocessor

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/ymotongpoo/opentelemetry-collector-extra/processor/panprocessor/internal/metadata"
)

func NewFactory() processor.Factory {
	return processor.NewFactory{
		metadata.Type,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
	}
}

func createLogsProcessor(
	_ context.Context,
	settings processor.CreateSettings,
	cfg component.Config,
	consumer consumer.Logs,
) (processor.Logs, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("could not initialize PAN processor")
	}
	if c.ReplaceMode != ReplaceModeFull && c.ReplaceMode != ReplaceModeFirstTwelve {
		return nil, errors.New("invalid replace mode")
	}
	return newLogsProcessor(c, consumer)
}
