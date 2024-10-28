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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
)

type panProcessor struct {
	mode ReplaceMode
}

func newLogsProcessor(c *Config, consumer consumer.Logs) (*panProcessor, error) {
	pp := &panProcessor{
		mode: c.ReplaceMode,
	}
	return pp, nil
}

func (pp *panProcessor) Start(_ context.Context, host component.Host) error {
	return nil
}

func (pp *panProcessor) ConsumeLogs(ctx context.Context, l plog.Logs) error {
	for i := 0; i < l.ResourceLogs().Len(); i++ {
		rlogs := l.ResourceLogs().At(i)
		for j := 0; j < rls.ScopeLogs().Len(); j++ {
			scope := rls.ScopeLogs().At(j)
			item := fromConverterWorkerItem{
				Resource:       rls.Resource(),
				Scope:          scope,
				LogRecordSlice: scope.LogRecords(),
			}
			select {
			case c.workerChan <- item:
				continue
			case <-c.stopChan:
				return nil
			}
		}
	}
}
