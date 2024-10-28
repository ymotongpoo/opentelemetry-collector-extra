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
	"errors"

	"go.opentelemetry.io/collector/component"
)

type ReplaceMode string

const (
	ReplaceModeFull        ReplaceMode = "full"
	ReplaceModeFirstTwelve             = "twelve"
)

type Config struct {
	ReplaceMode ReplaceMode `mapstructure:"replace_mode"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	if c.ReplaceMode != ReplaceModeFull && c.ReplaceMode != ReplaceModeFirstTwelve {
		return errors.New("unknown option for replace mode")
	}
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{
		ReplaceMode: ReplaceModeFull,
	}
}
