// Copyright 2025 Yoshi Yamaguchi <ymotongpoo@gmail.com>
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

package s3tablesexporter

import (
	"testing"
)

func TestNewSlogLogger(t *testing.T) {
	logger := newSlogLogger()
	if logger == nil {
		t.Fatal("newSlogLogger() returned nil")
	}

	// Test that logger can be used
	logger.Info("test message", "key", "value")
	logger.Debug("debug message")
	logger.Error("error message", "error", "test error")
}
