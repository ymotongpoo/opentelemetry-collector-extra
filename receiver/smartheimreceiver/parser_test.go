// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package smartheimreceiver

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/japanese"
)

func fixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name + ".html")
	require.NoError(t, err)
	return string(b)
}

func TestSummaryShiftJISAndDirection(t *testing.T) {
	for _, flow := range []string{"売電中", "買電中"} {
		t.Run(flow, func(t *testing.T) {
			src := strings.ReplaceAll(fixture(t, "summary"), "売電中", flow)
			src = strings.ReplaceAll(src, "utf-8", "Shift_JIS")
			encoded, err := japanese.ShiftJIS.NewEncoder().Bytes([]byte(src))
			require.NoError(t, err)
			root, err := document(bytes.NewReader(encoded), "")
			require.NoError(t, err)
			s, err := parseSummary(root)
			require.NoError(t, err)
			require.Equal(t, "TEST0001SC", s.id)
			require.Equal(t, float64(4000), s.generation)
			require.Equal(t, float64(500), s.consumption)
			require.True(t, s.hasGrid)
			if flow == "売電中" {
				require.Equal(t, float64(3500), s.gridExport)
				require.Zero(t, s.gridImport)
			} else {
				require.Equal(t, float64(3500), s.gridImport)
				require.Zero(t, s.gridExport)
			}
		})
	}
}

func TestSummaryInvalidAndMissing(t *testing.T) {
	for _, raw := range []string{"－", "NaN", "Inf", "-1", ""} {
		src := strings.Replace(fixture(t, "summary"), "4,000", raw, 1)
		root, err := document(strings.NewReader(src), "text/html; charset=utf-8")
		require.NoError(t, err)
		_, err = parseSummary(root)
		require.Error(t, err, raw)
	}
	root, err := document(strings.NewReader("<html>Login required</html>"), "")
	require.NoError(t, err)
	_, err = parseSummary(root)
	require.ErrorContains(t, err, "device number")
}

func TestBranchesSlotsMissingAndDuplicateNames(t *testing.T) {
	root, err := document(strings.NewReader(fixture(t, "branches")), "text/html; charset=utf-8")
	require.NoError(t, err)
	id, values, err := parseBranches(root)
	require.NoError(t, err)
	require.Equal(t, "TEST0001SC", id)
	require.Equal(t, []branch{{"1", "キッチン", 120}, {"3", "予備", 0}, {"4", "予備", 15}}, values)
	unnamed := strings.Replace(fixture(t, "branches"), "<dt>キッチン</dt>", "<dt> </dt>", 1)
	root, err = document(strings.NewReader(unnamed), "text/html; charset=utf-8")
	require.NoError(t, err)
	_, values, err = parseBranches(root)
	require.NoError(t, err)
	require.Equal(t, []branch{{"1", "", 120}, {"3", "予備", 0}, {"4", "予備", 15}}, values)
	for _, src := range []string{
		strings.Replace(fixture(t, "branches"), "> 120<", "> broken<", 1),
		"<div id='number'>TEST0001SC</div><div id='wattage'></div>",
	} {
		root, err = document(strings.NewReader(src), "text/html; charset=utf-8")
		require.NoError(t, err)
		_, _, err = parseBranches(root)
		require.Error(t, err)
	}
}
