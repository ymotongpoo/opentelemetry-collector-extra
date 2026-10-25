// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package smartheimreceiver

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/charset"
)

type summary struct {
	id                      string
	generation, consumption float64
	gridImport, gridExport  float64
	hasGrid                 bool
}

type branch struct {
	id, name string
	power    float64
}

func document(body io.Reader, contentType string) (*html.Node, error) {
	r, err := charset.NewReader(body, contentType)
	if err != nil {
		return nil, err
	}
	return html.Parse(r)
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func find(n *html.Node, match func(*html.Node) bool) *html.Node {
	if n.Type == html.ElementNode && match(n) {
		return n
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if found := find(c, match); found != nil {
			return found
		}
	}
	return nil
}

func byID(n *html.Node, id string) *html.Node {
	return find(n, func(n *html.Node) bool { return attr(n, "id") == id })
}

func nodeText(n *html.Node) string {
	if n == nil {
		return ""
	}
	if n.Type == html.TextNode {
		return n.Data
	}
	if n.Type != html.ElementNode && n.Type != html.DocumentNode {
		return ""
	}
	var b strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		b.WriteString(nodeText(c))
	}
	return b.String()
}

func valueNode(n *html.Node) *html.Node {
	if n == nil {
		return nil
	}
	return find(n, func(n *html.Node) bool {
		for _, class := range strings.Fields(attr(n, "class")) {
			if class == "value" {
				return true
			}
		}
		return false
	})
}

func watts(n *html.Node) (float64, error) {
	v := valueNode(n)
	if v == nil {
		return 0, errors.New("missing power value")
	}
	raw := strings.ReplaceAll(strings.TrimSpace(nodeText(v)), ",", "")
	p, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(p) || math.IsInf(p, 0) || p < 0 {
		return 0, fmt.Errorf("invalid power value %q", raw)
	}
	return p, nil
}

func pageDeviceID(root *html.Node) (string, error) {
	id := strings.TrimSpace(nodeText(byID(root, "number")))
	if id == "" {
		return "", errors.New("missing Smart Heim device number")
	}
	return id, nil
}

func parseSummary(root *html.Node) (summary, error) {
	s := summary{}
	var err error
	if s.id, err = pageDeviceID(root); err != nil {
		return s, err
	}
	if s.generation, err = watts(byID(root, "status-generate")); err != nil {
		return s, fmt.Errorf("generation: %w", err)
	}
	if s.consumption, err = watts(byID(root, "status-consume")); err != nil {
		return s, fmt.Errorf("consumption: %w", err)
	}
	// The arrow CSS class is not a reliable indication of flow direction.
	// Use the displayed Japanese label, including on battery-equipped layouts.
	grid := find(root, func(n *html.Node) bool { return strings.HasPrefix(attr(n, "id"), "status-buysell") })
	if grid != nil {
		p, e := watts(grid)
		if e != nil {
			return s, fmt.Errorf("grid: %w", e)
		}
		text := nodeText(grid)
		switch {
		case strings.Contains(text, "買電"):
			s.gridImport, s.hasGrid = p, true
		case strings.Contains(text, "売電"):
			s.gridExport, s.hasGrid = p, true
		case p == 0:
			s.hasGrid = true
		default:
			return s, errors.New("unrecognized grid flow label")
		}
	}
	return s, nil
}

func parseBranches(root *html.Node) (string, []branch, error) {
	id, err := pageDeviceID(root)
	if err != nil {
		return "", nil, err
	}
	wattage := byID(root, "wattage")
	if wattage == nil {
		return "", nil, errors.New("missing branch table")
	}
	var branches []branch
	position, slots := 0, 0
	var walk func(*html.Node) error
	walk = func(n *html.Node) error {
		if n.Type == html.ElementNode && n.Data == "dl" {
			dt := find(n, func(n *html.Node) bool { return n.Data == "dt" })
			name := strings.TrimSpace(nodeText(dt))
			v := valueNode(n)
			if name == "" && v == nil {
				return nil
			}
			position++
			if v == nil {
				return fmt.Errorf("branch %d: missing value", position)
			}
			slots++
			raw := strings.TrimSpace(nodeText(v))
			if raw == "－" || raw == "-" || raw == "―" || raw == "—" {
				return nil
			}
			p, e := watts(n)
			if e != nil {
				return fmt.Errorf("branch %d: %w", position, e)
			}
			branches = append(branches, branch{id: strconv.Itoa(position), name: name, power: p})
			return nil
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if e := walk(c); e != nil {
				return e
			}
		}
		return nil
	}
	if err := walk(wattage); err != nil {
		return "", nil, err
	}
	if slots == 0 {
		return "", nil, errors.New("empty branch table")
	}
	return id, branches, nil
}
