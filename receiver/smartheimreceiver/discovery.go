// Copyright 2026 Yoshi Yamaguchi
// SPDX-License-Identifier: Apache-2.0

package smartheimreceiver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"
)

// Discovery needs no network-admin capabilities, multicast, or external command.
// On Linux, net.Interfaces needs an AF_NETLINK socket for NETLINK_ROUTE queries.
// A valid measurement page and device number identify the target.
func discoveryNetworks(configured []string) ([]netip.Prefix, error) {
	var networks []netip.Prefix
	add := func(raw string) {
		p, err := discoveryPrefix(raw)
		if err == nil && !slices.Contains(networks, p) {
			networks = append(networks, p)
		}
	}
	if len(configured) > 0 {
		for _, raw := range configured {
			add(raw)
		}
	} else {
		interfaces, err := net.Interfaces()
		if err != nil {
			return nil, err
		}
		for _, iface := range interfaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagPointToPoint != 0 {
				continue
			}
			// Exclude common container/VM bridges and tunnel interfaces.
			if virtualInterface(iface.Name) {
				continue
			}
			addresses, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, address := range addresses {
				add(address.String())
			}
		}
	}
	if len(networks) == 0 || len(networks) > 4 {
		return nil, errors.New("discovery requires one to four private IPv4 /24 through /30 networks; set discovery.networks explicitly")
	}
	return networks, nil
}

func virtualInterface(name string) bool {
	for _, prefix := range []string{"docker", "br-", "veth", "virbr", "tailscale", "tun", "tap"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func candidateEndpoints(networks []netip.Prefix) []string {
	var endpoints []string
	seen := map[netip.Addr]bool{}
	for _, network := range networks {
		// Exclude the network and broadcast address on these /24-/30 networks.
		for ip := network.Addr().Next(); network.Contains(ip.Next()); ip = ip.Next() {
			if !seen[ip] {
				endpoints = append(endpoints, "http://"+ip.String())
				seen[ip] = true
			}
		}
	}
	return endpoints
}

func discover(ctx context.Context, client *http.Client, endpoints []string, deviceID string, concurrency int) (string, error) {
	jobs := make(chan string)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var matches []string
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for endpoint := range jobs {
				if ctx.Err() != nil {
					return
				}
				s, err := readSummary(ctx, client, endpoint)
				if err == nil && (deviceID == "" || deviceID == s.id) {
					mu.Lock()
					matches = append(matches, endpoint)
					mu.Unlock()
				}
			}
		}()
	}
send:
	for _, endpoint := range endpoints {
		select {
		case jobs <- endpoint:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	switch len(matches) {
	case 0:
		return "", errors.New("no matching Smart Heim Navi device found")
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf("multiple matching Smart Heim Navi devices found (%d); configure device_id or endpoint", len(matches))
	}
}
