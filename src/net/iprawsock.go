// Copyright 2010 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package net

// IPAddr represents the address of an IP end point.
type IPAddr struct { // raw ip 地址结构，实现Addr接口
	IP   IP
	Zone string // IPv6 scoped addressing zone
}

// Network returns the address's network name, "ip".
func (a *IPAddr) Network() string { return "ip" } // 返回ip地址网络名ip

func (a *IPAddr) String() string { // 将ip地址变为字符串
	if a == nil {
		return "<nil>"
	}
	ip := ipEmptyString(a.IP)
	if a.Zone != "" {
		return ip + "%" + a.Zone
	}
	return ip
}

func (a *IPAddr) isWildcard() bool {
	if a == nil || a.IP == nil {
		return true
	}
	return a.IP.IsUnspecified()
}

func (a *IPAddr) opAddr() Addr {
	if a == nil {
		return nil
	}
	return a
}

// ResolveIPAddr parses addr as an IP address of the form "host" or
// "ipv6-host%zone" and resolves the domain name on the network net,
// which must be "ip", "ip4" or "ip6".
func ResolveIPAddr(net, addr string) (*IPAddr, error) { // 通过字符串解析出IP地址
	if net == "" { // a hint wildcard for Go 1.0 undocumented behavior
		net = "ip"
	}
	afnet, _, err := parseNetwork(net)
	if err != nil {
		return nil, err
	}
	switch afnet {
	case "ip", "ip4", "ip6":
	default:
		return nil, UnknownNetworkError(net)
	}
	addrs, err := internetAddrList(afnet, addr, noDeadline)
	if err != nil {
		return nil, err
	}
	return addrs.first(isIPv4).(*IPAddr), nil
}
