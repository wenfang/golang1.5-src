// Copyright 2009 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package net

// UnixAddr represents the address of a Unix domain socket end point.
type UnixAddr struct { // unix socket 地址结构
	Name string
	Net  string
}

// Network returns the address's network name, "unix", "unixgram" or
// "unixpacket".
func (a *UnixAddr) Network() string { // 返回网络名
	return a.Net
}

func (a *UnixAddr) String() string { // 返回地址名
	if a == nil {
		return "<nil>"
	}
	return a.Name
}

func (a *UnixAddr) isWildcard() bool {
	return a == nil || a.Name == ""
}

func (a *UnixAddr) opAddr() Addr {
	if a == nil {
		return nil
	}
	return a
}

// ResolveUnixAddr parses addr as a Unix domain socket address.
// The string net gives the network name, "unix", "unixgram" or
// "unixpacket".
func ResolveUnixAddr(net, addr string) (*UnixAddr, error) { // 解析unix地址，返回unix地址结构
	switch net {
	case "unix", "unixgram", "unixpacket":
		return &UnixAddr{Name: addr, Net: net}, nil
	default: // 非Unix域地址，返回错误
		return nil, UnknownNetworkError(net)
	}
}
