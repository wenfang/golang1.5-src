// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package net

import "errors"

var ( // 错误类型
	errInvalidInterface         = errors.New("invalid network interface")
	errInvalidInterfaceIndex    = errors.New("invalid network interface index")
	errInvalidInterfaceName     = errors.New("invalid network interface name")
	errNoSuchInterface          = errors.New("no such network interface")
	errNoSuchMulticastInterface = errors.New("no such multicast network interface")
)

// Interface represents a mapping between network interface name
// and index.  It also represents network interface facility
// information.
type Interface struct { // 网络接口名到索引的映射
	Index        int          // positive integer that starts at one, zero is never used 索引值
	MTU          int          // maximum transmission unit MTU大小
	Name         string       // e.g., "en0", "lo0", "eth0.100" 接口名
	HardwareAddr HardwareAddr // IEEE MAC-48, EUI-48 and EUI-64 form 硬件地址
	Flags        Flags        // e.g., FlagUp, FlagLoopback, FlagMulticast 接口Flag
}

type Flags uint

const ( // 接口的状态标记，位标记
	FlagUp           Flags = 1 << iota // interface is up 启动状态
	FlagBroadcast                      // interface supports broadcast access capability 广播状态
	FlagLoopback                       // interface is a loopback interface 本地loopback
	FlagPointToPoint                   // interface belongs to a point-to-point link 点对点连接
	FlagMulticast                      // interface supports multicast access capability 多播状态
)

var flagNames = []string{ // 接口状态的标记字符串
	"up",
	"broadcast",
	"loopback",
	"pointtopoint",
	"multicast",
}

func (f Flags) String() string { // flag到字符串的转换
	s := ""
	for i, name := range flagNames { // 遍历所有的flag名称
		if f&(1<<uint(i)) != 0 {
			if s != "" {
				s += "|"
			}
			s += name
		}
	}
	if s == "" {
		s = "0"
	}
	return s
}

// Addrs returns interface addresses for a specific interface.
func (ifi *Interface) Addrs() ([]Addr, error) {
	if ifi == nil {
		return nil, &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: errInvalidInterface}
	}
	ifat, err := interfaceAddrTable(ifi)
	if err != nil {
		err = &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: err}
	}
	return ifat, err
}

// MulticastAddrs returns multicast, joined group addresses for
// a specific interface.
func (ifi *Interface) MulticastAddrs() ([]Addr, error) {
	if ifi == nil {
		return nil, &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: errInvalidInterface}
	}
	ifat, err := interfaceMulticastAddrTable(ifi)
	if err != nil {
		err = &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: err}
	}
	return ifat, err
}

// Interfaces returns a list of the system's network interfaces.
func Interfaces() ([]Interface, error) { // 返回系统网络接口列表
	ift, err := interfaceTable(0)
	if err != nil {
		err = &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: err}
	}
	return ift, err
}

// InterfaceAddrs returns a list of the system's network interface
// addresses.
func InterfaceAddrs() ([]Addr, error) { // 返回接口的地址列表
	ifat, err := interfaceAddrTable(nil)
	if err != nil {
		err = &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: err}
	}
	return ifat, err
}

// InterfaceByIndex returns the interface specified by index.
func InterfaceByIndex(index int) (*Interface, error) {
	if index <= 0 {
		return nil, &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: errInvalidInterfaceIndex}
	}
	ift, err := interfaceTable(index)
	if err != nil {
		return nil, &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: err}
	}
	ifi, err := interfaceByIndex(ift, index)
	if err != nil {
		err = &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: err}
	}
	return ifi, err
}

func interfaceByIndex(ift []Interface, index int) (*Interface, error) {
	for _, ifi := range ift {
		if index == ifi.Index {
			return &ifi, nil
		}
	}
	return nil, errNoSuchInterface
}

// InterfaceByName returns the interface specified by name.
func InterfaceByName(name string) (*Interface, error) {
	if name == "" {
		return nil, &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: errInvalidInterfaceName}
	}
	ift, err := interfaceTable(0)
	if err != nil {
		return nil, &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: err}
	}
	for _, ifi := range ift {
		if name == ifi.Name {
			return &ifi, nil
		}
	}
	return nil, &OpError{Op: "route", Net: "ip+net", Source: nil, Addr: nil, Err: errNoSuchInterface}
}
