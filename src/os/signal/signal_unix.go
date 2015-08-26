// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build darwin dragonfly freebsd linux nacl netbsd openbsd solaris windows

package signal

import (
	"os"
	"syscall"
)

// In assembly. 下面三个函数的定义在sig.s
func signal_disable(uint32)
func signal_enable(uint32)
func signal_ignore(uint32)
func signal_recv() uint32

func loop() { // loop执行process
	for {
		process(syscall.Signal(signal_recv())) // 循环执行process
	}
}

func init() { // 在导入该package后会生成一个goroutine执行loop
	signal_enable(0) // first call - initialize
	go loop()        // 生成一个goroutine，循环执行loop
}

const (
	numSig = 65 // max across all systems  最大信号的number
)

func signum(sig os.Signal) int {
	switch sig := sig.(type) {
	case syscall.Signal:
		i := int(sig)
		if i < 0 || i >= numSig {
			return -1
		}
		return i
	default:
		return -1
	}
}

func enableSignal(sig int) { // enable信号sig
	signal_enable(uint32(sig))
}

func disableSignal(sig int) { // disable信号sig
	signal_disable(uint32(sig))
}

func ignoreSignal(sig int) {
	signal_ignore(uint32(sig))
}
