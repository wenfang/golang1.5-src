// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build darwin dragonfly freebsd linux netbsd openbsd windows solaris

package net

import (
	"sync"
	"syscall"
	"time"
)

// runtimeNano returns the current value of the runtime clock in nanoseconds.
func runtimeNano() int64

func runtime_pollServerInit()
func runtime_pollOpen(fd uintptr) (uintptr, int)
func runtime_pollClose(ctx uintptr)
func runtime_pollWait(ctx uintptr, mode int) int
func runtime_pollWaitCanceled(ctx uintptr, mode int) int
func runtime_pollReset(ctx uintptr, mode int) int
func runtime_pollSetDeadline(ctx uintptr, d int64, mode int)
func runtime_pollUnblock(ctx uintptr)

type pollDesc struct { // 对底层PollDesc结构的封装
	runtimeCtx uintptr // 指向底层PollDesc的指针
}

var serverInit sync.Once // 控制只执行一次

func (pd *pollDesc) Init(fd *netFD) error { // 初始化pollDesc
	serverInit.Do(runtime_pollServerInit)             // 执行一次runtime_pollServerInit
	ctx, errno := runtime_pollOpen(uintptr(fd.sysfd)) // 打开对应的fd，返回底层的PollDesc结构
	if errno != 0 {                                   // 如果返回的errno非零
		return syscall.Errno(errno)
	}
	pd.runtimeCtx = ctx // 设置对底层PollDesc结构的封装
	return nil
}

func (pd *pollDesc) Close() { // 关闭pollDesc
	if pd.runtimeCtx == 0 {
		return
	}
	runtime_pollClose(pd.runtimeCtx) // 关闭runtime的PollDesc
	pd.runtimeCtx = 0
}

// Evict evicts fd from the pending list, unblocking any I/O running on fd.
func (pd *pollDesc) Evict() { // 从pending列表中移除fd
	if pd.runtimeCtx == 0 {
		return
	}
	runtime_pollUnblock(pd.runtimeCtx)
}

func (pd *pollDesc) Prepare(mode int) error { // 按照读写模式进行fd重置
	res := runtime_pollReset(pd.runtimeCtx, mode) // 设置对应的poll模式
	return convertErr(res)                        // 返回错误
}

func (pd *pollDesc) PrepareRead() error { // 准备读
	return pd.Prepare('r')
}

func (pd *pollDesc) PrepareWrite() error { // 准备写
	return pd.Prepare('w')
}

func (pd *pollDesc) Wait(mode int) error {
	res := runtime_pollWait(pd.runtimeCtx, mode) // 调用pollWait等待读写,mode为等待类型，读或写
	return convertErr(res)                       // 返回错误类型
}

func (pd *pollDesc) WaitRead() error { // 等待读
	return pd.Wait('r')
}

func (pd *pollDesc) WaitWrite() error { // 等待写
	return pd.Wait('w')
}

func (pd *pollDesc) WaitCanceled(mode int) { // 调用底层的waitcancel
	runtime_pollWaitCanceled(pd.runtimeCtx, mode)
}

func (pd *pollDesc) WaitCanceledRead() {
	pd.WaitCanceled('r')
}

func (pd *pollDesc) WaitCanceledWrite() {
	pd.WaitCanceled('w')
}

func convertErr(res int) error { // 根据错误类型进行转换，或者无错误，或者已经关闭，或者已经超时
	switch res {
	case 0:
		return nil // 无错误
	case 1:
		return errClosing // 底层PollDesc已经被关闭
	case 2:
		return errTimeout // 超时错误
	}
	println("unreachable: ", res) // 错误号无效panic
	panic("unreachable")
}

func (fd *netFD) setDeadline(t time.Time) error { // 设置读写超时Deadline时间
	return setDeadlineImpl(fd, t, 'r'+'w')
}

func (fd *netFD) setReadDeadline(t time.Time) error { // 设置读超时时间
	return setDeadlineImpl(fd, t, 'r')
}

func (fd *netFD) setWriteDeadline(t time.Time) error { // 设置写超时时间
	return setDeadlineImpl(fd, t, 'w')
}

func setDeadlineImpl(fd *netFD, t time.Time, mode int) error { // 最后都需要调用的超时设置
	d := runtimeNano() + int64(t.Sub(time.Now())) // 获得超时的绝对时间
	if t.IsZero() {
		d = 0
	}
	if err := fd.incref(); err != nil {
		return err
	}
	runtime_pollSetDeadline(fd.pd.runtimeCtx, d, mode) // 设置超时触发器
	fd.decref()
	return nil
}
