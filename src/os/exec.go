// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package os

import (
	"runtime"
	"sync/atomic"
	"syscall"
)

// Process stores the information about a process created by StartProcess.
type Process struct { // 进程结构
	Pid    int     // 进程pid
	handle uintptr // handle is accessed atomically on Windows
	isdone uint32  // process has been successfully waited on, non zero if true
}

func newProcess(pid int, handle uintptr) *Process { // 新创建一个进程结构
	p := &Process{Pid: pid, handle: handle}
	runtime.SetFinalizer(p, (*Process).Release)
	return p
}

func (p *Process) setDone() { // 设置process已经执行完成
	atomic.StoreUint32(&p.isdone, 1)
}

func (p *Process) done() bool { // 查看process是否执行完成
	return atomic.LoadUint32(&p.isdone) > 0
}

// ProcAttr holds the attributes that will be applied to a new process
// started by StartProcess.
type ProcAttr struct { // 进程属性
	// If Dir is non-empty, the child changes into the directory before
	// creating the process.
	Dir string // 如果目录非空，进程在启动前进入到该目录
	// If Env is non-nil, it gives the environment variables for the
	// new process in the form returned by Environ.
	// If it is nil, the result of Environ will be used.
	Env []string // 给定进程的环境变量
	// Files specifies the open files inherited by the new process.  The
	// first three entries correspond to standard input, standard output, and
	// standard error.  An implementation may support additional entries,
	// depending on the underlying operating system.  A nil entry corresponds
	// to that file being closed when the process starts.
	Files []*File // 被新进程继承的打开文件列表，前三项对应标准输入，标准输出和标准错误

	// Operating system-specific process creation attributes.
	// Note that setting this field means that your program
	// may not execute properly or even compile on some
	// operating systems.
	Sys *syscall.SysProcAttr // 操作系统特定的进程创建属性
}

// A Signal represents an operating system signal.
// The usual underlying implementation is operating system-dependent:
// on Unix it is syscall.Signal.
type Signal interface { // 代表操作系统的信号
	String() string
	Signal() // to distinguish from other Stringers
}

// Getpid returns the process id of the caller.
func Getpid() int { return syscall.Getpid() } //返回进程id

// Getppid returns the process id of the caller's parent.
func Getppid() int { return syscall.Getppid() } // 返回进程parent id
