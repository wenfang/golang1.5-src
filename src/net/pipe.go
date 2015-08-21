// Copyright 2010 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package net

import (
	"errors"
	"io"
	"time"
)

// Pipe creates a synchronous, in-memory, full duplex
// network connection; both ends implement the Conn interface.
// Reads on one end are matched with writes on the other,
// copying data directly between the two; there is no internal
// buffering.
func Pipe() (Conn, Conn) { // 创建全双工的网络连接，底层是两个io.Pipe
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	return &pipe{r1, w2}, &pipe{r2, w1}
}

type pipe struct { // 封装pipe，使其匹配net.Conn接口
	*io.PipeReader
	*io.PipeWriter
}

type pipeAddr int

func (pipeAddr) Network() string {
	return "pipe"
}

func (pipeAddr) String() string {
	return "pipe"
}

func (p *pipe) Close() error { // 关闭pipe
	err := p.PipeReader.Close()
	err1 := p.PipeWriter.Close()
	if err == nil {
		err = err1
	}
	return err
}

func (p *pipe) LocalAddr() Addr { // 返回本地地址
	return pipeAddr(0)
}

func (p *pipe) RemoteAddr() Addr { // 返回远端地址
	return pipeAddr(0)
}

func (p *pipe) SetDeadline(t time.Time) error {
	return &OpError{Op: "set", Net: "pipe", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}

func (p *pipe) SetReadDeadline(t time.Time) error {
	return &OpError{Op: "set", Net: "pipe", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}

func (p *pipe) SetWriteDeadline(t time.Time) error {
	return &OpError{Op: "set", Net: "pipe", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}
