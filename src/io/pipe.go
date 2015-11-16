// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Pipe adapter to connect code expecting an io.Reader
// with code expecting an io.Writer.

package io

import (
	"errors"
	"sync"
)

// ErrClosedPipe is the error used for read or write operations on a closed pipe.
var ErrClosedPipe = errors.New("io: read/write on closed pipe")

type pipeResult struct {
	n   int
	err error
}

// A pipe is the shared pipe structure underlying PipeReader and PipeWriter.
type pipe struct { // pipe结构
	rl    sync.Mutex // gates readers one at a time 保证读互斥
	wl    sync.Mutex // gates writers one at a time 保证写互斥
	l     sync.Mutex // protects remaining fields 保护下面的域
	data  []byte     // data remaining in pending write 数据区
	rwait sync.Cond  // waiting reader
	wwait sync.Cond  // waiting writer
	rerr  error      // if reader closed, error to give writes 读者错误
	werr  error      // if writer closed, error to give reads 如果写者关闭，返回给读者的错误
}

func (p *pipe) read(b []byte) (n int, err error) { // 处理管道的读
	// One reader at a time.
	p.rl.Lock()
	defer p.rl.Unlock() // 读互斥加锁

	p.l.Lock()
	defer p.l.Unlock()
	for {
		if p.rerr != nil { // 如果读者错误，返回pipe已经被关闭
			return 0, ErrClosedPipe
		}
		if p.data != nil { // 如果有data数据，跳出循环
			break
		}
		if p.werr != nil {
			return 0, p.werr
		}
		p.rwait.Wait() // 如果当前没有data数据，开始等待写者
	}
	n = copy(b, p.data)   // 将p.data中的数据拷贝到b中，n为拷贝的数据长度
	p.data = p.data[n:]   // 截断数据
	if len(p.data) == 0 { // 如果数据已经被读完了
		p.data = nil
		p.wwait.Signal() // 唤醒写者
	}
	return
}

var zero [0]byte

func (p *pipe) write(b []byte) (n int, err error) {
	// pipe uses nil to mean not available
	if b == nil {
		b = zero[:]
	}

	// One writer at a time.
	p.wl.Lock()
	defer p.wl.Unlock()

	p.l.Lock()
	defer p.l.Unlock()
	if p.werr != nil {
		err = ErrClosedPipe
		return
	}
	p.data = b
	p.rwait.Signal()
	for {
		if p.data == nil {
			break
		}
		if p.rerr != nil {
			err = p.rerr
			break
		}
		if p.werr != nil {
			err = ErrClosedPipe
		}
		p.wwait.Wait()
	}
	n = len(b) - len(p.data)
	p.data = nil // in case of rerr or werr
	return
}

func (p *pipe) rclose(err error) {
	if err == nil {
		err = ErrClosedPipe
	}
	p.l.Lock()
	defer p.l.Unlock()
	p.rerr = err
	p.rwait.Signal()
	p.wwait.Signal()
}

func (p *pipe) wclose(err error) {
	if err == nil {
		err = EOF
	}
	p.l.Lock()
	defer p.l.Unlock()
	p.werr = err
	p.rwait.Signal()
	p.wwait.Signal()
}

// A PipeReader is the read half of a pipe.
type PipeReader struct { // Pipe的读端
	p *pipe
}

// Read implements the standard Read interface:
// it reads data from the pipe, blocking until a writer
// arrives or the write end is closed.
// If the write end is closed with an error, that error is
// returned as err; otherwise err is EOF.
func (r *PipeReader) Read(data []byte) (n int, err error) {
	return r.p.read(data)
}

// Close closes the reader; subsequent writes to the
// write half of the pipe will return the error ErrClosedPipe.
func (r *PipeReader) Close() error {
	return r.CloseWithError(nil)
}

// CloseWithError closes the reader; subsequent writes
// to the write half of the pipe will return the error err.
func (r *PipeReader) CloseWithError(err error) error { // PipeReader关闭，带有错误err
	r.p.rclose(err)
	return nil
}

// A PipeWriter is the write half of a pipe.
type PipeWriter struct { // Pipe的写端
	p *pipe
}

// Write implements the standard Write interface:
// it writes data to the pipe, blocking until readers
// have consumed all the data or the read end is closed.
// If the read end is closed with an error, that err is
// returned as err; otherwise err is ErrClosedPipe.
func (w *PipeWriter) Write(data []byte) (n int, err error) {
	return w.p.write(data)
}

// Close closes the writer; subsequent reads from the
// read half of the pipe will return no bytes and EOF.
func (w *PipeWriter) Close() error {
	return w.CloseWithError(nil)
}

// CloseWithError closes the writer; subsequent reads from the
// read half of the pipe will return no bytes and the error err,
// or EOF if err is nil.
//
// CloseWithError always returns nil.
func (w *PipeWriter) CloseWithError(err error) error {
	w.p.wclose(err)
	return nil
}

// Pipe创建一个同步的在内存中的pipe
// Pipe creates a synchronous in-memory pipe.
// It can be used to connect code expecting an io.Reader
// with code expecting an io.Writer.
// Reads on one end are matched with writes on the other,
// copying data directly between the two; there is no internal buffering.
// It is safe to call Read and Write in parallel with each other or with
// Close. Close will complete once pending I/O is done. Parallel calls to
// Read, and parallel calls to Write, are also safe:
// the individual calls will be gated sequentially.
func Pipe() (*PipeReader, *PipeWriter) { // 创建一个pipe，返回一个PipeReader和一个PipeWriter
	p := new(pipe) // 创建一个新的pipe结构
	p.rwait.L = &p.l
	p.wwait.L = &p.l
	r := &PipeReader{p}
	w := &PipeWriter{p}
	return r, w
}
