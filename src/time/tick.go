// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package time

import "errors"

// A Ticker holds a channel that delivers `ticks' of a clock
// at intervals.
// Ticker中包含一个channel
type Ticker struct {
	C <-chan Time  // The channel on which the ticks are delivered.
	r runtimeTimer // 内部包含一个runtimeTimer结构
}

// NewTicker returns a new Ticker containing a channel that will send the
// time with a period specified by the duration argument.
// It adjusts the intervals or drops ticks to make up for slow receivers.
// The duration d must be greater than zero; if not, NewTicker will panic.
// Stop the ticker to release associated resources.
func NewTicker(d Duration) *Ticker { // 新创建一个Ticker
	if d <= 0 { // 间隔时间错误，打印错误字符串
		panic(errors.New("non-positive interval for NewTicker"))
	}
	// Give the channel a 1-element time buffer.
	// If the client falls behind while reading, we drop ticks
	// on the floor until the client catches up.
	c := make(chan Time, 1) // 生成Time结构的chan
	t := &Ticker{           // 创建一个Ticker
		C: c, // 创建一个类型为Time的chan
		r: runtimeTimer{ // 初始化runtimeTimer结构
			when:   when(d),
			period: int64(d), // 周期性运行
			f:      sendTime,
			arg:    c,
		},
	}
	startTimer(&t.r) // 启动定时器
	return t         // 返回一个Ticker结构
}

// Stop turns off a ticker.  After Stop, no more ticks will be sent.
// Stop does not close the channel, to prevent a read from the channel succeeding
// incorrectly.
func (t *Ticker) Stop() { // 停止Ticker，只有NewTicker出的Ticker才能被Stop
	stopTimer(&t.r)
}

// Tick is a convenience wrapper for NewTicker providing access to the ticking
// channel only. While Tick is useful for clients that have no need to shut down
// the Ticker, be aware that without a way to shut it down the underlying
// Ticker cannot be recovered by the garbage collector; it "leaks".
func Tick(d Duration) <-chan Time { // 直接返回一个Timer的chan
	if d <= 0 {
		return nil
	}
	return NewTicker(d).C // 返回包装后的Timer chan
}
