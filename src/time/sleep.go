// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package time

// Sleep pauses the current goroutine for at least the duration d.
// A negative or zero duration causes Sleep to return immediately.
func Sleep(d Duration) // 睡眠指定的时间

// runtimeNano returns the current value of the runtime clock in nanoseconds.
func runtimeNano() int64 // 以纳秒的形式返回当前的运行时时钟

// Interface to timers implemented in package runtime.
// Must be in sync with ../runtime/runtime.h:/^struct.Timer$
type runtimeTimer struct { // 创建一个运行时timer结构，和runtime中的Timer结构相同
	i      int
	when   int64                      // timer何时到期
	period int64                      // 间隔多长时间，周期性
	f      func(interface{}, uintptr) // NOTE: must not be closure
	arg    interface{}                // 到期执行函数的参数
	seq    uintptr
}

// when is a helper function for setting the 'when' field of a runtimeTimer.
// It returns what the time will be, in nanoseconds, Duration d in the future.
// If d is negative, it is ignored.  If the returned value would be less than
// zero because of an overflow, MaxInt64 is returned.
func when(d Duration) int64 { // 返回d时间以后对应的纳秒时间
	if d <= 0 { // 如果d小于等于0，返回当前时间
		return runtimeNano()
	}
	t := runtimeNano() + int64(d)
	if t < 0 {
		t = 1<<63 - 1 // math.MaxInt64
	}
	return t
}

func startTimer(*runtimeTimer)
func stopTimer(*runtimeTimer) bool

// The Timer type represents a single event.
// When the Timer expires, the current time will be sent on C,
// unless the Timer was created by AfterFunc.
// A Timer must be created with NewTimer or AfterFunc.
type Timer struct { // 定时器结构
	C <-chan Time  // 发送通知的chan，发送的内容为Time
	r runtimeTimer // 包含一个runtimeTimer结构
}

// Stop prevents the Timer from firing.
// It returns true if the call stops the timer, false if the timer has already
// expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding
// incorrectly.
func (t *Timer) Stop() bool {
	if t.r.f == nil {
		panic("time: Stop called on uninitialized Timer")
	}
	return stopTimer(&t.r)
}

// NewTimer creates a new Timer that will send
// the current time on its channel after at least duration d.
func NewTimer(d Duration) *Timer { // 创建一个新的定时器，到期后向chan发送时间，只执行一次
	c := make(chan Time, 1) // 创建chan
	t := &Timer{            // 创建Timer
		C: c,
		r: runtimeTimer{
			when: when(d),  // 何时到期
			f:    sendTime, // 到期后执行的函数，到期后向chan中发送当前时间
			arg:  c,        // 执行函数的参数
		},
	}
	startTimer(&t.r) // 加入定时器
	return t
}

// Reset changes the timer to expire after duration d.
// It returns true if the timer had been active, false if the timer had
// expired or been stopped.
func (t *Timer) Reset(d Duration) bool { // 重置定时器，d时间后生效
	if t.r.f == nil {
		panic("time: Reset called on uninitialized Timer")
	}
	w := when(d)
	active := stopTimer(&t.r)
	t.r.when = w // 设置新时间
	startTimer(&t.r)
	return active
}

func sendTime(c interface{}, seq uintptr) {
	// Non-blocking send of time on c.
	// Used in NewTimer, it cannot block anyway (buffer).
	// Used in NewTicker, dropping sends on the floor is
	// the desired behavior when the reader gets behind,
	// because the sends are periodic.
	select {
	case c.(chan Time) <- Now(): // 将当前的时间发送给chan，如果阻塞则不发送
	default:
	}
}

// After waits for the duration to elapse and then sends the current time
// on the returned channel.
// It is equivalent to NewTimer(d).C.
func After(d Duration) <-chan Time { // 新创建一个定时器，返回其chan，等待d时间后发送当前时间到chan中
	return NewTimer(d).C // 新建一个timer，返回chan
}

// AfterFunc waits for the duration to elapse and then calls f
// in its own goroutine. It returns a Timer that can
// be used to cancel the call using its Stop method.
func AfterFunc(d Duration, f func()) *Timer { // 到时间后执行函数f
	t := &Timer{ // 新创建一个Timer
		r: runtimeTimer{
			when: when(d),
			f:    goFunc, // 到时后执行goFunc，参数为f
			arg:  f,
		},
	}
	startTimer(&t.r) // 执行该Timer
	return t
}

func goFunc(arg interface{}, seq uintptr) {
	go arg.(func())()
}
