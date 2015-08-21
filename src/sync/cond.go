// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sync

import (
	"sync/atomic"
	"unsafe"
)

// Cond implements a condition variable, a rendezvous point
// for goroutines waiting for or announcing the occurrence
// of an event.
//
// Each Cond has an associated Locker L (often a *Mutex or *RWMutex),
// which must be held when changing the condition and
// when calling the Wait method.
//
// A Cond can be created as part of other structures.
// A Cond must not be copied after first use.
type Cond struct { // 条件结构
	// L is held while observing or changing the condition
	L Locker // 对应条件的锁，和该锁一起，构成对资源使用的保护

	sema    syncSema
	waiters uint32      // number of waiters 等待者的数量
	checker copyChecker // 指向checker自身的一个指针，用作判断cond是否被拷贝过
}

// NewCond returns a new Cond with Locker l.
func NewCond(l Locker) *Cond { // 创建新的条件变量，根据Lock
	return &Cond{L: l}
}

// Wait atomically unlocks c.L and suspends execution
// of the calling goroutine.  After later resuming execution,
// Wait locks c.L before returning.  Unlike in other systems,
// Wait cannot return unless awoken by Broadcast or Signal.
//
// Because c.L is not locked when Wait first resumes, the caller
// typically cannot assume that the condition is true when
// Wait returns.  Instead, the caller should Wait in a loop:
//
//    c.L.Lock()
//    for !condition() {
//        c.Wait()
//    }
//    ... make use of condition ...
//    c.L.Unlock()
//
// 释放锁，等待执行，如果条件满足，函数返回时已加上锁
func (c *Cond) Wait() { // 等待条件满足
	c.checker.check() // 检查Cond没有被拷贝
	if raceenabled {
		raceDisable()
	}
	atomic.AddUint32(&c.waiters, 1) // 等待者的数量增加
	if raceenabled {
		raceEnable()
	}
	c.L.Unlock()                    // 先解锁
	runtime_Syncsemacquire(&c.sema) // 等待在sema上
	c.L.Lock()
}

// Signal wakes one goroutine waiting on c, if there is any.
//
// It is allowed but not required for the caller to hold c.L
// during the call.
// 通知条件的一个等待者，条件已满足
func (c *Cond) Signal() {
	c.signalImpl(false)
}

// Broadcast wakes all goroutines waiting on c.
//
// It is allowed but not required for the caller to hold c.L
// during the call.
// 通知条件的所有等待者，条件已满足
func (c *Cond) Broadcast() {
	c.signalImpl(true)
}

func (c *Cond) signalImpl(all bool) { // 通知的具体实现，all表示是否通知所有的等待者
	c.checker.check() // 检查Cond没有被拷贝
	if raceenabled {
		raceDisable()
	}
	for {
		old := atomic.LoadUint32(&c.waiters) // 查看有多少人在等待该条件
		if old == 0 {                        // 如果没人等待，直接返回
			if raceenabled {
				raceEnable()
			}
			return
		}
		new := old - 1
		if all {
			new = 0
		}
		if atomic.CompareAndSwapUint32(&c.waiters, old, new) {
			if raceenabled {
				raceEnable()
			}
			runtime_Syncsemrelease(&c.sema, old-new) // 设置唤醒多少个
			return
		}
	}
}

// copyChecker holds back pointer to itself to detect object copying.
type copyChecker uintptr // copyChecker保存指向自己的指针，用来检测对象拷贝

func (c *copyChecker) check() {
	if uintptr(*c) != uintptr(unsafe.Pointer(c)) &&
		!atomic.CompareAndSwapUintptr((*uintptr)(c), 0, uintptr(unsafe.Pointer(c))) &&
		uintptr(*c) != uintptr(unsafe.Pointer(c)) { // 如果c不指向自身了，或者c本身为空(初始化的情况，这时给c赋值)，
		panic("sync.Cond is copied") // 如果初始化失败，或者不指向自身了，panic
	}
}
