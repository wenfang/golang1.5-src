// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sync provides basic synchronization primitives such as mutual
// exclusion locks.  Other than the Once and WaitGroup types, most are intended
// for use by low-level library routines.  Higher-level synchronization is
// better done via channels and communication.
//
// Values containing the types defined in this package should not be copied.
package sync

import (
	"sync/atomic"
	"unsafe"
)

// A Mutex is a mutual exclusion lock.
// Mutexes can be created as part of other structures;
// the zero value for a Mutex is an unlocked mutex.
type Mutex struct { //Mutex排他锁
	state int32  // 表明锁的状态信息，最后一位为1表明被锁定，倒数第2为表明是否刚被唤醒，然后前面的位表明等待者的数量
	sema  uint32 // 等待睡眠的sema
}

// A Locker represents an object that can be locked and unlocked.
type Locker interface { // 锁接口，表明一个对象可以被加解锁
	Lock()
	Unlock()
}

const ( // 锁的状态
	mutexLocked      = 1 << iota // mutex is locked 被锁定，第一位表明是否被锁定
	mutexWoken                   // 第二位表明是否刚被唤醒
	mutexWaiterShift = iota      // 等待者起始的位数，第三位
)

// Lock locks m.
// If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (m *Mutex) Lock() { // 排他锁加锁
	// Fast path: grab unlocked mutex.
	if atomic.CompareAndSwapInt32(&m.state, 0, mutexLocked) { // CAS操作加锁，如果原值为0，变为1，表明加锁成功
		if raceenabled {
			raceAcquire(unsafe.Pointer(m))
		}
		return // 加锁成功
	}
	// 如果加锁不成功
	awoke := false // awoke初值为当前的goroutine未被唤醒
	iter := 0      // 迭代次数
	for {
		old := m.state            // 获取Mutex的状态
		new := old | mutexLocked  // 设置新状态置已被Lock标记
		if old&mutexLocked != 0 { // 如果老的已经被标记了
			if runtime_canSpin(iter) {
				// Active spinning makes sense.
				// Try to set mutexWoken flag to inform Unlock
				// to not wake other blocked goroutines.
				if !awoke && old&mutexWoken == 0 && old>>mutexWaiterShift != 0 &&
					atomic.CompareAndSwapInt32(&m.state, old, old|mutexWoken) {
					awoke = true
				}
				runtime_doSpin()
				iter++
				continue
			}
			new = old + 1<<mutexWaiterShift
		}
		if awoke { // 当前的goroutine从睡眠中被唤醒，
			// The goroutine has been woken from sleep,
			// so we need to reset the flag in either case.
			if new&mutexWoken == 0 {
				panic("sync: inconsistent mutex state")
			}
			new &^= mutexWoken // 将Woken位清0
		}
		if atomic.CompareAndSwapInt32(&m.state, old, new) { // 状态未变，将新状态赋给老状态，多了一个等待者，否则继续到for来一遍
			if old&mutexLocked == 0 { // 如果锁被释放了，直接跳出
				break
			}
			runtime_Semacquire(&m.sema) // 当前的goroutine阻塞等待被唤醒
			awoke = true                // 当前的goroutine被唤醒，重新执行一遍for循环
			iter = 0
		}
	}

	if raceenabled { // 竞争条件检查
		raceAcquire(unsafe.Pointer(m))
	}
}

// Unlock unlocks m.
// It is a run-time error if m is not locked on entry to Unlock.
//
// A locked Mutex is not associated with a particular goroutine.
// It is allowed for one goroutine to lock a Mutex and then
// arrange for another goroutine to unlock it.
func (m *Mutex) Unlock() { // 排他锁解锁
	if raceenabled {
		_ = m.state
		raceRelease(unsafe.Pointer(m))
	}

	// Fast path: drop lock bit.
	new := atomic.AddInt32(&m.state, -mutexLocked) // 减去mutexLocked位，相当于解锁
	if (new+mutexLocked)&mutexLocked == 0 {        // 如果重复解锁了，panic
		panic("sync: unlock of unlocked mutex")
	}
	// 下面就有可能在多个goroutine间争用了
	old := new
	for {
		// If there are no waiters or a goroutine has already
		// been woken or grabbed the lock, no need to wake anyone.
		if old>>mutexWaiterShift == 0 || old&(mutexLocked|mutexWoken) != 0 { // 没有人等待，或者有一个已经唤醒了，直接返回
			return
		}
		// Grab the right to wake someone.
		new = (old - 1<<mutexWaiterShift) | mutexWoken      // 设置唤醒一个
		if atomic.CompareAndSwapInt32(&m.state, old, new) { // 尝试唤醒一次
			runtime_Semrelease(&m.sema) // 唤醒等待者
			return
		}
		old = m.state
	}
}
