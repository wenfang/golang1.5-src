// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package net

import "sync/atomic"

// fdMutex is a specialized synchronization primitive
// that manages lifetime of an fd and serializes access
// to Read and Write methods on netFD.
type fdMutex struct { // 对文件的序列化同步原语，管理fd的生命周期并序列化读写方法
	state uint64 // 该mutex的状态
	rsema uint32
	wsema uint32
}

// fdMutex.state is organized as follows:
// 1 bit - whether netFD is closed, if set all subsequent lock operations will fail.
// 1 bit - lock for read operations.
// 1 bit - lock for write operations.
// 20 bits - total number of references (read+write+misc).
// 20 bits - number of outstanding read waiters.
// 20 bits - number of outstanding write waiters.
const (
	mutexClosed  = 1 << 0 // 第一位表明是否netFD被关闭
	mutexRLock   = 1 << 1 // 第二位，读操作的锁
	mutexWLock   = 1 << 2 // 第三位，写操作的锁
	mutexRef     = 1 << 3 // 从第四位开始为引用计数，总共20位
	mutexRefMask = (1<<20 - 1) << 3
	mutexRWait   = 1 << 23
	mutexRMask   = (1<<20 - 1) << 23 // 从第24位开始为等待读者的数量，总共20位
	mutexWWait   = 1 << 43
	mutexWMask   = (1<<20 - 1) << 43 // 从第44为开始为等待写者的数量，总共20位
)

// Read operations must do RWLock(true)/RWUnlock(true).
// Write operations must do RWLock(false)/RWUnlock(false).
// Misc operations must do Incref/Decref. Misc operations include functions like
// setsockopt and setDeadline. They need to use Incref/Decref to ensure that
// they operate on the correct fd in presence of a concurrent Close call
// (otherwise fd can be closed under their feet).
// Close operation must do IncrefAndClose/Decref.

// RWLock/Incref return whether fd is open.
// RWUnlock/Decref return whether fd is closed and there are no remaining references.

func (mu *fdMutex) Incref() bool {
	for {
		old := atomic.LoadUint64(&mu.state) // 原子返回老的state值
		if old&mutexClosed != 0 {           // 如果已经关闭，无法增加引用计数
			return false
		}
		new := old + mutexRef      // 增加引用计数，以mutexRef为单位
		if new&mutexRefMask == 0 { // 不能超过最大的引用计数
			panic("net: inconsistent fdMutex")
		}
		if atomic.CompareAndSwapUint64(&mu.state, old, new) { // 为引用计数原子赋值
			return true
		}
	}
}

func (mu *fdMutex) IncrefAndClose() bool { // 增加引用计数，同时标记fd为关闭状态
	for {
		old := atomic.LoadUint64(&mu.state)
		if old&mutexClosed != 0 {
			return false
		}
		// Mark as closed and acquire a reference.
		new := (old | mutexClosed) + mutexRef
		if new&mutexRefMask == 0 {
			panic("net: inconsistent fdMutex")
		}
		// Remove all read and write waiters.
		new &^= mutexRMask | mutexWMask
		if atomic.CompareAndSwapUint64(&mu.state, old, new) {
			// Wake all read and write waiters,
			// they will observe closed flag after wakeup.
			for old&mutexRMask != 0 {
				old -= mutexRWait
				runtime_Semrelease(&mu.rsema)
			}
			for old&mutexWMask != 0 {
				old -= mutexWWait
				runtime_Semrelease(&mu.wsema)
			}
			return true
		}
	}
}

func (mu *fdMutex) Decref() bool { // 减少引用计数
	for {
		old := atomic.LoadUint64(&mu.state) // 原子获取state值
		if old&mutexRefMask == 0 {
			panic("net: inconsistent fdMutex")
		}
		new := old - mutexRef
		if atomic.CompareAndSwapUint64(&mu.state, old, new) {
			return new&(mutexClosed|mutexRefMask) == mutexClosed
		}
	}
}

func (mu *fdMutex) RWLock(read bool) bool { // 根据read参数，对fd加读锁，或者写锁
	var mutexBit, mutexWait, mutexMask uint64
	var mutexSema *uint32
	if read {
		mutexBit = mutexRLock
		mutexWait = mutexRWait
		mutexMask = mutexRMask
		mutexSema = &mu.rsema
	} else {
		mutexBit = mutexWLock
		mutexWait = mutexWWait
		mutexMask = mutexWMask
		mutexSema = &mu.wsema
	}
	for {
		old := atomic.LoadUint64(&mu.state)
		if old&mutexClosed != 0 {
			return false
		}
		var new uint64
		if old&mutexBit == 0 {
			// Lock is free, acquire it.
			new = (old | mutexBit) + mutexRef
			if new&mutexRefMask == 0 {
				panic("net: inconsistent fdMutex")
			}
		} else {
			// Wait for lock.
			new = old + mutexWait
			if new&mutexMask == 0 {
				panic("net: inconsistent fdMutex")
			}
		}
		if atomic.CompareAndSwapUint64(&mu.state, old, new) {
			if old&mutexBit == 0 {
				return true
			}
			runtime_Semacquire(mutexSema)
			// The signaller has subtracted mutexWait.
		}
	}
}

func (mu *fdMutex) RWUnlock(read bool) bool { // 根据read参数，对fd解读锁或者写锁
	var mutexBit, mutexWait, mutexMask uint64
	var mutexSema *uint32
	if read {
		mutexBit = mutexRLock
		mutexWait = mutexRWait
		mutexMask = mutexRMask
		mutexSema = &mu.rsema
	} else {
		mutexBit = mutexWLock
		mutexWait = mutexWWait
		mutexMask = mutexWMask
		mutexSema = &mu.wsema
	}
	for {
		old := atomic.LoadUint64(&mu.state)
		if old&mutexBit == 0 || old&mutexRefMask == 0 {
			panic("net: inconsistent fdMutex")
		}
		// Drop lock, drop reference and wake read waiter if present.
		new := (old &^ mutexBit) - mutexRef
		if old&mutexMask != 0 {
			new -= mutexWait
		}
		if atomic.CompareAndSwapUint64(&mu.state, old, new) {
			if old&mutexMask != 0 {
				runtime_Semrelease(mutexSema)
			}
			return new&(mutexClosed|mutexRefMask) == mutexClosed
		}
	}
}

// Implemented in runtime package.
func runtime_Semacquire(sema *uint32)
func runtime_Semrelease(sema *uint32)
