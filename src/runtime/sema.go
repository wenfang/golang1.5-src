// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Semaphore implementation exposed to Go.
// Intended use is provide a sleep and wakeup
// primitive that can be used in the contended case
// of other synchronization primitives.
// Thus it targets the same goal as Linux's futex,
// but it has much simpler semantics.
//
// That is, don't think of these as semaphores.
// Think of them as a way to implement sleep and wakeup
// such that every sleep is paired with a single wakeup,
// even if, due to races, the wakeup happens before the sleep.
//
// See Mullender and Cox, ``Semaphores in Plan 9,''
// http://swtch.com/semaphore.pdf

package runtime

import "unsafe"

// Asynchronous semaphore for sync.Mutex.

type semaRoot struct { // 异步信号量结构，等待在一个地址上的goroutine列表
	lock  mutex
	head  *sudog
	tail  *sudog // 等待的goroutine队列
	nwait uint32 // Number of waiters. Read w/o the lock. 等待者的数量
}

// Prime to not correlate with any user patterns.
const semTabSize = 251

var semtable [semTabSize]struct { // 一个251项的结构数组，包含semaRoot结构变量
	root semaRoot // 总共有251个semaRoot
	pad  [_CacheLineSize - unsafe.Sizeof(semaRoot{})]byte
}

//go:linkname sync_runtime_Semacquire sync.runtime_Semacquire
func sync_runtime_Semacquire(addr *uint32) {
	semacquire(addr, true)
}

//go:linkname net_runtime_Semacquire net.runtime_Semacquire
func net_runtime_Semacquire(addr *uint32) {
	semacquire(addr, true)
}

//go:linkname sync_runtime_Semrelease sync.runtime_Semrelease
func sync_runtime_Semrelease(addr *uint32) {
	semrelease(addr)
}

//go:linkname net_runtime_Semrelease net.runtime_Semrelease
func net_runtime_Semrelease(addr *uint32) {
	semrelease(addr)
}

// Called from runtime.
func semacquire(addr *uint32, profile bool) { // profile表明是否进行性能profile
	gp := getg()         // 获取当前的goroutine
	if gp != gp.m.curg { // 如果没有在当前m的当前的G上执行，抛出异常
		throw("semacquire not on the G stack")
	}

	// Easy case.
	if cansemacquire(addr) { // 快速获取资源，成功后返回
		return
	}

	// Harder case:
	//	increment waiter count
	//	try cansemacquire one more time, return if succeeded
	//	enqueue itself as a waiter
	//	sleep
	//	(waiter descriptor is dequeued by signaler)
	s := acquireSudog()   // 获取一个Sudog结构
	root := semroot(addr) // 根据地址获取对应的semroot结构
	t0 := int64(0)        // 设置t0初值为0
	s.releasetime = 0     // 设置释放时间
	if profile && blockprofilerate > 0 {
		t0 = cputicks()
		s.releasetime = -1
	}
	for { // 循环进行加锁
		lock(&root.lock) // 对获取的semaroot进行加锁
		// Add ourselves to nwait to disable "easy case" in semrelease.
		xadd(&root.nwait, 1) // 增加等待者数量
		// Check cansemacquire to avoid missed wakeup.
		if cansemacquire(addr) { // 如果能够获得资源了，减去等待者数量，返回
			xadd(&root.nwait, -1)
			unlock(&root.lock)
			break
		}
		// Any semrelease after the cansemacquire knows we're waiting
		// (we set nwait above), so go to sleep.
		root.queue(addr, s)                    // 加入addr加入到队列s中
		goparkunlock(&root.lock, "semacquire", traceEvGoBlockSync, 4)
		if cansemacquire(addr) {
			break
		}
	}
	if s.releasetime > 0 {
		blockevent(int64(s.releasetime)-t0, 3)
	}
	releaseSudog(s)
}

func semrelease(addr *uint32) {
	root := semroot(addr)
	xadd(addr, 1)

	// Easy case: no waiters?
	// This check must happen after the xadd, to avoid a missed wakeup
	// (see loop in semacquire).
	if atomicload(&root.nwait) == 0 {
		return
	}

	// Harder case: search for a waiter and wake it.
	lock(&root.lock)
	if atomicload(&root.nwait) == 0 {
		// The count is already consumed by another goroutine,
		// so no need to wake up another goroutine.
		unlock(&root.lock)
		return
	}
	s := root.head
	for ; s != nil; s = s.next {
		if s.elem == unsafe.Pointer(addr) {
			xadd(&root.nwait, -1)
			root.dequeue(s)
			break
		}
	}
	unlock(&root.lock)
	if s != nil {
		if s.releasetime != 0 {
			s.releasetime = cputicks()
		}
		goready(s.g, 4)
	}
}

func semroot(addr *uint32) *semaRoot { // 返回对应addr的semaRoot结构
	return &semtable[(uintptr(unsafe.Pointer(addr))>>3)%semTabSize].root
}

func cansemacquire(addr *uint32) bool { // 获取addr位置的资源,addr位置为一个值，当为0时表明资源已被获得
	for {
		v := atomicload(addr) // 获得addr位置的值
		if v == 0 {           // 如果当前值为0，不能获取资源了
			return false
		}
		if cas(addr, v, v-1) { // 将当前的值减1，获取资源成功
			return true
		}
	}
}

func (root *semaRoot) queue(addr *uint32, s *sudog) {
	s.g = getg()                  // 获取当前的goroutine结构，放入g中
	s.elem = unsafe.Pointer(addr) // 设置元素，为sema的地址
	s.next = nil
	s.prev = root.tail // 将sudog放入到semaRoot尾部
	if root.tail != nil {
		root.tail.next = s
	} else {
		root.head = s
	}
	root.tail = s
}

func (root *semaRoot) dequeue(s *sudog) { // 将sudog从semaRoot队列中移除
	if s.next != nil {
		s.next.prev = s.prev
	} else {
		root.tail = s.prev
	}
	if s.prev != nil {
		s.prev.next = s.next
	} else {
		root.head = s.next
	}
	s.elem = nil
	s.next = nil
	s.prev = nil
}

// Synchronous semaphore for sync.Cond.
type syncSema struct {
	lock mutex
	head *sudog
	tail *sudog
}

// syncsemacquire waits for a pairing syncsemrelease on the same semaphore s.
//go:linkname syncsemacquire sync.runtime_Syncsemacquire
func syncsemacquire(s *syncSema) {
	lock(&s.lock)
	if s.head != nil && s.head.nrelease > 0 {
		// Have pending release, consume it.
		var wake *sudog
		s.head.nrelease--
		if s.head.nrelease == 0 {
			wake = s.head
			s.head = wake.next
			if s.head == nil {
				s.tail = nil
			}
		}
		unlock(&s.lock)
		if wake != nil {
			wake.next = nil
			goready(wake.g, 4)
		}
	} else {
		// Enqueue itself.
		w := acquireSudog()
		w.g = getg()
		w.nrelease = -1
		w.next = nil
		w.releasetime = 0
		t0 := int64(0)
		if blockprofilerate > 0 {
			t0 = cputicks()
			w.releasetime = -1
		}
		if s.tail == nil {
			s.head = w
		} else {
			s.tail.next = w
		}
		s.tail = w
		goparkunlock(&s.lock, "semacquire", traceEvGoBlockCond, 3)
		if t0 != 0 {
			blockevent(int64(w.releasetime)-t0, 2)
		}
		releaseSudog(w)
	}
}

// syncsemrelease waits for n pairing syncsemacquire on the same semaphore s.
//go:linkname syncsemrelease sync.runtime_Syncsemrelease
func syncsemrelease(s *syncSema, n uint32) {
	lock(&s.lock)
	for n > 0 && s.head != nil && s.head.nrelease < 0 {
		// Have pending acquire, satisfy it.
		wake := s.head
		s.head = wake.next
		if s.head == nil {
			s.tail = nil
		}
		if wake.releasetime != 0 {
			wake.releasetime = cputicks()
		}
		wake.next = nil
		goready(wake.g, 4)
		n--
	}
	if n > 0 {
		// enqueue itself
		w := acquireSudog()
		w.g = getg()
		w.nrelease = int32(n)
		w.next = nil
		w.releasetime = 0
		if s.tail == nil {
			s.head = w
		} else {
			s.tail.next = w
		}
		s.tail = w
		goparkunlock(&s.lock, "semarelease", traceEvGoBlockCond, 3)
		releaseSudog(w)
	} else {
		unlock(&s.lock)
	}
}

//go:linkname syncsemcheck sync.runtime_Syncsemcheck
func syncsemcheck(sz uintptr) {
	if sz != unsafe.Sizeof(syncSema{}) {
		print("runtime: bad syncSema size - sync=", sz, " runtime=", unsafe.Sizeof(syncSema{}), "\n")
		throw("bad syncSema size")
	}
}
