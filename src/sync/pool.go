// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sync

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// A Pool is a set of temporary objects that may be individually saved and
// retrieved.
//
// Any item stored in the Pool may be removed automatically at any time without
// notification. If the Pool holds the only reference when this happens, the
// item might be deallocated.
//
// A Pool is safe for use by multiple goroutines simultaneously.
//
// Pool's purpose is to cache allocated but unused items for later reuse,
// relieving pressure on the garbage collector. That is, it makes it easy to
// build efficient, thread-safe free lists. However, it is not suitable for all
// free lists.
//
// An appropriate use of a Pool is to manage a group of temporary items
// silently shared among and potentially reused by concurrent independent
// clients of a package. Pool provides a way to amortize allocation overhead
// across many clients.
//
// An example of good use of a Pool is in the fmt package, which maintains a
// dynamically-sized store of temporary output buffers. The store scales under
// load (when many goroutines are actively printing) and shrinks when
// quiescent.
//
// On the other hand, a free list maintained as part of a short-lived object is
// not a suitable use for a Pool, since the overhead does not amortize well in
// that scenario. It is more efficient to have such objects implement their own
// free list.
//
type Pool struct {
	local     unsafe.Pointer // local fixed-size per-P pool, actual type is [P]poolLocal 真实类型为poolLocal的数组
	localSize uintptr        // size of the local array local array的大小

	// New optionally specifies a function to generate
	// a value when Get would otherwise return nil.
	// It may not be changed concurrently with calls to Get.
	New func() interface{} // 当调用Get从Pool中获取不到时，调用New生成一个新对象
}

// Local per-P Pool appendix.
type poolLocal struct { // 对应每个P的poolLocal，每个Pool对应每个P都有一个poolLocal
	private interface{}   // Can be used only by the respective P. // 只能被当前的P使用
	shared  []interface{} // Can be used by any P. // 可以被任何的P使用
	Mutex                 // Protects shared. // 保护共享数据
	pad     [128]byte     // Prevents false sharing.
}

// Put adds x to the pool.
func (p *Pool) Put(x interface{}) { // 将对象加入pool中
	if raceenabled {
		// Under race detector the Pool degenerates into no-op.
		// It's conforming, simple and does not introduce excessive
		// happens-before edges between unrelated goroutines.
		return
	}
	if x == nil { // 要加入的对象为空，直接返回
		return
	}
	l := p.pin()
	if l.private == nil { // 先尝试加入private部分
		l.private = x
		x = nil
	}
	runtime_procUnpin()
	if x == nil {
		return
	}
	l.Lock()
	l.shared = append(l.shared, x) // 尝试加入shared部分
	l.Unlock()
}

// Get selects an arbitrary item from the Pool, removes it from the
// Pool, and returns it to the caller.
// Get may choose to ignore the pool and treat it as empty.
// Callers should not assume any relation between values passed to Put and
// the values returned by Get.
//
// If Get would otherwise return nil and p.New is non-nil, Get returns
// the result of calling p.New.
func (p *Pool) Get() interface{} { // 从Pool中获取一个元素
	if raceenabled { // 在raceenabled条件下，只使用New构造
		if p.New != nil {
			return p.New()
		}
		return nil
	}
	l := p.pin()        // 获得对应的poolLocal
	x := l.private      // 获得poolLocal中的private部分
	l.private = nil     // 取出来private了，将原private变为空
	runtime_procUnpin() // P解锁，可以调度了
	if x != nil {       // 如果获得了元素，返回
		return x
	}
	l.Lock()                  // private部分没有了，从shared部分获取，需要加锁
	last := len(l.shared) - 1 // 如果没有private从共享部分获得元素
	if last >= 0 {
		x = l.shared[last]
		l.shared = l.shared[:last]
	}
	l.Unlock()
	if x != nil {
		return x
	}
	return p.getSlow()
}

func (p *Pool) getSlow() (x interface{}) {
	// See the comment in pin regarding ordering of the loads.
	size := atomic.LoadUintptr(&p.localSize) // load-acquire
	local := p.local                         // load-consume
	// Try to steal one element from other procs.
	pid := runtime_procPin()
	runtime_procUnpin()
	for i := 0; i < int(size); i++ { // 从该Pool所有poolLocal的共享部分选择
		l := indexLocal(local, (pid+i+1)%int(size))
		l.Lock()
		last := len(l.shared) - 1
		if last >= 0 {
			x = l.shared[last]
			l.shared = l.shared[:last]
			l.Unlock()
			break
		}
		l.Unlock()
	}

	if x == nil && p.New != nil { // 如果没有从Pool中找到，调用New生成一个
		x = p.New() // 最后调用New生成一个元素
	}
	return x
}

// pin pins the current goroutine to P, disables preemption and returns poolLocal pool for the P.
// Caller must call runtime_procUnpin() when done with the pool.
func (p *Pool) pin() *poolLocal { // 获取特定于P的pool
	pid := runtime_procPin() // 获得当前P的id
	// In pinSlow we store to localSize and then to local, here we load in opposite order.
	// Since we've disabled preemption, GC can not happen in between.
	// Thus here we must observe local at least as large localSize.
	// We can observe a newer/larger local, it is fine (we must observe its zero-initialized-ness).
	s := atomic.LoadUintptr(&p.localSize) // load-acquire 获得pool的本地大小
	l := p.local                          // load-consume
	if uintptr(pid) < s {                 // 如果pid小于localSize的大小，表明P的数量无变化，直接取出poolLocal
		return indexLocal(l, pid) // 返回对应pid的poolLocal
	}
	return p.pinSlow() // 如果获得的pid大于localSize，表明P的大小变化了，使用pinSlow获得poolLocal
}

func (p *Pool) pinSlow() *poolLocal {
	// Retry under the mutex.
	// Can not lock the mutex while pinned.
	runtime_procUnpin() // 在allPoolsMu加锁的情况下查找，这时候必须unpin
	allPoolsMu.Lock()
	defer allPoolsMu.Unlock() // 在allPoolsMu的保护下执行
	pid := runtime_procPin()  // 再次获得P的id
	// poolCleanup won't be called while we are pinned.
	s := p.localSize
	l := p.local
	if uintptr(pid) < s { // 尝试获取poolLocal
		return indexLocal(l, pid)
	}
	if p.local == nil { // 获取失败，表明是第一次，将当前的Pool加入到allPools中
		allPools = append(allPools, p)
	}
	// If GOMAXPROCS changes between GCs, we re-allocate the array and lose the old one.
	size := runtime.GOMAXPROCS(0)                                               // 如果到这里，表明P的数量发生变化了，丢弃以前的poolLocal
	local := make([]poolLocal, size)                                            // 分配procs个poolLocal
	atomic.StorePointer((*unsafe.Pointer)(&p.local), unsafe.Pointer(&local[0])) // store-release 存储poolLocal
	atomic.StoreUintptr(&p.localSize, uintptr(size))                            // store-release 存储大小
	return &local[pid]                                                          // 返回对应P的poolLocal指针
}

func poolCleanup() {
	// This function is called with the world stopped, at the beginning of a garbage collection.
	// It must not allocate and probably should not call any runtime functions.
	// Defensively zero out everything, 2 reasons:
	// 1. To prevent false retention of whole Pools.
	// 2. If GC happens while a goroutine works with l.shared in Put/Get,
	//    it will retain whole Pool. So next cycle memory consumption would be doubled.
	for i, p := range allPools { // 遍历所有的Pool
		allPools[i] = nil
		for i := 0; i < int(p.localSize); i++ {
			l := indexLocal(p.local, i)
			l.private = nil
			for j := range l.shared {
				l.shared[j] = nil
			}
			l.shared = nil
		}
		p.local = nil
		p.localSize = 0
	}
	allPools = []*Pool{}
}

var (
	allPoolsMu Mutex
	allPools   []*Pool
)

func init() {
	runtime_registerPoolCleanup(poolCleanup) // 注册pool的cleanup函数
}

func indexLocal(l unsafe.Pointer, i int) *poolLocal { // 将l转变为poolLocal类型，并获得索引i位置的值
	return &(*[1000000]poolLocal)(l)[i]
}

// Implemented in runtime.
func runtime_registerPoolCleanup(cleanup func())
func runtime_procPin() int
func runtime_procUnpin()
