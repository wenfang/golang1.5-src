// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Garbage collector: sweeping

package runtime

import "unsafe"

var sweep sweepdata // 全局的sweep结构

// State of background sweep.
type sweepdata struct { // 后台的sweep的状态
	lock    mutex
	g       *g   // 执行sweep的goroutine
	parked  bool // 是否进入休眠状态
	started bool

	spanidx uint32 // background sweeper position 已经处理到的span的索引

	nbgsweep    uint32
	npausesweep uint32
}

//go:nowritebarrier
func finishsweep_m() { // 在world停止后，完成sweep
	// The world is stopped so we should be able to complete the sweeps
	// quickly.
	for sweepone() != ^uintptr(0) { // 持续执行sweepone
		sweep.npausesweep++
	}

	// There may be some other spans being swept concurrently that
	// we need to wait for. If finishsweep_m is done with the world stopped
	// this code is not required.
	sg := mheap_.sweepgen
	for _, s := range work.spans {
		if s.sweepgen != sg && s.state == _MSpanInUse {
			mSpan_EnsureSwept(s)
		}
	}
}

func bgsweep(c chan int) { // 启动bgsweep goroutine执行
	sweep.g = getg() // 获得当前的goroutine指针，为sweep.g赋值，也就是bgsweep自己运行的goroutine

	lock(&sweep.lock)
	sweep.parked = true                                           // 先将使sweep进入parked休眠状态
	c <- 1                                                        // 赋值完成，下面可以继续执行
	goparkunlock(&sweep.lock, "GC sweep wait", traceEvGoBlock, 1) // bgsweep groutine进入休眠状态

	for {
		for gosweepone() != ^uintptr(0) {
			sweep.nbgsweep++
			Gosched()
		}
		lock(&sweep.lock)
		if !gosweepdone() {
			// This can happen if a GC runs between
			// gosweepone returning ^0 above
			// and the lock being acquired.
			unlock(&sweep.lock)
			continue
		}
		sweep.parked = true
		goparkunlock(&sweep.lock, "GC sweep wait", traceEvGoBlock, 1)
	}
}

// sweep一个span，返回归还到heap的页面的数量，如果当前没有要sweep的span，返回0的按位非值
// sweeps one span
// returns number of pages returned to heap, or ^uintptr(0) if there is nothing to sweep
//go:nowritebarrier
func sweepone() uintptr {
	_g_ := getg() // 获得当前的goroutine结构指针

	// increment locks to ensure that the goroutine is not preempted
	// in the middle of sweep thus leaving the span in an inconsistent state for next GC
	_g_.m.locks++         // 保证该goroutine不会被抢占
	sg := mheap_.sweepgen // 获得堆的sweep代数
	for {
		idx := xadd(&sweep.spanidx, 1) - 1  // 获得spanidx索引，上次处理的span
		if idx >= uint32(len(work.spans)) { // 如果spanidx的索引大于spans列表的长度
			mheap_.sweepdone = 1 // 一次sweep已经完成
			_g_.m.locks--
			return ^uintptr(0) // 已经完成了一次sweep
		}
		s := work.spans[idx]       // 获取索引对应的mspan
		if s.state != mSpanInUse { // 如果mspan的状态不是正在使用
			s.sweepgen = sg // 将mspan的代数设置为堆得代数
			continue        // 查看下一个
		}
		if s.sweepgen != sg-2 || !cas(&s.sweepgen, sg-2, sg-1) { // 如果当前由人正在sweep该mspan，查看下一个
			continue
		}
		npages := s.npages          // 取出mspan的页面大小
		if !mSpan_Sweep(s, false) { // 执行mSpan_Sweep执行sweep
			npages = 0
		}
		_g_.m.locks--
		return npages
	}
}

//go:nowritebarrier
func gosweepone() uintptr { // 在系统栈上执行一次sweepone
	var ret uintptr
	systemstack(func() { // 在系统栈上执行sweepone，返回ret
		ret = sweepone()
	})
	return ret
}

//go:nowritebarrier
func gosweepdone() bool { // 判断sweep是否结束
	return mheap_.sweepdone != 0
}

// Returns only when span s has been swept.
//go:nowritebarrier
func mSpan_EnsureSwept(s *mspan) {
	// Caller must disable preemption.
	// Otherwise when this function returns the span can become unswept again
	// (if GC is triggered on another goroutine).
	_g_ := getg()
	if _g_.m.locks == 0 && _g_.m.mallocing == 0 && _g_ != _g_.m.g0 {
		throw("MSpan_EnsureSwept: m is not locked")
	}

	sg := mheap_.sweepgen
	if atomicload(&s.sweepgen) == sg {
		return
	}
	// The caller must be sure that the span is a MSpanInUse span.
	if cas(&s.sweepgen, sg-2, sg-1) {
		mSpan_Sweep(s, false)
		return
	}
	// unfortunate condition, and we don't have efficient means to wait
	for atomicload(&s.sweepgen) != sg {
		osyield()
	}
}

// 释放mark阶段没有被mark的块释放，或者收集finalizer，为下一次gc周期清除mark标记。
// 如果Span被归还到了mheap中，返回true，如果preserve为true，不将Span返回到heap中
// 也不将Span重新加入MCentral列表中，由调用者负责处理
// Sweep frees or collects finalizers for blocks not marked in the mark phase.
// It clears the mark bits in preparation for the next GC round.
// Returns true if the span was returned to heap.
// If preserve=true, don't return it to heap nor relink in MCentral lists;
// caller takes care of it.
//TODO go:nowritebarrier
func mSpan_Sweep(s *mspan, preserve bool) bool {
	// It's critical that we enter this function with preemption disabled,
	// GC must not start while we are in the middle of this function.
	_g_ := getg()                                                    // 返回当前的goroutine
	if _g_.m.locks == 0 && _g_.m.mallocing == 0 && _g_ != _g_.m.g0 { // 如果m没有被锁定，抛出异常
		throw("MSpan_Sweep: m is not locked")
	}
	sweepgen := mheap_.sweepgen                            // 获得heap的代数
	if s.state != mSpanInUse || s.sweepgen != sweepgen-1 { // 如果mspan的状态不是正在使用，或者代数并不和heamp的相差1，抛出异常
		print("MSpan_Sweep: state=", s.state, " sweepgen=", s.sweepgen, " mheap.sweepgen=", sweepgen, "\n")
		throw("MSpan_Sweep: bad span state")
	}

	if trace.enabled {
		traceGCSweepStart()
	}
	// 增加统计，被sweep的页面的数量
	xadd64(&mheap_.pagesSwept, int64(s.npages))

	cl := s.sizeclass  //该mspan对应的sizeclass
	size := s.elemsize // 该mspan保存的元素大小
	res := false
	nfree := 0

	var head, end gclinkptr

	c := _g_.m.mcache // 获得该goroutine对应的mcache
	freeToHeap := false

	// Mark any free objects in this span so we don't collect them.
	sstart := uintptr(s.start << _PageShift)
	for link := s.freelist; link.ptr() != nil; link = link.ptr().next {
		if uintptr(link) < sstart || s.limit <= uintptr(link) {
			// Free list is corrupted.
			dumpFreeList(s)
			throw("free list corrupted")
		}
		heapBitsForAddr(uintptr(link)).setMarkedNonAtomic()
	}

	// Unlink & free special records for any objects we're about to free.
	specialp := &s.specials
	special := *specialp
	for special != nil {
		// A finalizer can be set for an inner byte of an object, find object beginning.
		p := uintptr(s.start<<_PageShift) + uintptr(special.offset)/size*size
		hbits := heapBitsForAddr(p)
		if !hbits.isMarked() {
			// Find the exact byte for which the special was setup
			// (as opposed to object beginning).
			p := uintptr(s.start<<_PageShift) + uintptr(special.offset)
			// about to free object: splice out special record
			y := special
			special = special.next
			*specialp = special
			if !freespecial(y, unsafe.Pointer(p), size, false) {
				// stop freeing of object if it has a finalizer
				hbits.setMarkedNonAtomic()
			}
		} else {
			// object is still live: keep special record
			specialp = &special.next
			special = *specialp
		}
	}

	// Sweep through n objects of given size starting at p.
	// This thread owns the span now, so it can manipulate
	// the block bitmap without atomic operations.

	size, n, _ := s.layout()
	heapBitsSweepSpan(s.base(), size, n, func(p uintptr) {
		// At this point we know that we are looking at garbage object
		// that needs to be collected.
		if debug.allocfreetrace != 0 {
			tracefree(unsafe.Pointer(p), size)
		}

		// Reset to allocated+noscan.
		if cl == 0 {
			// Free large span.
			if preserve {
				throw("can't preserve large span")
			}
			heapBitsForSpan(p).initSpan(s.layout())
			s.needzero = 1

			// important to set sweepgen before returning it to heap
			atomicstore(&s.sweepgen, sweepgen)

			// Free the span after heapBitsSweepSpan
			// returns, since it's not done with the span.
			freeToHeap = true
		} else {
			// Free small object.
			if size > 2*ptrSize {
				*(*uintptr)(unsafe.Pointer(p + ptrSize)) = uintptrMask & 0xdeaddeaddeaddead // mark as "needs to be zeroed"
			} else if size > ptrSize {
				*(*uintptr)(unsafe.Pointer(p + ptrSize)) = 0
			}
			if head.ptr() == nil {
				head = gclinkptr(p)
			} else {
				end.ptr().next = gclinkptr(p)
			}
			end = gclinkptr(p)
			end.ptr().next = gclinkptr(0x0bade5)
			nfree++
		}
	})

	// We need to set s.sweepgen = h.sweepgen only when all blocks are swept,
	// because of the potential for a concurrent free/SetFinalizer.
	// But we need to set it before we make the span available for allocation
	// (return it to heap or mcentral), because allocation code assumes that a
	// span is already swept if available for allocation.
	//
	// TODO(austin): Clean this up by consolidating atomicstore in
	// large span path above with this.
	if !freeToHeap && nfree == 0 {
		// The span must be in our exclusive ownership until we update sweepgen,
		// check for potential races.
		if s.state != mSpanInUse || s.sweepgen != sweepgen-1 {
			print("MSpan_Sweep: state=", s.state, " sweepgen=", s.sweepgen, " mheap.sweepgen=", sweepgen, "\n")
			throw("MSpan_Sweep: bad span state after sweep")
		}
		atomicstore(&s.sweepgen, sweepgen)
	}
	if nfree > 0 {
		c.local_nsmallfree[cl] += uintptr(nfree)
		res = mCentral_FreeSpan(&mheap_.central[cl].mcentral, s, int32(nfree), head, end, preserve)
		// MCentral_FreeSpan updates sweepgen
	} else if freeToHeap {
		// Free large span to heap

		// NOTE(rsc,dvyukov): The original implementation of efence
		// in CL 22060046 used SysFree instead of SysFault, so that
		// the operating system would eventually give the memory
		// back to us again, so that an efence program could run
		// longer without running out of memory. Unfortunately,
		// calling SysFree here without any kind of adjustment of the
		// heap data structures means that when the memory does
		// come back to us, we have the wrong metadata for it, either in
		// the MSpan structures or in the garbage collection bitmap.
		// Using SysFault here means that the program will run out of
		// memory fairly quickly in efence mode, but at least it won't
		// have mysterious crashes due to confused memory reuse.
		// It should be possible to switch back to SysFree if we also
		// implement and then call some kind of MHeap_DeleteSpan.
		if debug.efence > 0 {
			s.limit = 0 // prevent mlookup from finding this span
			sysFault(unsafe.Pointer(uintptr(s.start<<_PageShift)), size)
		} else {
			mHeap_Free(&mheap_, s, 1)
		}
		c.local_nlargefree++
		c.local_largefree += size
		res = true
	}
	if trace.enabled {
		traceGCSweepDone()
	}
	return res
}

// deductSweepCredit deducts sweep credit for allocating a span of
// size spanBytes. This must be performed *before* the span is
// allocated to ensure the system has enough credit. If necessary, it
// performs sweeping to prevent going in to debt. If the caller will
// also sweep pages (e.g., for a large allocation), it can pass a
// non-zero callerSweepPages to leave that many pages unswept.
//
// deductSweepCredit is the core of the "proportional sweep" system.
// It uses statistics gathered by the garbage collector to perform
// enough sweeping so that all pages are swept during the concurrent
// sweep phase between GC cycles.
//
// mheap_ must NOT be locked.
func deductSweepCredit(spanBytes uintptr, callerSweepPages uintptr) {
	if mheap_.sweepPagesPerByte == 0 {
		// Proportional sweep is done or disabled.
		return
	}

	// Account for this span allocation.
	spanBytesAlloc := xadd64(&mheap_.spanBytesAlloc, int64(spanBytes))

	// Fix debt if necessary.
	pagesOwed := int64(mheap_.sweepPagesPerByte * float64(spanBytesAlloc))
	for pagesOwed-int64(atomicload64(&mheap_.pagesSwept)) > int64(callerSweepPages) {
		if gosweepone() == ^uintptr(0) {
			mheap_.sweepPagesPerByte = 0
			break
		}
	}
}

func dumpFreeList(s *mspan) {
	printlock()
	print("runtime: free list of span ", s, ":\n")
	sstart := uintptr(s.start << _PageShift)
	link := s.freelist
	for i := 0; i < int(s.npages*_PageSize/s.elemsize); i++ {
		if i != 0 {
			print(" -> ")
		}
		print(hex(link))
		if link.ptr() == nil {
			break
		}
		if uintptr(link) < sstart || s.limit <= uintptr(link) {
			// Bad link. Stop walking before we crash.
			print(" (BAD)")
			break
		}
		link = link.ptr().next
	}
	print("\n")
	printunlock()
}
