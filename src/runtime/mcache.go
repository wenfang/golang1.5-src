// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import "unsafe"

// 每个P的cache，用来分配小对象，因为只被一个P所使用，所以不需要锁定
// Per-thread (in Go, per-P) cache for small objects.
// No locking needed because it is per-thread (per-P).
type mcache struct {
	// 下列成员在每个malloc调用时访问，因此成组出现
	// The following members are accessed on every malloc,
	// so they are grouped here for better caching.
	next_sample      int32   // trigger heap sample after allocating this many bytes
	local_cachealloc uintptr // bytes allocated from cache since last lock of heap
	local_scan       uintptr // bytes of scannable heap allocated
	// Allocator cache for tiny objects w/o pointers.
	// See "Tiny allocator" comment in malloc.go.
	tiny             unsafe.Pointer
	tinyoffset       uintptr
	local_tinyallocs uintptr // number of tiny allocs not counted in other stats

	// The rest is not accessed on every malloc.
	alloc [_NumSizeClasses]*mspan // spans to allocate from 对应每个67类的mspan

	stackcache [_NumStackOrders]stackfreelist // 用作分配栈空间的stackfreelist

	// Local allocator stats, flushed during GC.
	local_nlookup    uintptr                  // number of pointer lookups
	local_largefree  uintptr                  // bytes freed for large objects (>maxsmallsize)
	local_nlargefree uintptr                  // number of frees for large objects (>maxsmallsize)
	local_nsmallfree [_NumSizeClasses]uintptr // number of frees for small objects (<=maxsmallsize)
}

// A gclink is a node in a linked list of blocks, like mlink,
// but it is opaque to the garbage collector.
// The GC does not trace the pointers during collection,
// and the compiler does not emit write barriers for assignments
// of gclinkptr values. Code should store references to gclinks
// as gclinkptr, not as *gclink.
type gclink struct {
	next gclinkptr
}

// A gclinkptr is a pointer to a gclink, but it is opaque
// to the garbage collector.
type gclinkptr uintptr

// ptr returns the *gclink form of p.
// The result should be used for accessing fields, not stored
// in other data structures.
func (p gclinkptr) ptr() *gclink {
	return (*gclink)(unsafe.Pointer(p))
}

type stackfreelist struct {
	list gclinkptr // linked list of free stacks
	size uintptr   // total size of stacks in list
}

// dummy MSpan that contains no free objects.
var emptymspan mspan

func allocmcache() *mcache { // 分配出mcache结构
	lock(&mheap_.lock)                                 // 先给mheap加锁
	c := (*mcache)(fixAlloc_Alloc(&mheap_.cachealloc)) // 分配出来一个mcache结构
	unlock(&mheap_.lock)                               // 分配完结构后就给mheap解锁
	memclr(unsafe.Pointer(c), unsafe.Sizeof(*c))       // 清除mcache结构
	for i := 0; i < _NumSizeClasses; i++ {             // 对67个class的每个class，都设置为emptymspan
		c.alloc[i] = &emptymspan
	}

	// Set first allocation sample size.
	rate := MemProfileRate
	if rate > 0x3fffffff { // make 2*rate not overflow
		rate = 0x3fffffff
	}
	if rate != 0 {
		c.next_sample = int32(int(fastrand1()) % (2 * rate))
	}

	return c // 返回分配出的mcache结构
}

func freemcache(c *mcache) {
	systemstack(func() {
		mCache_ReleaseAll(c)
		stackcache_clear(c)

		// NOTE(rsc,rlh): If gcworkbuffree comes back, we need to coordinate
		// with the stealing of gcworkbufs during garbage collection to avoid
		// a race where the workbuf is double-freed.
		// gcworkbuffree(c.gcworkbuf)

		lock(&mheap_.lock)
		purgecachedstats(c)
		fixAlloc_Free(&mheap_.cachealloc, unsafe.Pointer(c))
		unlock(&mheap_.lock)
	})
}

// 获得一个mspan，可用于分配空间，并加入到当前mcache对应的class数组中
// Gets a span that has a free object in it and assigns it
// to be the cached span for the given sizeclass.  Returns this span.
func mCache_Refill(c *mcache, sizeclass int32) *mspan {
	_g_ := getg() // 返回当前的goroutine

	_g_.m.locks++ // 为当前goroutine的m加锁
	// Return the current cached span to the central lists.
	s := c.alloc[sizeclass]      // 获得用于分配该class的mspan
	if s.freelist.ptr() != nil { // 如果是个非空mspan，抛出异常
		throw("refill on a nonempty span")
	}
	if s != &emptymspan {
		s.incache = false
	}

	// Get a new cached span from the central lists.
	s = mCentral_CacheSpan(&mheap_.central[sizeclass].mcentral) // 从mcentral中获得一个mspan，对应sizeclass
	if s == nil {                                               // 获得mspan失败，内存溢出了
		throw("out of memory")
	}
	if s.freelist.ptr() == nil { // 如果分配出的是一个空mspan，抛出异常
		println(s.ref, (s.npages<<_PageShift)/s.elemsize)
		throw("empty span")
	}
	c.alloc[sizeclass] = s // 将分配得到的mspan赋值给mcache
	_g_.m.locks--          // 为当前的m解锁
	return s
}

func mCache_ReleaseAll(c *mcache) {
	for i := 0; i < _NumSizeClasses; i++ {
		s := c.alloc[i]
		if s != &emptymspan {
			mCentral_UncacheSpan(&mheap_.central[i].mcentral, s)
			c.alloc[i] = &emptymspan
		}
	}
}
