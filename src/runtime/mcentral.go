// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Central free lists.
//
// See malloc.go for an overview.
// mcentral不真实包含空闲对象的列表，而是由mspan包含，每个mcentral包含两个mspan列表，一个包含空闲对象，另一个已完全分配完
// The MCentral doesn't actually contain the list of free objects; the MSpan does.
// Each MCentral is two lists of MSpans: those with free objects (c->nonempty)
// and those that are completely allocated (c->empty).

package runtime

// Central list of free objects of a given size.
type mcentral struct { // 给定大小的空闲对象
	lock      mutex // mcentral的锁
	sizeclass int32 // 该mcentral用于分配sizeclass大小的空间
	nonempty  mspan // list of spans with a free object 还具有空闲对象的mspan
	empty     mspan // list of spans with no free objects (or cached in an mcache) 没有空闲对象的mspan
}

// Initialize a single central free list.
func mCentral_Init(c *mcentral, sizeclass int32) { // 初始化mcentral结构
	c.sizeclass = sizeclass     // 用于分配的sizeclass
	mSpanList_Init(&c.nonempty) // 初始化mcentral内部的nonempty mspan
	mSpanList_Init(&c.empty)    // 初始化mcentral内部的empty mspan
}

// Allocate a span to use in an MCache.
func mCentral_CacheSpan(c *mcentral) *mspan { // 分配一个mspan在MCache中使用
	// Deduct credit for this span allocation and sweep if necessary.
	deductSweepCredit(uintptr(class_to_size[c.sizeclass]), 0)

	lock(&c.lock)
	sg := mheap_.sweepgen // 获得当前mheap的sweep代数
retry:
	var s *mspan
	for s = c.nonempty.next; s != &c.nonempty; s = s.next { // 遍历nonempty span列表
		if s.sweepgen == sg-2 && cas(&s.sweepgen, sg-2, sg-1) { // 如果central中mspan的代数比heap小2，尝试将该mspan sweep的代数增加1
			mSpanList_Remove(s)               // 从列表中获取mspan
			mSpanList_InsertBack(&c.empty, s) // 将获取到的mspan加入到empty列表尾部
			unlock(&c.lock)
			mSpan_Sweep(s, true) // 需要自己主动的进行一次MSpan的sweep
			goto havespan
		}
		if s.sweepgen == sg-1 { // 如果当前mspan的代数已经变为sg-1了，表明后台有sweeper,sweep了该Mspan，跳过
			// the span is being swept by background sweeper, skip
			continue
		}
		// we have a nonempty span that does not require sweeping, allocate from it 到这里有个不需要sweep的mspan，从其中分配
		mSpanList_Remove(s)               // 从列表中获取s
		mSpanList_InsertBack(&c.empty, s) // 将s加入empty列表
		unlock(&c.lock)
		goto havespan // 查找到了mspan，放到了s中，掉转到havespan
	}

	for s = c.empty.next; s != &c.empty; s = s.next { // 遍历所有empty也就是没有空闲空间的mspan
		if s.sweepgen == sg-2 && cas(&s.sweepgen, sg-2, sg-1) {
			// we have an empty span that requires sweeping,
			// sweep it and see if we can free some space in it
			mSpanList_Remove(s)
			// swept spans are at the end of the list
			mSpanList_InsertBack(&c.empty, s)
			unlock(&c.lock)
			mSpan_Sweep(s, true)
			if s.freelist.ptr() != nil {
				goto havespan
			}
			lock(&c.lock)
			// the span is still empty after sweep
			// it is already in the empty list, so just retry
			goto retry
		}
		if s.sweepgen == sg-1 {
			// the span is being swept by background sweeper, skip
			continue
		}
		// already swept empty span,
		// all subsequent ones must also be either swept or in process of sweeping
		break
	}
	unlock(&c.lock)

	// 如果到这里还没有发现mspan，需要重新进行mcentral_grow了
	// Replenish central list if empty.
	s = mCentral_Grow(c)
	if s == nil {
		return nil
	}
	lock(&c.lock)
	mSpanList_InsertBack(&c.empty, s)
	unlock(&c.lock)

	// 到这里时，s是一个非空的mspan
	// At this point s is a non-empty span, queued at the end of the empty list,
	// c is unlocked.
havespan:
	cap := int32((s.npages << _PageShift) / s.elemsize) // 获得该mspan可保持多少对象
	n := cap - int32(s.ref)
	if n == 0 { // 保存的对象为0，这是一个空的mspan，抛出异常
		throw("empty span")
	}
	if s.freelist.ptr() == nil {
		throw("freelist empty")
	}
	s.incache = true // 从mcentral中获取了一个mspan，将要被加到mcache中，设置incache为true
	return s         // 返回找到的mspan
}

// Return span from an MCache.
func mCentral_UncacheSpan(c *mcentral, s *mspan) { // 从mcache中归还mspan到mcentral
	lock(&c.lock)

	s.incache = false // mspan已经不在mcache中了

	if s.ref == 0 {
		throw("uncaching full span")
	}

	cap := int32((s.npages << _PageShift) / s.elemsize)
	n := cap - int32(s.ref)
	if n > 0 {
		mSpanList_Remove(s)
		mSpanList_Insert(&c.nonempty, s)
	}
	unlock(&c.lock)
}

// Free n objects from a span s back into the central free list c.
// Called during sweep.
// Returns true if the span was returned to heap.  Sets sweepgen to
// the latest generation.
// If preserve=true, don't return the span to heap nor relink in MCentral lists;
// caller takes care of it.
func mCentral_FreeSpan(c *mcentral, s *mspan, n int32, start gclinkptr, end gclinkptr, preserve bool) bool {
	if s.incache {
		throw("freespan into cached span")
	}

	// Add the objects back to s's free list.
	wasempty := s.freelist.ptr() == nil
	end.ptr().next = s.freelist
	s.freelist = start
	s.ref -= uint16(n)

	if preserve {
		// preserve is set only when called from MCentral_CacheSpan above,
		// the span must be in the empty list.
		if s.next == nil {
			throw("can't preserve unlinked span")
		}
		atomicstore(&s.sweepgen, mheap_.sweepgen)
		return false
	}

	lock(&c.lock)

	// Move to nonempty if necessary.
	if wasempty {
		mSpanList_Remove(s)
		mSpanList_Insert(&c.nonempty, s)
	}

	// delay updating sweepgen until here.  This is the signal that
	// the span may be used in an MCache, so it must come after the
	// linked list operations above (actually, just after the
	// lock of c above.)
	atomicstore(&s.sweepgen, mheap_.sweepgen)

	if s.ref != 0 {
		unlock(&c.lock)
		return false
	}

	// s is completely freed, return it to the heap.
	mSpanList_Remove(s)
	s.needzero = 1
	s.freelist = 0
	unlock(&c.lock)
	heapBitsForSpan(s.base()).initSpan(s.layout())
	mHeap_Free(&mheap_, s, 0)
	return true
}

// Fetch a new span from the heap and carve into objects for the free list.
func mCentral_Grow(c *mcentral) *mspan { // 从堆中获得新的mspan
	npages := uintptr(class_to_allocnpages[c.sizeclass]) // 获得要分配多少页面
	size := uintptr(class_to_size[c.sizeclass])          // 获得一次分配多少空间
	n := (npages << _PageShift) / size                   // 获得可以分配多少元素

	s := mHeap_Alloc(&mheap_, npages, c.sizeclass, false, true) // 从mheap中获取mspan
	if s == nil {
		return nil
	}

	p := uintptr(s.start << _PageShift) // 获得mspan的起始页面号
	s.limit = p + size*n                // 获得mspan结束位置
	head := gclinkptr(p)
	tail := gclinkptr(p)
	// i==0 iteration already done
	for i := uintptr(1); i < n; i++ { // 作为gclinkptr串起来
		p += size
		tail.ptr().next = gclinkptr(p)
		tail = gclinkptr(p)
	}
	if s.freelist.ptr() != nil {
		throw("freelist not empty")
	}
	tail.ptr().next = 0
	s.freelist = head // 加入到freelist列表中
	heapBitsForSpan(s.base()).initSpan(s.layout())
	return s
}
