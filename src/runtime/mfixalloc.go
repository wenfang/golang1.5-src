// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Fixed-size object allocator.  Returned memory is not zeroed.
//
// See malloc.go for overview.

package runtime

import "unsafe"

// FixAlloc用作固定大小的对象的分配,malloc使用FixAlloc管理MCache和MSpan结构对象
// FixAlloc is a simple free-list allocator for fixed size objects.
// Malloc uses a FixAlloc wrapped around sysAlloc to manages its
// MCache and MSpan objects.
// 由Fixalloc_Alloc返回的内存没有清0，调用者负责锁定
// Memory returned by FixAlloc_Alloc is not zeroed.
// The caller is responsible for locking around FixAlloc calls.
// Callers can keep state in the object but the first word is
// smashed by freeing and reallocating.
type fixalloc struct { // fixalloc结构
	size   uintptr        //  用来分配多大的对象
	first  unsafe.Pointer // go func(unsafe.pointer, unsafe.pointer); f(arg, p) called first time p is returned
	arg    unsafe.Pointer
	list   *mlink // 当前结构的连接列表
	chunk  *byte
	nchunk uint32
	inuse  uintptr // in-use bytes now 当前由该fixalloc分配的处于使用状态的字节数
	stat   *uint64
}

// 通用的块的连接列表，块的大小一般都比mlink大，由于给mlink.next的赋值会导致写分界，因而
// 其不能被一些内部的GC结构使用
// A generic linked list of blocks.  (Typically the block is bigger than sizeof(MLink).)
// Since assignments to mlink.next will result in a write barrier being preformed
// this can not be used by some of the internal GC structures. For example when
// the sweeper is placing an unmarked object on the free list it does not want the
// write barrier to be called since that could result in the object being reachable.
type mlink struct {
	next *mlink
}

// 初始化一个f，用来分配固定size大小的对象，内部使用分配器获得成块的内存
// Initialize f to allocate objects of the given size,
// using the allocator to obtain chunks of memory.
func fixAlloc_Init(f *fixalloc, size uintptr, first func(unsafe.Pointer, unsafe.Pointer), arg unsafe.Pointer, stat *uint64) {
	f.size = size // 用来分配多大的对象
	f.first = *(*unsafe.Pointer)(unsafe.Pointer(&first))
	f.arg = arg
	f.list = nil
	f.chunk = nil
	f.nchunk = 0
	f.inuse = 0
	f.stat = stat
}

func fixAlloc_Alloc(f *fixalloc) unsafe.Pointer { // 分配一段固定大小的内存
	if f.size == 0 { // 如果该fixalloc可分配的固定size为0，抛出异常
		print("runtime: use of FixAlloc_Alloc before FixAlloc_Init\n")
		throw("runtime: internal error")
	}

	if f.list != nil { // 如果列表中有数据，从列表中取出
		v := unsafe.Pointer(f.list)
		f.list = f.list.next
		f.inuse += f.size
		return v
	}
	if uintptr(f.nchunk) < f.size {
		f.chunk = (*uint8)(persistentalloc(_FixAllocChunk, 0, f.stat)) // 一次批量分配16K
		f.nchunk = _FixAllocChunk                                      // 设定当前chunk的大小为16K
	}

	v := (unsafe.Pointer)(f.chunk) // 获得当前指向chunk位置内存的指针，也就是分配出来的内存
	if f.first != nil {
		fn := *(*func(unsafe.Pointer, unsafe.Pointer))(unsafe.Pointer(&f.first)) // 将first转换为函数
		fn(f.arg, v)                                                             // 以arg和v为参数调用fn
	}
	f.chunk = (*byte)(add(unsafe.Pointer(f.chunk), f.size)) // 向后移动chunk的指针
	f.nchunk -= uint32(f.size)                              // 当前chunk仍保留的内存的大小
	f.inuse += f.size
	return v
}

func fixAlloc_Free(f *fixalloc, p unsafe.Pointer) { // 将p指向的内存释放到fixalloc中，也就是加入list中
	f.inuse -= f.size
	v := (*mlink)(p)
	v.next = f.list
	f.list = v
}
