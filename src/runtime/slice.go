// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"unsafe"
)

type slice struct {
	array unsafe.Pointer // 指向数据的指针
	len   int            // 当前slice的长度
	cap   int            // 当前slice的容量
}

// TODO: take uintptrs instead of int64s?
func makeslice(t *slicetype, len64, cap64 int64) slice {
	// NOTE: The len > MaxMem/elemsize check here is not strictly necessary,
	// but it produces a 'len out of range' error instead of a 'cap out of range' error
	// when someone does make([]T, bignumber). 'cap out of range' is true too,
	// but since the cap is only being supplied implicitly, saying len is clearer.
	// See issue 4085.
	len := int(len64)
	if len64 < 0 || int64(len) != len64 || t.elem.size > 0 && uintptr(len) > _MaxMem/uintptr(t.elem.size) {
		panic(errorString("makeslice: len out of range"))
	}
	cap := int(cap64)
	if cap < len || int64(cap) != cap64 || t.elem.size > 0 && uintptr(cap) > _MaxMem/uintptr(t.elem.size) {
		panic(errorString("makeslice: cap out of range"))
	}
	p := newarray(t.elem, uintptr(cap)) // 创建一个新array，元素类型为t.elem，容量大小为cap
	return slice{p, len, cap}
}

// growslice_n is a variant of growslice that takes the number of new elements
// instead of the new minimum capacity.
// TODO(rsc): This is used by append(slice, slice...).
// The compiler should change that code to use growslice directly (issue #11419).
func growslice_n(t *slicetype, old slice, n int) slice {
	if n < 1 {
		panic(errorString("growslice: invalid n"))
	}
	return growslice(t, old, old.cap+n)
}

// growslice handles slice growth during append.
// It is passed the slice type, the old slice, and the desired new minimum capacity,
// and it returns a new slice with at least that capacity, with the old data
// copied into it.
func growslice(t *slicetype, old slice, cap int) slice {
	if cap < old.cap || t.elem.size > 0 && uintptr(cap) > _MaxMem/uintptr(t.elem.size) {
		panic(errorString("growslice: cap out of range"))
	}

	if raceenabled {
		callerpc := getcallerpc(unsafe.Pointer(&t))
		racereadrangepc(old.array, uintptr(old.len*int(t.elem.size)), callerpc, funcPC(growslice))
	}

	et := t.elem      // 获得slice中的元素
	if et.size == 0 { // 元素大小为0，不用重新分配，直接返回
		// append should not create a slice with nil pointer but non-zero len.
		// We assume that append doesn't need to preserve old.array in this case.
		return slice{unsafe.Pointer(&zerobase), old.len, cap}
	}

	newcap := old.cap
	if newcap+newcap < cap { // 新的容量大于2倍的现容量，设置为新容量
		newcap = cap
	} else {
		for {
			if old.len < 1024 { // 如果现容量小于1024，以翻倍的容量增长
				newcap += newcap
			} else { // 如果现容量大于等于1024，以四分之一的容量增长
				newcap += newcap / 4
			}
			if newcap >= cap {
				break
			}
		}
	}

	if uintptr(newcap) >= _MaxMem/uintptr(et.size) {
		panic(errorString("growslice: cap out of range"))
	}
	lenmem := uintptr(old.len) * uintptr(et.size) // 获取当前slice有效数据所占的空间的大小
	capmem := roundupsize(uintptr(newcap) * uintptr(et.size))
	newcap = int(capmem / uintptr(et.size))
	var p unsafe.Pointer
	if et.kind&kindNoPointers != 0 { // 如果slice元素类型中不包含指针
		p = rawmem(capmem) // 分配一段原生内存，不包括指针
		memmove(p, old.array, lenmem)
		memclr(add(p, lenmem), capmem-lenmem) // 清空内存
	} else {
		// Note: can't use rawmem (which avoids zeroing of memory), because then GC can scan uninitialized memory.
		p = newarray(et, uintptr(newcap)) // 无法使用rawmem，调用newarray分配
		if !writeBarrierEnabled {
			memmove(p, old.array, lenmem)
		} else {
			for i := uintptr(0); i < lenmem; i += et.size {
				typedmemmove(et, add(p, i), add(old.array, i))
			}
		}
	}

	return slice{p, old.len, newcap}
}

func slicecopy(to, fm slice, width uintptr) int {
	if fm.len == 0 || to.len == 0 {
		return 0
	}

	n := fm.len // 选出来fm和to中len最小的
	if to.len < n {
		n = to.len
	}

	if width == 0 {
		return n
	}

	if raceenabled {
		callerpc := getcallerpc(unsafe.Pointer(&to))
		pc := funcPC(slicecopy)
		racewriterangepc(to.array, uintptr(n*int(width)), callerpc, pc)
		racereadrangepc(fm.array, uintptr(n*int(width)), callerpc, pc)
	}

	size := uintptr(n) * width // 获得拷贝的数据量大小
	if size == 1 {             // common case worth about 2x to do here
		// TODO: is this still worth it with new memmove impl?
		*(*byte)(to.array) = *(*byte)(fm.array) // known to be a byte pointer
	} else {
		memmove(to.array, fm.array, size)
	}
	return int(n)
}

func slicestringcopy(to []byte, fm string) int { // 针对copy时目的为[]byte源为string的情况
	if len(fm) == 0 || len(to) == 0 {
		return 0
	}

	n := len(fm)
	if len(to) < n {
		n = len(to)
	}

	if raceenabled {
		callerpc := getcallerpc(unsafe.Pointer(&to))
		pc := funcPC(slicestringcopy)
		racewriterangepc(unsafe.Pointer(&to[0]), uintptr(n), callerpc, pc)
	}

	memmove(unsafe.Pointer(&to[0]), unsafe.Pointer((*stringStruct)(unsafe.Pointer(&fm)).str), uintptr(n))
	return n
}
