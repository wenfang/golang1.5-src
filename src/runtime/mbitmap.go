// Copyright 2009 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Garbage collector: type and heap bitmaps.
//
// 栈，数据和bss位图
// Stack, data, and bss bitmaps
//
// 在数据段和bss段中的栈帧和全局变量由1bit的位图描述，0表示不感兴趣，1表示活跃指针
// 需要在gc时检查。在每个字节中bit的消耗从低到高
// Stack frames and global variables in the data and bss sections are described
// by 1-bit bitmaps in which 0 means uninteresting and 1 means live pointer
// to be visited during GC. The bits in each byte are consumed starting with
// the low bit: 1<<0, 1<<1, and so on.
//
// 堆位图
// Heap bitmap
//
// 分配的堆内存来自于一段内存的子集[start, used)，对每个指针大小的word，堆位图中
// 对应2个bit，存储在start位置之前。也就是说在地址start-1的字节，保存了从start到
// start+3*ptrSize内存的情况,start-2的字节保存start+4*ptrSize到start+7*ptrSize的内存
// 情况
// The allocated heap comes from a subset of the memory in the range [start, used),
// where start == mheap_.arena_start and used == mheap_.arena_used.
// The heap bitmap comprises 2 bits for each pointer-sized word in that range,
// stored in bytes indexed backward in memory from start.
// That is, the byte at address start-1 holds the 2-bit entries for the four words
// start through start+3*ptrSize, the byte at start-2 holds the entries for
// start+4*ptrSize through start+7*ptrSize, and so on.
//
// 对应每个2bit项，低位bit的信息和1bit位图相同，0表示不感兴趣，1表示保存有活跃指针
// 需要在gc时进行检查。高位bit的含义依赖于该word在对应分配对象中的位置。如果是对象
// 的第一个word，高位bit是GC的marked位。在第二个word，高位bit是gc的checkmarked位
// 在第三个及后面的word中，高位bit指示该对象仍然被描述（仍然是该对象的一部分)
// 在这些word中，如果一个高位bit为0，对应的地位bit也是0，那么对象描述结束。
// 00也被称作dead编码，它提示下面word对gc无意义。
// In each 2-bit entry, the lower bit holds the same information as in the 1-bit
// bitmaps: 0 means uninteresting and 1 means live pointer to be visited during GC.
// The meaning of the high bit depends on the position of the word being described
// in its allocated object. In the first word, the high bit is the GC ``marked'' bit.
// In the second word, the high bit is the GC ``checkmarked'' bit (see below).
// In the third and later words, the high bit indicates that the object is still
// being described. In these words, if a bit pair with a high bit 0 is encountered,
// the low bit can also be assumed to be 0, and the object description is over.
// This 00 is called the ``dead'' encoding: it signals that the rest of the words
// in the object are uninteresting to the garbage collector.
//
// 当写入字节时这个2bit的项被分割，一个字节中的前4位时mark位，后4位时指针位
// The 2-bit entries are split when written into the byte, so that the top half
// of the byte contains 4 mark bits and the bottom half contains 4 pointer bits.
// This form allows a copy from the 1-bit to the 4-bit form to keep the
// pointer bits contiguous, instead of having to space them out.
//
// The code makes use of the fact that the zero value for a heap bitmap
// has no live pointer bit set and is (depending on position), not marked,
// not checkmarked, and is the dead encoding.
// These properties must be preserved when modifying the encoding.
//
// Checkmarks
//
// 在并发垃圾收集的过程中，担心由于没有写屏障或者收集器的实现bug导致mark
// 活跃对象失败。gc有一个checkmark位来进行有效性检查。
// 在checkmode模式，在堆的位图中，对象第二个word对应bit的高位为checkmark位
// 当不在checkmark模式时，该位被设置为1
// In a concurrent garbage collector, one worries about failing to mark
// a live object due to mutations without write barriers or bugs in the
// collector implementation. As a sanity check, the GC has a 'checkmark'
// mode that retraverses the object graph with the world stopped, to make
// sure that everything that should be marked is marked.
// In checkmark mode, in the heap bitmap, the high bit of the 2-bit entry
// for the second word of the object holds the checkmark bit.
// When not in checkmark mode, this bit is set to 1.
//
// The smallest possible allocation is 8 bytes. On a 32-bit machine, that
// means every allocated object has two words, so there is room for the
// checkmark bit. On a 64-bit machine, however, the 8-byte allocation is
// just one word, so the second bit pair is not available for encoding the
// checkmark. However, because non-pointer allocations are combined
// into larger 16-byte (maxTinySize) allocations, a plain 8-byte allocation
// must be a pointer, so the type bit in the first word is not actually needed.
// It is still used in general, except in checkmark the type bit is repurposed
// as the checkmark bit and then reinitialized (to 1) as the type bit when
// finished.

package runtime

import "unsafe"

const (
	bitPointer = 1 << 0 // 指针位，一个字节的低四位
	bitMarked  = 1 << 4 // Mark位，一个字节的高四位

	heapBitsShift   = 1                 // shift offset between successive bitPointer or bitMarked entries
	heapBitmapScale = ptrSize * (8 / 2) // number of data bytes described by one heap bitmap byte

	// all mark/pointer bits in a byte
	bitMarkedAll  = bitMarked | bitMarked<<heapBitsShift | bitMarked<<(2*heapBitsShift) | bitMarked<<(3*heapBitsShift)
	bitPointerAll = bitPointer | bitPointer<<heapBitsShift | bitPointer<<(2*heapBitsShift) | bitPointer<<(3*heapBitsShift)
)

// addb returns the byte pointer p+n.
//go:nowritebarrier
func addb(p *byte, n uintptr) *byte {
	// Note: wrote out full expression instead of calling add(p, n)
	// to reduce the number of temporaries generated by the
	// compiler for this trivial expression during inlining.
	return (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + n))
}

// subtractb returns the byte pointer p-n.
//go:nowritebarrier
func subtractb(p *byte, n uintptr) *byte {
	// Note: wrote out full expression instead of calling add(p, -n)
	// to reduce the number of temporaries generated by the
	// compiler for this trivial expression during inlining.
	return (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) - n))
}

// add1 returns the byte pointer p+1.
//go:nowritebarrier
func add1(p *byte) *byte {
	// Note: wrote out full expression instead of calling addb(p, 1)
	// to reduce the number of temporaries generated by the
	// compiler for this trivial expression during inlining.
	return (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + 1))
}

// subtract1 returns the byte pointer p-1.
//go:nowritebarrier
//
// nosplit because it is used during write barriers and must not be preempted.
//go:nosplit
func subtract1(p *byte) *byte {
	// Note: wrote out full expression instead of calling subtractb(p, 1)
	// to reduce the number of temporaries generated by the
	// compiler for this trivial expression during inlining.
	return (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) - 1))
}

// mHeap_MapBits is called each time arena_used is extended.
// It maps any additional bitmap memory needed for the new arena memory.
// It must be called with the expected new value of arena_used,
// *before* h.arena_used has been updated.
// Waiting to update arena_used until after the memory has been mapped
// avoids faults when other threads try access the bitmap immediately
// after observing the change to arena_used.
//
//go:nowritebarrier
func mHeap_MapBits(h *mheap, arena_used uintptr) {
	// Caller has added extra mappings to the arena.
	// Add extra mappings of bitmap words as needed.
	// We allocate extra bitmap pieces in chunks of bitmapChunk.
	const bitmapChunk = 8192

	n := (arena_used - mheap_.arena_start) / heapBitmapScale
	n = round(n, bitmapChunk)
	n = round(n, _PhysPageSize)
	if h.bitmap_mapped >= n {
		return
	}

	sysMap(unsafe.Pointer(h.arena_start-n), n-h.bitmap_mapped, h.arena_reserved, &memstats.gc_sys)
	h.bitmap_mapped = n
}

// heapBits provides access to the bitmap bits for a single heap word.
// The methods on heapBits take value receivers so that the compiler
// can more easily inline calls to those methods and registerize the
// struct fields independently.
type heapBits struct {
	bitp  *uint8
	shift uint32
}

// heapBitsForAddr returns the heapBits for the address addr.
// The caller must have already checked that addr is in the range [mheap_.arena_start, mheap_.arena_used).
//
// nosplit because it is used during write barriers and must not be preempted.
//go:nosplit
func heapBitsForAddr(addr uintptr) heapBits {
	// 2 bits per work, 4 pairs per byte, and a mask is hard coded.
	off := (addr - mheap_.arena_start) / ptrSize
	return heapBits{(*uint8)(unsafe.Pointer(mheap_.arena_start - off/4 - 1)), uint32(off & 3)}
}

// heapBitsForSpan returns the heapBits for the span base address base.
func heapBitsForSpan(base uintptr) (hbits heapBits) {
	if base < mheap_.arena_start || base >= mheap_.arena_used {
		throw("heapBitsForSpan: base out of range")
	}
	hbits = heapBitsForAddr(base)
	if hbits.shift != 0 {
		throw("heapBitsForSpan: unaligned start")
	}
	return hbits
}

// heapBitsForObject returns the base address for the heap object
// containing the address p, along with the heapBits for base.
// If p does not point into a heap object,
// return base == 0
// otherwise return the base of the object.
func heapBitsForObject(p uintptr) (base uintptr, hbits heapBits, s *mspan) {
	arenaStart := mheap_.arena_start
	if p < arenaStart || p >= mheap_.arena_used {
		return
	}
	off := p - arenaStart
	idx := off >> _PageShift
	// p points into the heap, but possibly to the middle of an object.
	// Consult the span table to find the block beginning.
	k := p >> _PageShift
	s = h_spans[idx]
	if s == nil || pageID(k) < s.start || p >= s.limit || s.state != mSpanInUse {
		if s == nil || s.state == _MSpanStack {
			// If s is nil, the virtual address has never been part of the heap.
			// This pointer may be to some mmap'd region, so we allow it.
			// Pointers into stacks are also ok, the runtime manages these explicitly.
			return
		}

		// The following ensures that we are rigorous about what data
		// structures hold valid pointers.
		// TODO(rsc): Check if this still happens.
		if debug.invalidptr != 0 {
			// Still happens sometimes. We don't know why.
			printlock()
			print("runtime:objectstart Span weird: p=", hex(p), " k=", hex(k))
			if s == nil {
				print(" s=nil\n")
			} else {
				print(" s.start=", hex(s.start<<_PageShift), " s.limit=", hex(s.limit), " s.state=", s.state, "\n")
			}
			printunlock()
			throw("objectstart: bad pointer in unexpected span")
		}
		return
	}
	// If this span holds object of a power of 2 size, just mask off the bits to
	// the interior of the object. Otherwise use the size to get the base.
	if s.baseMask != 0 {
		// optimize for power of 2 sized objects.
		base = s.base()
		base = base + (p-base)&s.baseMask
		// base = p & s.baseMask is faster for small spans,
		// but doesn't work for large spans.
		// Overall, it's faster to use the more general computation above.
	} else {
		base = s.base()
		if p-base >= s.elemsize {
			// n := (p - base) / s.elemsize, using division by multiplication
			n := uintptr(uint64(p-base) >> s.divShift * uint64(s.divMul) >> s.divShift2)
			base += n * s.elemsize
		}
	}
	// Now that we know the actual base, compute heapBits to return to caller.
	hbits = heapBitsForAddr(base)
	return
}

// prefetch the bits.
func (h heapBits) prefetch() {
	prefetchnta(uintptr(unsafe.Pointer((h.bitp))))
}

// next returns the heapBits describing the next pointer-sized word in memory.
// That is, if h describes address p, h.next() describes p+ptrSize.
// Note that next does not modify h. The caller must record the result.
//
// nosplit because it is used during write barriers and must not be preempted.
//go:nosplit
func (h heapBits) next() heapBits {
	if h.shift < 3*heapBitsShift {
		return heapBits{h.bitp, h.shift + heapBitsShift}
	}
	return heapBits{subtract1(h.bitp), 0}
}

// forward returns the heapBits describing n pointer-sized words ahead of h in memory.
// That is, if h describes address p, h.forward(n) describes p+n*ptrSize.
// h.forward(1) is equivalent to h.next(), just slower.
// Note that forward does not modify h. The caller must record the result.
// bits returns the heap bits for the current word.
func (h heapBits) forward(n uintptr) heapBits {
	n += uintptr(h.shift) / heapBitsShift
	return heapBits{subtractb(h.bitp, n/4), uint32(n%4) * heapBitsShift}
}

// The caller can test isMarked and isPointer by &-ing with bitMarked and bitPointer.
// The result includes in its higher bits the bits for subsequent words
// described by the same bitmap byte.
func (h heapBits) bits() uint32 {
	return uint32(*h.bitp) >> h.shift
}

// isMarked reports whether the heap bits have the marked bit set.
// h must describe the initial word of the object.
func (h heapBits) isMarked() bool {
	return *h.bitp&(bitMarked<<h.shift) != 0
}

// setMarked sets the marked bit in the heap bits, atomically.
// h must describe the initial word of the object.
func (h heapBits) setMarked() {
	// Each byte of GC bitmap holds info for four words.
	// Might be racing with other updates, so use atomic update always.
	// We used to be clever here and use a non-atomic update in certain
	// cases, but it's not worth the risk.
	atomicor8(h.bitp, bitMarked<<h.shift)
}

// setMarkedNonAtomic sets the marked bit in the heap bits, non-atomically.
// h must describe the initial word of the object.
func (h heapBits) setMarkedNonAtomic() {
	*h.bitp |= bitMarked << h.shift
}

// isPointer reports whether the heap bits describe a pointer word.
// h must describe the initial word of the object.
//
// nosplit because it is used during write barriers and must not be preempted.
//go:nosplit
func (h heapBits) isPointer() bool {
	return (*h.bitp>>h.shift)&bitPointer != 0
}

// hasPointers reports whether the given object has any pointers.
// It must be told how large the object at h is, so that it does not read too
// far into the bitmap.
// h must describe the initial word of the object.
func (h heapBits) hasPointers(size uintptr) bool {
	if size == ptrSize { // 1-word objects are always pointers
		return true
	}
	// Otherwise, at least a 2-word object, and at least 2-word aligned,
	// so h.shift is either 0 or 4, so we know we can get the bits for the
	// first two words out of *h.bitp.
	// If either of the first two words is a pointer, not pointer free.
	b := uint32(*h.bitp >> h.shift)
	if b&(bitPointer|bitPointer<<heapBitsShift) != 0 {
		return true
	}
	if size == 2*ptrSize {
		return false
	}
	// At least a 4-word object. Check scan bit (aka marked bit) in third word.
	if h.shift == 0 {
		return b&(bitMarked<<(2*heapBitsShift)) != 0
	}
	return uint32(*subtract1(h.bitp))&bitMarked != 0
}

// isCheckmarked reports whether the heap bits have the checkmarked bit set.
// It must be told how large the object at h is, because the encoding of the
// checkmark bit varies by size.
// h must describe the initial word of the object.
func (h heapBits) isCheckmarked(size uintptr) bool {
	if size == ptrSize {
		return (*h.bitp>>h.shift)&bitPointer != 0
	}
	// All multiword objects are 2-word aligned,
	// so we know that the initial word's 2-bit pair
	// and the second word's 2-bit pair are in the
	// same heap bitmap byte, *h.bitp.
	return (*h.bitp>>(heapBitsShift+h.shift))&bitMarked != 0
}

// setCheckmarked sets the checkmarked bit.
// It must be told how large the object at h is, because the encoding of the
// checkmark bit varies by size.
// h must describe the initial word of the object.
func (h heapBits) setCheckmarked(size uintptr) {
	if size == ptrSize {
		atomicor8(h.bitp, bitPointer<<h.shift)
		return
	}
	atomicor8(h.bitp, bitMarked<<(heapBitsShift+h.shift))
}

// heapBitsBulkBarrier executes writebarrierptr_nostore
// for every pointer slot in the memory range [p, p+size),
// using the heap bitmap to locate those pointer slots.
// This executes the write barriers necessary after a memmove.
// Both p and size must be pointer-aligned.
// The range [p, p+size) must lie within a single allocation.
//
// Callers should call heapBitsBulkBarrier immediately after
// calling memmove(p, src, size). This function is marked nosplit
// to avoid being preempted; the GC must not stop the goroutine
// between the memmove and the execution of the barriers.
//
// The heap bitmap is not maintained for allocations containing
// no pointers at all; any caller of heapBitsBulkBarrier must first
// make sure the underlying allocation contains pointers, usually
// by checking typ.kind&kindNoPointers.
//
//go:nosplit
func heapBitsBulkBarrier(p, size uintptr) {
	if (p|size)&(ptrSize-1) != 0 {
		throw("heapBitsBulkBarrier: unaligned arguments")
	}
	if !writeBarrierEnabled {
		return
	}
	if !inheap(p) {
		// If p is on the stack and in a higher frame than the
		// caller, we either need to execute write barriers on
		// it (which is what happens for normal stack writes
		// through pointers to higher frames), or we need to
		// force the mark termination stack scan to scan the
		// frame containing p.
		//
		// Executing write barriers on p is complicated in the
		// general case because we either need to unwind the
		// stack to get the stack map, or we need the type's
		// bitmap, which may be a GC program.
		//
		// Hence, we opt for forcing the re-scan to scan the
		// frame containing p, which we can do by simply
		// unwinding the stack barriers between the current SP
		// and p's frame.
		gp := getg().m.curg
		if gp != nil && gp.stack.lo <= p && p < gp.stack.hi {
			// Run on the system stack to give it more
			// stack space.
			systemstack(func() {
				gcUnwindBarriers(gp, p)
			})
		}
		return
	}

	h := heapBitsForAddr(p)
	for i := uintptr(0); i < size; i += ptrSize {
		if h.isPointer() {
			x := (*uintptr)(unsafe.Pointer(p + i))
			writebarrierptr_nostore(x, *x)
		}
		h = h.next()
	}
}

// typeBitsBulkBarrier executes writebarrierptr_nostore
// for every pointer slot in the memory range [p, p+size),
// using the type bitmap to locate those pointer slots.
// The type typ must correspond exactly to [p, p+size).
// This executes the write barriers necessary after a copy.
// Both p and size must be pointer-aligned.
// The type typ must have a plain bitmap, not a GC program.
// The only use of this function is in channel sends, and the
// 64 kB channel element limit takes care of this for us.
//
// Must not be preempted because it typically runs right after memmove,
// and the GC must not complete between those two.
//
//go:nosplit
func typeBitsBulkBarrier(typ *_type, p, size uintptr) {
	if typ == nil {
		throw("runtime: typeBitsBulkBarrier without type")
	}
	if typ.size != size {
		println("runtime: typeBitsBulkBarrier with type ", *typ._string, " of size ", typ.size, " but memory size", size)
		throw("runtime: invalid typeBitsBulkBarrier")
	}
	if typ.kind&kindGCProg != 0 {
		println("runtime: typeBitsBulkBarrier with type ", *typ._string, " with GC prog")
		throw("runtime: invalid typeBitsBulkBarrier")
	}
	if !writeBarrierEnabled {
		return
	}
	ptrmask := typ.gcdata
	var bits uint32
	for i := uintptr(0); i < typ.ptrdata; i += ptrSize {
		if i&(ptrSize*8-1) == 0 {
			bits = uint32(*ptrmask)
			ptrmask = addb(ptrmask, 1)
		} else {
			bits = bits >> 1
		}
		if bits&1 != 0 {
			x := (*uintptr)(unsafe.Pointer(p + i))
			writebarrierptr_nostore(x, *x)
		}
	}
}

// The methods operating on spans all require that h has been returned
// by heapBitsForSpan and that size, n, total are the span layout description
// returned by the mspan's layout method.
// If total > size*n, it means that there is extra leftover memory in the span,
// usually due to rounding.
//
// TODO(rsc): Perhaps introduce a different heapBitsSpan type.

// initSpan initializes the heap bitmap for a span.
func (h heapBits) initSpan(size, n, total uintptr) {
	if total%heapBitmapScale != 0 {
		throw("initSpan: unaligned length")
	}
	nbyte := total / heapBitmapScale
	if ptrSize == 8 && size == ptrSize {
		end := h.bitp
		bitp := subtractb(end, nbyte-1)
		for {
			*bitp = bitPointerAll
			if bitp == end {
				break
			}
			bitp = add1(bitp)
		}
		return
	}
	memclr(unsafe.Pointer(subtractb(h.bitp, nbyte-1)), nbyte)
}

// 初始化一个mspan进行checkmark，清除checkmark位，因为普通操作情况下,checkmark位为1
// initCheckmarkSpan initializes a span for being checkmarked.
// It clears the checkmark bits, which are set to 1 in normal operation.
// size为每个元素的大小，n为元素的数量，total为总空间大小
func (h heapBits) initCheckmarkSpan(size, n, total uintptr) {
	// The ptrSize == 8 is a compile-time constant false on 32-bit and eliminates this code entirely.
	// 如果是64位系统，并且size大小也为8个字节，也就是没有第二个word，也就没法设置checkmark位
	if ptrSize == 8 && size == ptrSize {
		// Checkmark bit is type bit, bottom bit of every 2-bit entry.
		// Only possible on 64-bit system, since minimum size is 8.
		// Must clear type bit (checkmark bit) of every word.
		// The type bit is the lower of every two-bit pair.
		bitp := h.bitp
		for i := uintptr(0); i < n; i += 4 {
			*bitp &^= bitPointerAll
			bitp = subtract1(bitp)
		}
		return
	}
	// 对其他字节大小，遍历所有的元素
	for i := uintptr(0); i < n; i++ {
		*h.bitp &^= bitMarked << (heapBitsShift + h.shift) // 清除该位
		h = h.forward(size / ptrSize)
	}
}

// clearCheckmarkSpan undoes all the checkmarking in a span.
// The actual checkmark bits are ignored, so the only work to do
// is to fix the pointer bits. (Pointer bits are ignored by scanobject
// but consulted by typedmemmove.)
func (h heapBits) clearCheckmarkSpan(size, n, total uintptr) {
	// The ptrSize == 8 is a compile-time constant false on 32-bit and eliminates this code entirely.
	if ptrSize == 8 && size == ptrSize {
		// Checkmark bit is type bit, bottom bit of every 2-bit entry.
		// Only possible on 64-bit system, since minimum size is 8.
		// Must clear type bit (checkmark bit) of every word.
		// The type bit is the lower of every two-bit pair.
		bitp := h.bitp
		for i := uintptr(0); i < n; i += 4 {
			*bitp |= bitPointerAll
			bitp = subtract1(bitp)
		}
	}
}

// heapBitsSweepSpan coordinates the sweeping of a span by reading
// and updating the corresponding heap bitmap entries.
// For each free object in the span, heapBitsSweepSpan sets the type
// bits for the first two words (or one for single-word objects) to typeDead
// and then calls f(p), where p is the object's base address.
// f is expected to add the object to a free list.
// For non-free objects, heapBitsSweepSpan turns off the marked bit.
func heapBitsSweepSpan(base, size, n uintptr, f func(uintptr)) {
	h := heapBitsForSpan(base)
	switch {
	default:
		throw("heapBitsSweepSpan")
	case ptrSize == 8 && size == ptrSize:
		// Consider mark bits in all four 2-bit entries of each bitmap byte.
		bitp := h.bitp
		for i := uintptr(0); i < n; i += 4 {
			x := uint32(*bitp)
			// Note that unlike the other size cases, we leave the pointer bits set here.
			// These are initialized during initSpan when the span is created and left
			// in place the whole time the span is used for pointer-sized objects.
			// That lets heapBitsSetType avoid an atomic update to set the pointer bit
			// during allocation.
			if x&bitMarked != 0 {
				x &^= bitMarked
			} else {
				f(base + i*ptrSize)
			}
			if x&(bitMarked<<heapBitsShift) != 0 {
				x &^= bitMarked << heapBitsShift
			} else {
				f(base + (i+1)*ptrSize)
			}
			if x&(bitMarked<<(2*heapBitsShift)) != 0 {
				x &^= bitMarked << (2 * heapBitsShift)
			} else {
				f(base + (i+2)*ptrSize)
			}
			if x&(bitMarked<<(3*heapBitsShift)) != 0 {
				x &^= bitMarked << (3 * heapBitsShift)
			} else {
				f(base + (i+3)*ptrSize)
			}
			*bitp = uint8(x)
			bitp = subtract1(bitp)
		}

	case size%(4*ptrSize) == 0:
		// Mark bit is in first word of each object.
		// Each object starts at bit 0 of a heap bitmap byte.
		bitp := h.bitp
		step := size / heapBitmapScale
		for i := uintptr(0); i < n; i++ {
			x := uint32(*bitp)
			if x&bitMarked != 0 {
				x &^= bitMarked
			} else {
				x = 0
				f(base + i*size)
			}
			*bitp = uint8(x)
			bitp = subtractb(bitp, step)
		}

	case size%(4*ptrSize) == 2*ptrSize:
		// Mark bit is in first word of each object,
		// but every other object starts halfway through a heap bitmap byte.
		// Unroll loop 2x to handle alternating shift count and step size.
		bitp := h.bitp
		step := size / heapBitmapScale
		var i uintptr
		for i = uintptr(0); i < n; i += 2 {
			x := uint32(*bitp)
			if x&bitMarked != 0 {
				x &^= bitMarked
			} else {
				x &^= bitMarked | bitPointer | (bitMarked|bitPointer)<<heapBitsShift
				f(base + i*size)
				if size > 2*ptrSize {
					x = 0
				}
			}
			*bitp = uint8(x)
			if i+1 >= n {
				break
			}
			bitp = subtractb(bitp, step)
			x = uint32(*bitp)
			if x&(bitMarked<<(2*heapBitsShift)) != 0 {
				x &^= bitMarked << (2 * heapBitsShift)
			} else {
				x &^= (bitMarked|bitPointer)<<(2*heapBitsShift) | (bitMarked|bitPointer)<<(3*heapBitsShift)
				f(base + (i+1)*size)
				if size > 2*ptrSize {
					*subtract1(bitp) = 0
				}
			}
			*bitp = uint8(x)
			bitp = subtractb(bitp, step+1)
		}
	}
}

// heapBitsSetType records that the new allocation [x, x+size)
// holds in [x, x+dataSize) one or more values of type typ.
// (The number of values is given by dataSize / typ.size.)
// If dataSize < size, the fragment [x+dataSize, x+size) is
// recorded as non-pointer data.
// It is known that the type has pointers somewhere;
// malloc does not call heapBitsSetType when there are no pointers,
// because all free objects are marked as noscan during
// heapBitsSweepSpan.
// There can only be one allocation from a given span active at a time,
// so this code is not racing with other instances of itself,
// and we don't allocate from a span until it has been swept,
// so this code is not racing with heapBitsSweepSpan.
// It is, however, racing with the concurrent GC mark phase,
// which can be setting the mark bit in the leading 2-bit entry
// of an allocated block. The block we are modifying is not quite
// allocated yet, so the GC marker is not racing with updates to x's bits,
// but if the start or end of x shares a bitmap byte with an adjacent
// object, the GC marker is racing with updates to those object's mark bits.
func heapBitsSetType(x, size, dataSize uintptr, typ *_type) {
	const doubleCheck = false // slow but helpful; enable to test modifications to this code

	// dataSize is always size rounded up to the next malloc size class,
	// except in the case of allocating a defer block, in which case
	// size is sizeof(_defer{}) (at least 6 words) and dataSize may be
	// arbitrarily larger.
	//
	// The checks for size == ptrSize and size == 2*ptrSize can therefore
	// assume that dataSize == size without checking it explicitly.

	if ptrSize == 8 && size == ptrSize {
		// It's one word and it has pointers, it must be a pointer.
		// In general we'd need an atomic update here if the
		// concurrent GC were marking objects in this span,
		// because each bitmap byte describes 3 other objects
		// in addition to the one being allocated.
		// However, since all allocated one-word objects are pointers
		// (non-pointers are aggregated into tinySize allocations),
		// initSpan sets the pointer bits for us. Nothing to do here.
		if doubleCheck {
			h := heapBitsForAddr(x)
			if !h.isPointer() {
				throw("heapBitsSetType: pointer bit missing")
			}
		}
		return
	}

	h := heapBitsForAddr(x)
	ptrmask := typ.gcdata // start of 1-bit pointer mask (or GC program, handled below)

	// Heap bitmap bits for 2-word object are only 4 bits,
	// so also shared with objects next to it; use atomic updates.
	// This is called out as a special case primarily for 32-bit systems,
	// so that on 32-bit systems the code below can assume all objects
	// are 4-word aligned (because they're all 16-byte aligned).
	if size == 2*ptrSize {
		if typ.size == ptrSize {
			// We're allocating a block big enough to hold two pointers.
			// On 64-bit, that means the actual object must be two pointers,
			// or else we'd have used the one-pointer-sized block.
			// On 32-bit, however, this is the 8-byte block, the smallest one.
			// So it could be that we're allocating one pointer and this was
			// just the smallest block available. Distinguish by checking dataSize.
			// (In general the number of instances of typ being allocated is
			// dataSize/typ.size.)
			if ptrSize == 4 && dataSize == ptrSize {
				// 1 pointer.
				if gcphase == _GCoff {
					*h.bitp |= bitPointer << h.shift
				} else {
					atomicor8(h.bitp, bitPointer<<h.shift)
				}
			} else {
				// 2-element slice of pointer.
				if gcphase == _GCoff {
					*h.bitp |= (bitPointer | bitPointer<<heapBitsShift) << h.shift
				} else {
					atomicor8(h.bitp, (bitPointer|bitPointer<<heapBitsShift)<<h.shift)
				}
			}
			return
		}
		// Otherwise typ.size must be 2*ptrSize, and typ.kind&kindGCProg == 0.
		if doubleCheck {
			if typ.size != 2*ptrSize || typ.kind&kindGCProg != 0 {
				print("runtime: heapBitsSetType size=", size, " but typ.size=", typ.size, " gcprog=", typ.kind&kindGCProg != 0, "\n")
				throw("heapBitsSetType")
			}
		}
		b := uint32(*ptrmask)
		hb := b & 3
		if gcphase == _GCoff {
			*h.bitp |= uint8(hb << h.shift)
		} else {
			atomicor8(h.bitp, uint8(hb<<h.shift))
		}
		return
	}

	// Copy from 1-bit ptrmask into 2-bit bitmap.
	// The basic approach is to use a single uintptr as a bit buffer,
	// alternating between reloading the buffer and writing bitmap bytes.
	// In general, one load can supply two bitmap byte writes.
	// This is a lot of lines of code, but it compiles into relatively few
	// machine instructions.

	var (
		// Ptrmask input.
		p     *byte   // last ptrmask byte read
		b     uintptr // ptrmask bits already loaded
		nb    uintptr // number of bits in b at next read
		endp  *byte   // final ptrmask byte to read (then repeat)
		endnb uintptr // number of valid bits in *endp
		pbits uintptr // alternate source of bits

		// Heap bitmap output.
		w     uintptr // words processed
		nw    uintptr // number of words to process
		hbitp *byte   // next heap bitmap byte to write
		hb    uintptr // bits being prepared for *hbitp
	)

	hbitp = h.bitp

	// Handle GC program. Delayed until this part of the code
	// so that we can use the same double-checking mechanism
	// as the 1-bit case. Nothing above could have encountered
	// GC programs: the cases were all too small.
	if typ.kind&kindGCProg != 0 {
		heapBitsSetTypeGCProg(h, typ.ptrdata, typ.size, dataSize, size, addb(typ.gcdata, 4))
		if doubleCheck {
			// Double-check the heap bits written by GC program
			// by running the GC program to create a 1-bit pointer mask
			// and then jumping to the double-check code below.
			// This doesn't catch bugs shared between the 1-bit and 4-bit
			// GC program execution, but it does catch mistakes specific
			// to just one of those and bugs in heapBitsSetTypeGCProg's
			// implementation of arrays.
			lock(&debugPtrmask.lock)
			if debugPtrmask.data == nil {
				debugPtrmask.data = (*byte)(persistentalloc(1<<20, 1, &memstats.other_sys))
			}
			ptrmask = debugPtrmask.data
			runGCProg(addb(typ.gcdata, 4), nil, ptrmask, 1)
			goto Phase4
		}
		return
	}

	// Note about sizes:
	//
	// typ.size is the number of words in the object,
	// and typ.ptrdata is the number of words in the prefix
	// of the object that contains pointers. That is, the final
	// typ.size - typ.ptrdata words contain no pointers.
	// This allows optimization of a common pattern where
	// an object has a small header followed by a large scalar
	// buffer. If we know the pointers are over, we don't have
	// to scan the buffer's heap bitmap at all.
	// The 1-bit ptrmasks are sized to contain only bits for
	// the typ.ptrdata prefix, zero padded out to a full byte
	// of bitmap. This code sets nw (below) so that heap bitmap
	// bits are only written for the typ.ptrdata prefix; if there is
	// more room in the allocated object, the next heap bitmap
	// entry is a 00, indicating that there are no more pointers
	// to scan. So only the ptrmask for the ptrdata bytes is needed.
	//
	// Replicated copies are not as nice: if there is an array of
	// objects with scalar tails, all but the last tail does have to
	// be initialized, because there is no way to say "skip forward".
	// However, because of the possibility of a repeated type with
	// size not a multiple of 4 pointers (one heap bitmap byte),
	// the code already must handle the last ptrmask byte specially
	// by treating it as containing only the bits for endnb pointers,
	// where endnb <= 4. We represent large scalar tails that must
	// be expanded in the replication by setting endnb larger than 4.
	// This will have the effect of reading many bits out of b,
	// but once the real bits are shifted out, b will supply as many
	// zero bits as we try to read, which is exactly what we need.

	p = ptrmask
	if typ.size < dataSize {
		// Filling in bits for an array of typ.
		// Set up for repetition of ptrmask during main loop.
		// Note that ptrmask describes only a prefix of
		const maxBits = ptrSize*8 - 7
		if typ.ptrdata/ptrSize <= maxBits {
			// Entire ptrmask fits in uintptr with room for a byte fragment.
			// Load into pbits and never read from ptrmask again.
			// This is especially important when the ptrmask has
			// fewer than 8 bits in it; otherwise the reload in the middle
			// of the Phase 2 loop would itself need to loop to gather
			// at least 8 bits.

			// Accumulate ptrmask into b.
			// ptrmask is sized to describe only typ.ptrdata, but we record
			// it as describing typ.size bytes, since all the high bits are zero.
			nb = typ.ptrdata / ptrSize
			for i := uintptr(0); i < nb; i += 8 {
				b |= uintptr(*p) << i
				p = add1(p)
			}
			nb = typ.size / ptrSize

			// Replicate ptrmask to fill entire pbits uintptr.
			// Doubling and truncating is fewer steps than
			// iterating by nb each time. (nb could be 1.)
			// Since we loaded typ.ptrdata/ptrSize bits
			// but are pretending to have typ.size/ptrSize,
			// there might be no replication necessary/possible.
			pbits = b
			endnb = nb
			if nb+nb <= maxBits {
				for endnb <= ptrSize*8 {
					pbits |= pbits << endnb
					endnb += endnb
				}
				// Truncate to a multiple of original ptrmask.
				endnb = maxBits / nb * nb
				pbits &= 1<<endnb - 1
				b = pbits
				nb = endnb
			}

			// Clear p and endp as sentinel for using pbits.
			// Checked during Phase 2 loop.
			p = nil
			endp = nil
		} else {
			// Ptrmask is larger. Read it multiple times.
			n := (typ.ptrdata/ptrSize+7)/8 - 1
			endp = addb(ptrmask, n)
			endnb = typ.size/ptrSize - n*8
		}
	}
	if p != nil {
		b = uintptr(*p)
		p = add1(p)
		nb = 8
	}

	if typ.size == dataSize {
		// Single entry: can stop once we reach the non-pointer data.
		nw = typ.ptrdata / ptrSize
	} else {
		// Repeated instances of typ in an array.
		// Have to process first N-1 entries in full, but can stop
		// once we reach the non-pointer data in the final entry.
		nw = ((dataSize/typ.size-1)*typ.size + typ.ptrdata) / ptrSize
	}
	if nw == 0 {
		// No pointers! Caller was supposed to check.
		println("runtime: invalid type ", *typ._string)
		throw("heapBitsSetType: called with non-pointer type")
		return
	}
	if nw < 2 {
		// Must write at least 2 words, because the "no scan"
		// encoding doesn't take effect until the third word.
		nw = 2
	}

	// Phase 1: Special case for leading byte (shift==0) or half-byte (shift==4).
	// The leading byte is special because it contains the bits for words 0 and 1,
	// which do not have the marked bits set.
	// The leading half-byte is special because it's a half a byte and must be
	// manipulated atomically.
	switch {
	default:
		throw("heapBitsSetType: unexpected shift")

	case h.shift == 0:
		// Ptrmask and heap bitmap are aligned.
		// Handle first byte of bitmap specially.
		// The first byte we write out contains the first two words of the object.
		// In those words, the mark bits are mark and checkmark, respectively,
		// and must not be set. In all following words, we want to set the mark bit
		// as a signal that the object continues to the next 2-bit entry in the bitmap.
		hb = b & bitPointerAll
		hb |= bitMarked<<(2*heapBitsShift) | bitMarked<<(3*heapBitsShift)
		if w += 4; w >= nw {
			goto Phase3
		}
		*hbitp = uint8(hb)
		hbitp = subtract1(hbitp)
		b >>= 4
		nb -= 4

	case ptrSize == 8 && h.shift == 2:
		// Ptrmask and heap bitmap are misaligned.
		// The bits for the first two words are in a byte shared with another object
		// and must be updated atomically.
		// NOTE(rsc): The atomic here may not be necessary.
		// We took care of 1-word and 2-word objects above,
		// so this is at least a 6-word object, so our start bits
		// are shared only with the type bits of another object,
		// not with its mark bit. Since there is only one allocation
		// from a given span at a time, we should be able to set
		// these bits non-atomically. Not worth the risk right now.
		hb = (b & 3) << (2 * heapBitsShift)
		b >>= 2
		nb -= 2
		// Note: no bitMarker in hb because the first two words don't get markers from us.
		if gcphase == _GCoff {
			*hbitp |= uint8(hb)
		} else {
			atomicor8(hbitp, uint8(hb))
		}
		hbitp = subtract1(hbitp)
		if w += 2; w >= nw {
			// We know that there is more data, because we handled 2-word objects above.
			// This must be at least a 6-word object. If we're out of pointer words,
			// mark no scan in next bitmap byte and finish.
			hb = 0
			w += 4
			goto Phase3
		}
	}

	// Phase 2: Full bytes in bitmap, up to but not including write to last byte (full or partial) in bitmap.
	// The loop computes the bits for that last write but does not execute the write;
	// it leaves the bits in hb for processing by phase 3.
	// To avoid repeated adjustment of nb, we subtract out the 4 bits we're going to
	// use in the first half of the loop right now, and then we only adjust nb explicitly
	// if the 8 bits used by each iteration isn't balanced by 8 bits loaded mid-loop.
	nb -= 4
	for {
		// Emit bitmap byte.
		// b has at least nb+4 bits, with one exception:
		// if w+4 >= nw, then b has only nw-w bits,
		// but we'll stop at the break and then truncate
		// appropriately in Phase 3.
		hb = b & bitPointerAll
		hb |= bitMarkedAll
		if w += 4; w >= nw {
			break
		}
		*hbitp = uint8(hb)
		hbitp = subtract1(hbitp)
		b >>= 4

		// Load more bits. b has nb right now.
		if p != endp {
			// Fast path: keep reading from ptrmask.
			// nb unmodified: we just loaded 8 bits,
			// and the next iteration will consume 8 bits,
			// leaving us with the same nb the next time we're here.
			if nb < 8 {
				b |= uintptr(*p) << nb
				p = add1(p)
			} else {
				// Reduce the number of bits in b.
				// This is important if we skipped
				// over a scalar tail, since nb could
				// be larger than the bit width of b.
				nb -= 8
			}
		} else if p == nil {
			// Almost as fast path: track bit count and refill from pbits.
			// For short repetitions.
			if nb < 8 {
				b |= pbits << nb
				nb += endnb
			}
			nb -= 8 // for next iteration
		} else {
			// Slow path: reached end of ptrmask.
			// Process final partial byte and rewind to start.
			b |= uintptr(*p) << nb
			nb += endnb
			if nb < 8 {
				b |= uintptr(*ptrmask) << nb
				p = add1(ptrmask)
			} else {
				nb -= 8
				p = ptrmask
			}
		}

		// Emit bitmap byte.
		hb = b & bitPointerAll
		hb |= bitMarkedAll
		if w += 4; w >= nw {
			break
		}
		*hbitp = uint8(hb)
		hbitp = subtract1(hbitp)
		b >>= 4
	}

Phase3:
	// Phase 3: Write last byte or partial byte and zero the rest of the bitmap entries.
	if w > nw {
		// Counting the 4 entries in hb not yet written to memory,
		// there are more entries than possible pointer slots.
		// Discard the excess entries (can't be more than 3).
		mask := uintptr(1)<<(4-(w-nw)) - 1
		hb &= mask | mask<<4 // apply mask to both pointer bits and mark bits
	}

	// Change nw from counting possibly-pointer words to total words in allocation.
	nw = size / ptrSize

	// Write whole bitmap bytes.
	// The first is hb, the rest are zero.
	if w <= nw {
		*hbitp = uint8(hb)
		hbitp = subtract1(hbitp)
		hb = 0 // for possible final half-byte below
		for w += 4; w <= nw; w += 4 {
			*hbitp = 0
			hbitp = subtract1(hbitp)
		}
	}

	// Write final partial bitmap byte if any.
	// We know w > nw, or else we'd still be in the loop above.
	// It can be bigger only due to the 4 entries in hb that it counts.
	// If w == nw+4 then there's nothing left to do: we wrote all nw entries
	// and can discard the 4 sitting in hb.
	// But if w == nw+2, we need to write first two in hb.
	// The byte is shared with the next object so we may need an atomic.
	if w == nw+2 {
		if gcphase == _GCoff {
			*hbitp = *hbitp&^(bitPointer|bitMarked|(bitPointer|bitMarked)<<heapBitsShift) | uint8(hb)
		} else {
			atomicand8(hbitp, ^uint8(bitPointer|bitMarked|(bitPointer|bitMarked)<<heapBitsShift))
			atomicor8(hbitp, uint8(hb))
		}
	}

Phase4:
	// Phase 4: all done, but perhaps double check.
	if doubleCheck {
		end := heapBitsForAddr(x + size)
		if typ.kind&kindGCProg == 0 && (hbitp != end.bitp || (w == nw+2) != (end.shift == 2)) {
			println("ended at wrong bitmap byte for", *typ._string, "x", dataSize/typ.size)
			print("typ.size=", typ.size, " typ.ptrdata=", typ.ptrdata, " dataSize=", dataSize, " size=", size, "\n")
			print("w=", w, " nw=", nw, " b=", hex(b), " nb=", nb, " hb=", hex(hb), "\n")
			h0 := heapBitsForAddr(x)
			print("initial bits h0.bitp=", h0.bitp, " h0.shift=", h0.shift, "\n")
			print("ended at hbitp=", hbitp, " but next starts at bitp=", end.bitp, " shift=", end.shift, "\n")
			throw("bad heapBitsSetType")
		}

		// Double-check that bits to be written were written correctly.
		// Does not check that other bits were not written, unfortunately.
		h := heapBitsForAddr(x)
		nptr := typ.ptrdata / ptrSize
		ndata := typ.size / ptrSize
		count := dataSize / typ.size
		totalptr := ((count-1)*typ.size + typ.ptrdata) / ptrSize
		for i := uintptr(0); i < size/ptrSize; i++ {
			j := i % ndata
			var have, want uint8
			have = (*h.bitp >> h.shift) & (bitPointer | bitMarked)
			if i >= totalptr {
				want = 0 // deadmarker
				if typ.kind&kindGCProg != 0 && i < (totalptr+3)/4*4 {
					want = bitMarked
				}
			} else {
				if j < nptr && (*addb(ptrmask, j/8)>>(j%8))&1 != 0 {
					want |= bitPointer
				}
				if i >= 2 {
					want |= bitMarked
				} else {
					have &^= bitMarked
				}
			}
			if have != want {
				println("mismatch writing bits for", *typ._string, "x", dataSize/typ.size)
				print("typ.size=", typ.size, " typ.ptrdata=", typ.ptrdata, " dataSize=", dataSize, " size=", size, "\n")
				print("kindGCProg=", typ.kind&kindGCProg != 0, "\n")
				print("w=", w, " nw=", nw, " b=", hex(b), " nb=", nb, " hb=", hex(hb), "\n")
				h0 := heapBitsForAddr(x)
				print("initial bits h0.bitp=", h0.bitp, " h0.shift=", h0.shift, "\n")
				print("current bits h.bitp=", h.bitp, " h.shift=", h.shift, " *h.bitp=", hex(*h.bitp), "\n")
				print("ptrmask=", ptrmask, " p=", p, " endp=", endp, " endnb=", endnb, " pbits=", hex(pbits), " b=", hex(b), " nb=", nb, "\n")
				println("at word", i, "offset", i*ptrSize, "have", have, "want", want)
				if typ.kind&kindGCProg != 0 {
					println("GC program:")
					dumpGCProg(addb(typ.gcdata, 4))
				}
				throw("bad heapBitsSetType")
			}
			h = h.next()
		}
		if ptrmask == debugPtrmask.data {
			unlock(&debugPtrmask.lock)
		}
	}
}

var debugPtrmask struct {
	lock mutex
	data *byte
}

// heapBitsSetTypeGCProg implements heapBitsSetType using a GC program.
// progSize is the size of the memory described by the program.
// elemSize is the size of the element that the GC program describes (a prefix of).
// dataSize is the total size of the intended data, a multiple of elemSize.
// allocSize is the total size of the allocated memory.
//
// GC programs are only used for large allocations.
// heapBitsSetType requires that allocSize is a multiple of 4 words,
// so that the relevant bitmap bytes are not shared with surrounding
// objects and need not be accessed with atomic instructions.
func heapBitsSetTypeGCProg(h heapBits, progSize, elemSize, dataSize, allocSize uintptr, prog *byte) {
	if ptrSize == 8 && allocSize%(4*ptrSize) != 0 {
		// Alignment will be wrong.
		throw("heapBitsSetTypeGCProg: small allocation")
	}
	var totalBits uintptr
	if elemSize == dataSize {
		totalBits = runGCProg(prog, nil, h.bitp, 2)
		if totalBits*ptrSize != progSize {
			println("runtime: heapBitsSetTypeGCProg: total bits", totalBits, "but progSize", progSize)
			throw("heapBitsSetTypeGCProg: unexpected bit count")
		}
	} else {
		count := dataSize / elemSize

		// Piece together program trailer to run after prog that does:
		//	literal(0)
		//	repeat(1, elemSize-progSize-1) // zeros to fill element size
		//	repeat(elemSize, count-1) // repeat that element for count
		// This zero-pads the data remaining in the first element and then
		// repeats that first element to fill the array.
		var trailer [40]byte // 3 varints (max 10 each) + some bytes
		i := 0
		if n := elemSize/ptrSize - progSize/ptrSize; n > 0 {
			// literal(0)
			trailer[i] = 0x01
			i++
			trailer[i] = 0
			i++
			if n > 1 {
				// repeat(1, n-1)
				trailer[i] = 0x81
				i++
				n--
				for ; n >= 0x80; n >>= 7 {
					trailer[i] = byte(n | 0x80)
					i++
				}
				trailer[i] = byte(n)
				i++
			}
		}
		// repeat(elemSize/ptrSize, count-1)
		trailer[i] = 0x80
		i++
		n := elemSize / ptrSize
		for ; n >= 0x80; n >>= 7 {
			trailer[i] = byte(n | 0x80)
			i++
		}
		trailer[i] = byte(n)
		i++
		n = count - 1
		for ; n >= 0x80; n >>= 7 {
			trailer[i] = byte(n | 0x80)
			i++
		}
		trailer[i] = byte(n)
		i++
		trailer[i] = 0
		i++

		runGCProg(prog, &trailer[0], h.bitp, 2)

		// Even though we filled in the full array just now,
		// record that we only filled in up to the ptrdata of the
		// last element. This will cause the code below to
		// memclr the dead section of the final array element,
		// so that scanobject can stop early in the final element.
		totalBits = (elemSize*(count-1) + progSize) / ptrSize
	}
	endProg := unsafe.Pointer(subtractb(h.bitp, (totalBits+3)/4))
	endAlloc := unsafe.Pointer(subtractb(h.bitp, allocSize/heapBitmapScale))
	memclr(add(endAlloc, 1), uintptr(endProg)-uintptr(endAlloc))
}

// progToPointerMask返回1bit的指针mask输出
// progToPointerMask returns the 1-bit pointer mask output by the GC program prog.
// size the size of the region described by prog, in bytes.
// The resulting bitvector will have no more than size/ptrSize bits.
func progToPointerMask(prog *byte, size uintptr) bitvector {
	n := (size/ptrSize + 7) / 8 // 首先将size按照8个字节对齐
	x := (*[1 << 30]byte)(persistentalloc(n+1, 1, &memstats.buckhash_sys))[:n+1]
	x[len(x)-1] = 0xa1 // overflow check sentinel
	n = runGCProg(prog, nil, &x[0], 1)
	if x[len(x)-1] != 0xa1 {
		throw("progToPointerMask: overflow")
	}
	return bitvector{int32(n), &x[0]}
}

// Packed GC pointer bitmaps, aka GC programs.
//
// For large types containing arrays, the type information has a
// natural repetition that can be encoded to save space in the
// binary and in the memory representation of the type information.
//
// The encoding is a simple Lempel-Ziv style bytecode machine
// with the following instructions:
//
//	00000000: stop
//	0nnnnnnn: emit n bits copied from the next (n+7)/8 bytes
//	10000000 n c: repeat the previous n bits c times; n, c are varints
//	1nnnnnnn c: repeat the previous n bits c times; c is a varint

// runGCProg executes the GC program prog, and then trailer if non-nil,
// writing to dst with entries of the given size.
// If size == 1, dst is a 1-bit pointer mask laid out moving forward from dst.
// If size == 2, dst is the 2-bit heap bitmap, and writes move backward
// starting at dst (because the heap bitmap does). In this case, the caller guarantees
// that only whole bytes in dst need to be written.
//
// runGCProg returns the number of 1- or 2-bit entries written to memory.
func runGCProg(prog, trailer, dst *byte, size int) uintptr {
	dstStart := dst

	// Bits waiting to be written to memory.
	var bits uintptr
	var nbits uintptr

	p := prog
Run:
	for {
		// Flush accumulated full bytes.
		// The rest of the loop assumes that nbits <= 7.
		for ; nbits >= 8; nbits -= 8 {
			if size == 1 {
				*dst = uint8(bits)
				dst = add1(dst)
				bits >>= 8
			} else {
				v := bits&bitPointerAll | bitMarkedAll
				*dst = uint8(v)
				dst = subtract1(dst)
				bits >>= 4
				v = bits&bitPointerAll | bitMarkedAll
				*dst = uint8(v)
				dst = subtract1(dst)
				bits >>= 4
			}
		}

		// Process one instruction.
		inst := uintptr(*p)
		p = add1(p)
		n := inst & 0x7F
		if inst&0x80 == 0 {
			// Literal bits; n == 0 means end of program.
			if n == 0 {
				// Program is over; continue in trailer if present.
				if trailer != nil {
					//println("trailer")
					p = trailer
					trailer = nil
					continue
				}
				//println("done")
				break Run
			}
			//println("lit", n, dst)
			nbyte := n / 8
			for i := uintptr(0); i < nbyte; i++ {
				bits |= uintptr(*p) << nbits
				p = add1(p)
				if size == 1 {
					*dst = uint8(bits)
					dst = add1(dst)
					bits >>= 8
				} else {
					v := bits&0xf | bitMarkedAll
					*dst = uint8(v)
					dst = subtract1(dst)
					bits >>= 4
					v = bits&0xf | bitMarkedAll
					*dst = uint8(v)
					dst = subtract1(dst)
					bits >>= 4
				}
			}
			if n %= 8; n > 0 {
				bits |= uintptr(*p) << nbits
				p = add1(p)
				nbits += n
			}
			continue Run
		}

		// Repeat. If n == 0, it is encoded in a varint in the next bytes.
		if n == 0 {
			for off := uint(0); ; off += 7 {
				x := uintptr(*p)
				p = add1(p)
				n |= (x & 0x7F) << off
				if x&0x80 == 0 {
					break
				}
			}
		}

		// Count is encoded in a varint in the next bytes.
		c := uintptr(0)
		for off := uint(0); ; off += 7 {
			x := uintptr(*p)
			p = add1(p)
			c |= (x & 0x7F) << off
			if x&0x80 == 0 {
				break
			}
		}
		c *= n // now total number of bits to copy

		// If the number of bits being repeated is small, load them
		// into a register and use that register for the entire loop
		// instead of repeatedly reading from memory.
		// Handling fewer than 8 bits here makes the general loop simpler.
		// The cutoff is ptrSize*8 - 7 to guarantee that when we add
		// the pattern to a bit buffer holding at most 7 bits (a partial byte)
		// it will not overflow.
		src := dst
		const maxBits = ptrSize*8 - 7
		if n <= maxBits {
			// Start with bits in output buffer.
			pattern := bits
			npattern := nbits

			// If we need more bits, fetch them from memory.
			if size == 1 {
				src = subtract1(src)
				for npattern < n {
					pattern <<= 8
					pattern |= uintptr(*src)
					src = subtract1(src)
					npattern += 8
				}
			} else {
				src = add1(src)
				for npattern < n {
					pattern <<= 4
					pattern |= uintptr(*src) & 0xf
					src = add1(src)
					npattern += 4
				}
			}

			// We started with the whole bit output buffer,
			// and then we loaded bits from whole bytes.
			// Either way, we might now have too many instead of too few.
			// Discard the extra.
			if npattern > n {
				pattern >>= npattern - n
				npattern = n
			}

			// Replicate pattern to at most maxBits.
			if npattern == 1 {
				// One bit being repeated.
				// If the bit is 1, make the pattern all 1s.
				// If the bit is 0, the pattern is already all 0s,
				// but we can claim that the number of bits
				// in the word is equal to the number we need (c),
				// because right shift of bits will zero fill.
				if pattern == 1 {
					pattern = 1<<maxBits - 1
					npattern = maxBits
				} else {
					npattern = c
				}
			} else {
				b := pattern
				nb := npattern
				if nb+nb <= maxBits {
					// Double pattern until the whole uintptr is filled.
					for nb <= ptrSize*8 {
						b |= b << nb
						nb += nb
					}
					// Trim away incomplete copy of original pattern in high bits.
					// TODO(rsc): Replace with table lookup or loop on systems without divide?
					nb = maxBits / npattern * npattern
					b &= 1<<nb - 1
					pattern = b
					npattern = nb
				}
			}

			// Add pattern to bit buffer and flush bit buffer, c/npattern times.
			// Since pattern contains >8 bits, there will be full bytes to flush
			// on each iteration.
			for ; c >= npattern; c -= npattern {
				bits |= pattern << nbits
				nbits += npattern
				if size == 1 {
					for nbits >= 8 {
						*dst = uint8(bits)
						dst = add1(dst)
						bits >>= 8
						nbits -= 8
					}
				} else {
					for nbits >= 4 {
						*dst = uint8(bits&0xf | bitMarkedAll)
						dst = subtract1(dst)
						bits >>= 4
						nbits -= 4
					}
				}
			}

			// Add final fragment to bit buffer.
			if c > 0 {
				pattern &= 1<<c - 1
				bits |= pattern << nbits
				nbits += c
			}
			continue Run
		}

		// Repeat; n too large to fit in a register.
		// Since nbits <= 7, we know the first few bytes of repeated data
		// are already written to memory.
		off := n - nbits // n > nbits because n > maxBits and nbits <= 7
		if size == 1 {
			// Leading src fragment.
			src = subtractb(src, (off+7)/8)
			if frag := off & 7; frag != 0 {
				bits |= uintptr(*src) >> (8 - frag) << nbits
				src = add1(src)
				nbits += frag
				c -= frag
			}
			// Main loop: load one byte, write another.
			// The bits are rotating through the bit buffer.
			for i := c / 8; i > 0; i-- {
				bits |= uintptr(*src) << nbits
				src = add1(src)
				*dst = uint8(bits)
				dst = add1(dst)
				bits >>= 8
			}
			// Final src fragment.
			if c %= 8; c > 0 {
				bits |= (uintptr(*src) & (1<<c - 1)) << nbits
				nbits += c
			}
		} else {
			// Leading src fragment.
			src = addb(src, (off+3)/4)
			if frag := off & 3; frag != 0 {
				bits |= (uintptr(*src) & 0xf) >> (4 - frag) << nbits
				src = subtract1(src)
				nbits += frag
				c -= frag
			}
			// Main loop: load one byte, write another.
			// The bits are rotating through the bit buffer.
			for i := c / 4; i > 0; i-- {
				bits |= (uintptr(*src) & 0xf) << nbits
				src = subtract1(src)
				*dst = uint8(bits&0xf | bitMarkedAll)
				dst = subtract1(dst)
				bits >>= 4
			}
			// Final src fragment.
			if c %= 4; c > 0 {
				bits |= (uintptr(*src) & (1<<c - 1)) << nbits
				nbits += c
			}
		}
	}

	// Write any final bits out, using full-byte writes, even for the final byte.
	var totalBits uintptr
	if size == 1 {
		totalBits = (uintptr(unsafe.Pointer(dst))-uintptr(unsafe.Pointer(dstStart)))*8 + nbits
		nbits += -nbits & 7
		for ; nbits > 0; nbits -= 8 {
			*dst = uint8(bits)
			dst = add1(dst)
			bits >>= 8
		}
	} else {
		totalBits = (uintptr(unsafe.Pointer(dstStart))-uintptr(unsafe.Pointer(dst)))*4 + nbits
		nbits += -nbits & 3
		for ; nbits > 0; nbits -= 4 {
			v := bits&0xf | bitMarkedAll
			*dst = uint8(v)
			dst = subtract1(dst)
			bits >>= 4
		}
		// Clear the mark bits in the first two entries.
		// They are the actual mark and checkmark bits,
		// not non-dead markers. It simplified the code
		// above to set the marker in every bit written and
		// then clear these two as a special case at the end.
		*dstStart &^= bitMarked | bitMarked<<heapBitsShift
	}
	return totalBits
}

func dumpGCProg(p *byte) {
	nptr := 0
	for {
		x := *p
		p = add1(p)
		if x == 0 {
			print("\t", nptr, " end\n")
			break
		}
		if x&0x80 == 0 {
			print("\t", nptr, " lit ", x, ":")
			n := int(x+7) / 8
			for i := 0; i < n; i++ {
				print(" ", hex(*p))
				p = add1(p)
			}
			print("\n")
			nptr += int(x)
		} else {
			nbit := int(x &^ 0x80)
			if nbit == 0 {
				for nb := uint(0); ; nb += 7 {
					x := *p
					p = add1(p)
					nbit |= int(x&0x7f) << nb
					if x&0x80 == 0 {
						break
					}
				}
			}
			count := 0
			for nb := uint(0); ; nb += 7 {
				x := *p
				p = add1(p)
				count |= int(x&0x7f) << nb
				if x&0x80 == 0 {
					break
				}
			}
			print("\t", nptr, " repeat ", nbit, " × ", count, "\n")
			nptr += nbit * count
		}
	}
}

// Testing.

func getgcmaskcb(frame *stkframe, ctxt unsafe.Pointer) bool {
	target := (*stkframe)(ctxt)
	if frame.sp <= target.sp && target.sp < frame.varp {
		*target = *frame
		return false
	}
	return true
}

// gcbits returns the GC type info for x, for testing.
// The result is the bitmap entries (0 or 1), one entry per byte.
//go:linkname reflect_gcbits reflect.gcbits
func reflect_gcbits(x interface{}) []byte {
	ret := getgcmask(x)
	typ := (*ptrtype)(unsafe.Pointer((*eface)(unsafe.Pointer(&x))._type)).elem
	nptr := typ.ptrdata / ptrSize
	for uintptr(len(ret)) > nptr && ret[len(ret)-1] == 0 {
		ret = ret[:len(ret)-1]
	}
	return ret
}

// Returns GC type info for object p for testing.
func getgcmask(ep interface{}) (mask []byte) {
	e := *(*eface)(unsafe.Pointer(&ep))
	p := e.data
	t := e._type
	// data or bss
	for datap := &firstmoduledata; datap != nil; datap = datap.next {
		// data
		if datap.data <= uintptr(p) && uintptr(p) < datap.edata {
			bitmap := datap.gcdatamask.bytedata
			n := (*ptrtype)(unsafe.Pointer(t)).elem.size
			mask = make([]byte, n/ptrSize)
			for i := uintptr(0); i < n; i += ptrSize {
				off := (uintptr(p) + i - datap.data) / ptrSize
				mask[i/ptrSize] = (*addb(bitmap, off/8) >> (off % 8)) & 1
			}
			return
		}

		// bss
		if datap.bss <= uintptr(p) && uintptr(p) < datap.ebss {
			bitmap := datap.gcbssmask.bytedata
			n := (*ptrtype)(unsafe.Pointer(t)).elem.size
			mask = make([]byte, n/ptrSize)
			for i := uintptr(0); i < n; i += ptrSize {
				off := (uintptr(p) + i - datap.bss) / ptrSize
				mask[i/ptrSize] = (*addb(bitmap, off/8) >> (off % 8)) & 1
			}
			return
		}
	}

	// heap
	var n uintptr
	var base uintptr
	if mlookup(uintptr(p), &base, &n, nil) != 0 {
		mask = make([]byte, n/ptrSize)
		for i := uintptr(0); i < n; i += ptrSize {
			hbits := heapBitsForAddr(base + i)
			if hbits.isPointer() {
				mask[i/ptrSize] = 1
			}
			if i >= 2*ptrSize && !hbits.isMarked() {
				mask = mask[:i/ptrSize]
				break
			}
		}
		return
	}

	// stack
	if _g_ := getg(); _g_.m.curg.stack.lo <= uintptr(p) && uintptr(p) < _g_.m.curg.stack.hi {
		var frame stkframe
		frame.sp = uintptr(p)
		_g_ := getg()
		gentraceback(_g_.m.curg.sched.pc, _g_.m.curg.sched.sp, 0, _g_.m.curg, 0, nil, 1000, getgcmaskcb, noescape(unsafe.Pointer(&frame)), 0)
		if frame.fn != nil {
			f := frame.fn
			targetpc := frame.continpc
			if targetpc == 0 {
				return
			}
			if targetpc != f.entry {
				targetpc--
			}
			pcdata := pcdatavalue(f, _PCDATA_StackMapIndex, targetpc)
			if pcdata == -1 {
				return
			}
			stkmap := (*stackmap)(funcdata(f, _FUNCDATA_LocalsPointerMaps))
			if stkmap == nil || stkmap.n <= 0 {
				return
			}
			bv := stackmapdata(stkmap, pcdata)
			size := uintptr(bv.n) * ptrSize
			n := (*ptrtype)(unsafe.Pointer(t)).elem.size
			mask = make([]byte, n/ptrSize)
			for i := uintptr(0); i < n; i += ptrSize {
				bitmap := bv.bytedata
				off := (uintptr(p) + i - frame.varp + size) / ptrSize
				mask[i/ptrSize] = (*addb(bitmap, off/8) >> (off % 8)) & 1
			}
		}
		return
	}

	// otherwise, not something the GC knows about.
	// possibly read-only data, like malloc(0).
	// must not have pointers
	return
}
