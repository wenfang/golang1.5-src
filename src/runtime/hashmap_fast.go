// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"unsafe"
)

func mapaccess1_fast32(t *maptype, h *hmap, key uint32) unsafe.Pointer { // key为uint32，第一种访问的情况，只返回元素
	if raceenabled && h != nil {
		callerpc := getcallerpc(unsafe.Pointer(&t))
		racereadpc(unsafe.Pointer(h), callerpc, funcPC(mapaccess1_fast32))
	}
	if h == nil || h.count == 0 { // 如果hashmap结构为空或者不包含元素，返回0元素
		return unsafe.Pointer(t.elem.zero)
	}
	var b *bmap
	if h.B == 0 { // 只有一个bucket的表，不需要进行hash
		// One-bucket table.  No need to hash.
		b = (*bmap)(h.buckets)
	} else {
		hash := t.key.alg.hash(noescape(unsafe.Pointer(&key)), uintptr(h.hash0))
		m := uintptr(1)<<h.B - 1
		b = (*bmap)(add(h.buckets, (hash&m)*uintptr(t.bucketsize))) // 查找到对应的buckets
		if c := h.oldbuckets; c != nil {                            // 如果正在grow
			oldb := (*bmap)(add(c, (hash&(m>>1))*uintptr(t.bucketsize)))
			if !evacuated(oldb) {
				b = oldb
			}
		}
	}
	for {
		for i := uintptr(0); i < bucketCnt; i++ {
			k := *((*uint32)(add(unsafe.Pointer(b), dataOffset+i*4)))
			if k != key { // 如果key不相同，查找下一个
				continue
			}
			x := *((*uint8)(add(unsafe.Pointer(b), i))) // b.topbits[i] without the bounds check 获取对应位置的tophash的值
			if x == empty {                             // 如果对应位置为空，继续查找下一个
				continue
			}
			return add(unsafe.Pointer(b), dataOffset+bucketCnt*4+i*uintptr(t.valuesize)) // 返回对应的value值
		}
		b = b.overflow(t)
		if b == nil {  // 没有bucket了，返回0元素
			return unsafe.Pointer(t.elem.zero)
		}
	}
}

func mapaccess2_fast32(t *maptype, h *hmap, key uint32) (unsafe.Pointer, bool) { // key为uint32，第二种访问的情况，返回元素和是否有效
	if raceenabled && h != nil {
		callerpc := getcallerpc(unsafe.Pointer(&t))
		racereadpc(unsafe.Pointer(h), callerpc, funcPC(mapaccess2_fast32))
	}
	if h == nil || h.count == 0 { // 如果hmap为nil，或map中没有元素，返回0元素和false
		return unsafe.Pointer(t.elem.zero), false
	}
	var b *bmap
	if h.B == 0 {
		// One-bucket table.  No need to hash.
		b = (*bmap)(h.buckets)
	} else {
		hash := t.key.alg.hash(noescape(unsafe.Pointer(&key)), uintptr(h.hash0))
		m := uintptr(1)<<h.B - 1
		b = (*bmap)(add(h.buckets, (hash&m)*uintptr(t.bucketsize)))
		if c := h.oldbuckets; c != nil {
			oldb := (*bmap)(add(c, (hash&(m>>1))*uintptr(t.bucketsize)))
			if !evacuated(oldb) {
				b = oldb
			}
		}
	}
	for {
		for i := uintptr(0); i < bucketCnt; i++ {
			k := *((*uint32)(add(unsafe.Pointer(b), dataOffset+i*4)))
			if k != key {
				continue
			}
			x := *((*uint8)(add(unsafe.Pointer(b), i))) // b.topbits[i] without the bounds check
			if x == empty {
				continue
			}
			return add(unsafe.Pointer(b), dataOffset+bucketCnt*4+i*uintptr(t.valuesize)), true
		}
		b = b.overflow(t)
		if b == nil {
			return unsafe.Pointer(t.elem.zero), false
		}
	}
}

func mapaccess1_fast64(t *maptype, h *hmap, key uint64) unsafe.Pointer { // key为uint64第一种访问的情况
	if raceenabled && h != nil {
		callerpc := getcallerpc(unsafe.Pointer(&t))
		racereadpc(unsafe.Pointer(h), callerpc, funcPC(mapaccess1_fast64))
	}
	if h == nil || h.count == 0 {
		return unsafe.Pointer(t.elem.zero)
	}
	var b *bmap
	if h.B == 0 {
		// One-bucket table.  No need to hash.
		b = (*bmap)(h.buckets)
	} else {
		hash := t.key.alg.hash(noescape(unsafe.Pointer(&key)), uintptr(h.hash0))
		m := uintptr(1)<<h.B - 1
		b = (*bmap)(add(h.buckets, (hash&m)*uintptr(t.bucketsize)))
		if c := h.oldbuckets; c != nil {
			oldb := (*bmap)(add(c, (hash&(m>>1))*uintptr(t.bucketsize)))
			if !evacuated(oldb) {
				b = oldb
			}
		}
	}
	for {
		for i := uintptr(0); i < bucketCnt; i++ { // 遍历bucket
			k := *((*uint64)(add(unsafe.Pointer(b), dataOffset+i*8))) // 获得对应的key
			if k != key {                                             // 比较key不相同，继续下一个
				continue
			}
			x := *((*uint8)(add(unsafe.Pointer(b), i))) // b.topbits[i] without the bounds check
			if x == empty {
				continue
			}
			return add(unsafe.Pointer(b), dataOffset+bucketCnt*8+i*uintptr(t.valuesize))
		}
		b = b.overflow(t)
		if b == nil {
			return unsafe.Pointer(t.elem.zero)
		}
	}
}

func mapaccess2_fast64(t *maptype, h *hmap, key uint64) (unsafe.Pointer, bool) { // key为uint64第二种访问的情况
	if raceenabled && h != nil {
		callerpc := getcallerpc(unsafe.Pointer(&t))
		racereadpc(unsafe.Pointer(h), callerpc, funcPC(mapaccess2_fast64))
	}
	if h == nil || h.count == 0 {
		return unsafe.Pointer(t.elem.zero), false
	}
	var b *bmap
	if h.B == 0 {
		// One-bucket table.  No need to hash.
		b = (*bmap)(h.buckets)
	} else {
		hash := t.key.alg.hash(noescape(unsafe.Pointer(&key)), uintptr(h.hash0))
		m := uintptr(1)<<h.B - 1
		b = (*bmap)(add(h.buckets, (hash&m)*uintptr(t.bucketsize)))
		if c := h.oldbuckets; c != nil {
			oldb := (*bmap)(add(c, (hash&(m>>1))*uintptr(t.bucketsize)))
			if !evacuated(oldb) {
				b = oldb
			}
		}
	}
	for {
		for i := uintptr(0); i < bucketCnt; i++ {
			k := *((*uint64)(add(unsafe.Pointer(b), dataOffset+i*8)))
			if k != key {
				continue
			}
			x := *((*uint8)(add(unsafe.Pointer(b), i))) // b.topbits[i] without the bounds check
			if x == empty {
				continue
			}
			return add(unsafe.Pointer(b), dataOffset+bucketCnt*8+i*uintptr(t.valuesize)), true
		}
		b = b.overflow(t)
		if b == nil {
			return unsafe.Pointer(t.elem.zero), false
		}
	}
}

func mapaccess1_faststr(t *maptype, h *hmap, ky string) unsafe.Pointer { // key为string，第一种访问情况
	if raceenabled && h != nil {
		callerpc := getcallerpc(unsafe.Pointer(&t))
		racereadpc(unsafe.Pointer(h), callerpc, funcPC(mapaccess1_faststr))
	}
	if h == nil || h.count == 0 {
		return unsafe.Pointer(t.elem.zero)
	}
	key := (*stringStruct)(unsafe.Pointer(&ky)) // 将key转变为stringStruct
	if h.B == 0 {
		// One-bucket table.
		b := (*bmap)(h.buckets)
		if key.len < 32 { // 如果key的长度小于32个字节
			// short key, doing lots of comparisons is ok
			for i := uintptr(0); i < bucketCnt; i++ {
				x := *((*uint8)(add(unsafe.Pointer(b), i))) // b.topbits[i] without the bounds check
				if x == empty {
					continue
				}
				k := (*stringStruct)(add(unsafe.Pointer(b), dataOffset+i*2*ptrSize)) // 获取key的stringStruct指针
				if k.len != key.len {
					continue
				}
				if k.str == key.str || memeq(k.str, key.str, uintptr(key.len)) { // 如果字符串相同
					return add(unsafe.Pointer(b), dataOffset+bucketCnt*2*ptrSize+i*uintptr(t.valuesize)) // 返回value值
				}
			}
			return unsafe.Pointer(t.elem.zero)
		}
		// long key, try not to do more comparisons than necessary 如果key的长度大于32个字节
		keymaybe := uintptr(bucketCnt)
		for i := uintptr(0); i < bucketCnt; i++ {
			x := *((*uint8)(add(unsafe.Pointer(b), i))) // b.topbits[i] without the bounds check
			if x == empty {
				continue
			}
			k := (*stringStruct)(add(unsafe.Pointer(b), dataOffset+i*2*ptrSize))
			if k.len != key.len {
				continue
			}
			if k.str == key.str {
				return add(unsafe.Pointer(b), dataOffset+bucketCnt*2*ptrSize+i*uintptr(t.valuesize))
			}
			// check first 4 bytes
			// TODO: on amd64/386 at least, make this compile to one 4-byte comparison instead of
			// four 1-byte comparisons.
			if *((*[4]byte)(key.str)) != *((*[4]byte)(k.str)) {
				continue
			}
			// check last 4 bytes
			if *((*[4]byte)(add(key.str, uintptr(key.len)-4))) != *((*[4]byte)(add(k.str, uintptr(key.len)-4))) {
				continue
			}
			if keymaybe != bucketCnt {
				// Two keys are potential matches.  Use hash to distinguish them.
				goto dohash
			}
			keymaybe = i
		}
		if keymaybe != bucketCnt {
			k := (*stringStruct)(add(unsafe.Pointer(b), dataOffset+keymaybe*2*ptrSize))
			if memeq(k.str, key.str, uintptr(key.len)) {
				return add(unsafe.Pointer(b), dataOffset+bucketCnt*2*ptrSize+keymaybe*uintptr(t.valuesize))
			}
		}
		return unsafe.Pointer(t.elem.zero)
	}
dohash:
	hash := t.key.alg.hash(noescape(unsafe.Pointer(&ky)), uintptr(h.hash0))
	m := uintptr(1)<<h.B - 1
	b := (*bmap)(add(h.buckets, (hash&m)*uintptr(t.bucketsize)))
	if c := h.oldbuckets; c != nil {
		oldb := (*bmap)(add(c, (hash&(m>>1))*uintptr(t.bucketsize)))
		if !evacuated(oldb) {
			b = oldb
		}
	}
	top := uint8(hash >> (ptrSize*8 - 8))
	if top < minTopHash {
		top += minTopHash
	}
	for {
		for i := uintptr(0); i < bucketCnt; i++ {
			x := *((*uint8)(add(unsafe.Pointer(b), i))) // b.topbits[i] without the bounds check
			if x != top {
				continue
			}
			k := (*stringStruct)(add(unsafe.Pointer(b), dataOffset+i*2*ptrSize))
			if k.len != key.len {
				continue
			}
			if k.str == key.str || memeq(k.str, key.str, uintptr(key.len)) {
				return add(unsafe.Pointer(b), dataOffset+bucketCnt*2*ptrSize+i*uintptr(t.valuesize))
			}
		}
		b = b.overflow(t)
		if b == nil {
			return unsafe.Pointer(t.elem.zero)
		}
	}
}

func mapaccess2_faststr(t *maptype, h *hmap, ky string) (unsafe.Pointer, bool) {
	if raceenabled && h != nil {
		callerpc := getcallerpc(unsafe.Pointer(&t))
		racereadpc(unsafe.Pointer(h), callerpc, funcPC(mapaccess2_faststr))
	}
	if h == nil || h.count == 0 {
		return unsafe.Pointer(t.elem.zero), false
	}
	key := (*stringStruct)(unsafe.Pointer(&ky))
	if h.B == 0 {
		// One-bucket table.
		b := (*bmap)(h.buckets)
		if key.len < 32 {
			// short key, doing lots of comparisons is ok
			for i := uintptr(0); i < bucketCnt; i++ {
				x := *((*uint8)(add(unsafe.Pointer(b), i))) // b.topbits[i] without the bounds check
				if x == empty {
					continue
				}
				k := (*stringStruct)(add(unsafe.Pointer(b), dataOffset+i*2*ptrSize))
				if k.len != key.len {
					continue
				}
				if k.str == key.str || memeq(k.str, key.str, uintptr(key.len)) {
					return add(unsafe.Pointer(b), dataOffset+bucketCnt*2*ptrSize+i*uintptr(t.valuesize)), true
				}
			}
			return unsafe.Pointer(t.elem.zero), false
		}
		// long key, try not to do more comparisons than necessary
		keymaybe := uintptr(bucketCnt)
		for i := uintptr(0); i < bucketCnt; i++ {
			x := *((*uint8)(add(unsafe.Pointer(b), i))) // b.topbits[i] without the bounds check
			if x == empty {
				continue
			}
			k := (*stringStruct)(add(unsafe.Pointer(b), dataOffset+i*2*ptrSize))
			if k.len != key.len {
				continue
			}
			if k.str == key.str {
				return add(unsafe.Pointer(b), dataOffset+bucketCnt*2*ptrSize+i*uintptr(t.valuesize)), true
			}
			// check first 4 bytes
			if *((*[4]byte)(key.str)) != *((*[4]byte)(k.str)) {
				continue
			}
			// check last 4 bytes
			if *((*[4]byte)(add(key.str, uintptr(key.len)-4))) != *((*[4]byte)(add(k.str, uintptr(key.len)-4))) {
				continue
			}
			if keymaybe != bucketCnt {
				// Two keys are potential matches.  Use hash to distinguish them.
				goto dohash
			}
			keymaybe = i
		}
		if keymaybe != bucketCnt {
			k := (*stringStruct)(add(unsafe.Pointer(b), dataOffset+keymaybe*2*ptrSize))
			if memeq(k.str, key.str, uintptr(key.len)) {
				return add(unsafe.Pointer(b), dataOffset+bucketCnt*2*ptrSize+keymaybe*uintptr(t.valuesize)), true
			}
		}
		return unsafe.Pointer(t.elem.zero), false
	}
dohash:
	hash := t.key.alg.hash(noescape(unsafe.Pointer(&ky)), uintptr(h.hash0))
	m := uintptr(1)<<h.B - 1
	b := (*bmap)(add(h.buckets, (hash&m)*uintptr(t.bucketsize)))
	if c := h.oldbuckets; c != nil {
		oldb := (*bmap)(add(c, (hash&(m>>1))*uintptr(t.bucketsize)))
		if !evacuated(oldb) {
			b = oldb
		}
	}
	top := uint8(hash >> (ptrSize*8 - 8))
	if top < minTopHash {
		top += minTopHash
	}
	for {
		for i := uintptr(0); i < bucketCnt; i++ {
			x := *((*uint8)(add(unsafe.Pointer(b), i))) // b.topbits[i] without the bounds check
			if x != top {
				continue
			}
			k := (*stringStruct)(add(unsafe.Pointer(b), dataOffset+i*2*ptrSize))
			if k.len != key.len {
				continue
			}
			if k.str == key.str || memeq(k.str, key.str, uintptr(key.len)) {
				return add(unsafe.Pointer(b), dataOffset+bucketCnt*2*ptrSize+i*uintptr(t.valuesize)), true
			}
		}
		b = b.overflow(t)
		if b == nil {
			return unsafe.Pointer(t.elem.zero), false
		}
	}
}
