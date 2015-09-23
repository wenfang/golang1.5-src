// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Runtime type representation. 运行时的类型表示

package runtime

import "unsafe"

// Needs to be in sync with ../cmd/internal/ld/decodesym.go:/^func.commonsize,
// ../cmd/internal/gc/reflect.go:/^func.dcommontype and
// ../reflect/type.go:/^type.rtype.
type _type struct { // 类型结构
	size       uintptr
	ptrdata    uintptr // size of memory prefix holding all pointers
	hash       uint32
	_unused    uint8
	align      uint8
	fieldalign uint8
	kind       uint8
	alg        *typeAlg
	// gcdata stores the GC type data for the garbage collector.
	// If the KindGCProg bit is set in kind, gcdata is a GC program.
	// Otherwise it is a ptrmask bitmap. See mbitmap.go for details.
	gcdata  *byte
	_string *string
	x       *uncommontype
	ptrto   *_type
	zero    *byte // ptr to the zero value for this type 指向该类型的0值
}

type method struct { // 方法结构
	name    *string // 方法名
	pkgpath *string // 包路径
	mtyp    *_type
	typ     *_type
	ifn     unsafe.Pointer
	tfn     unsafe.Pointer
}

type uncommontype struct {
	name    *string
	pkgpath *string
	mhdr    []method
}

type imethod struct { // 接口方法结构
	name    *string
	pkgpath *string
	_type   *_type
}

type interfacetype struct { // 接口类型的结构
	typ  _type
	mhdr []imethod // 接口方法的slice
}

type maptype struct { // map类型结构
	typ           _type
	key           *_type
	elem          *_type
	bucket        *_type // internal type representing a hash bucket
	hmap          *_type // internal type representing a hmap
	keysize       uint8  // size of key slot
	indirectkey   bool   // store ptr to key instead of key itself
	valuesize     uint8  // size of value slot
	indirectvalue bool   // store ptr to value instead of value itself
	bucketsize    uint16 // size of bucket
	reflexivekey  bool   // true if k==k for all keys
}

type chantype struct { // chan类型
	typ  _type
	elem *_type
	dir  uintptr
}

type slicetype struct { // slice类型
	typ  _type
	elem *_type
}

type functype struct { // 函数类型
	typ       _type
	dotdotdot bool
	in        slice
	out       slice
}

type ptrtype struct { // 指针类型
	typ  _type
	elem *_type
}
