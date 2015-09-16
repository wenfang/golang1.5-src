// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package ring implements operations on circular lists.
package ring

// A Ring is an element of a circular list, or ring.
// Rings do not have a beginning or end; a pointer to any ring element
// serves as reference to the entire ring. Empty rings are represented
// as nil Ring pointers. The zero value for a Ring is a one-element
// ring with a nil Value.
//
type Ring struct { // 环的结构
	next, prev *Ring       // 环的双向指针
	Value      interface{} // for use by client; untouched by this library
}

func (r *Ring) init() *Ring { // 初始化一个Ring，指向自身
	r.next = r
	r.prev = r
	return r
}

// Next returns the next ring element. r must not be empty.
func (r *Ring) Next() *Ring { // 获得后一个环结构
	if r.next == nil { // 如果next指针为nil，先进行初始化
		return r.init()
	}
	return r.next
}

// Prev returns the previous ring element. r must not be empty.
func (r *Ring) Prev() *Ring { // 获得前一个环结构
	if r.next == nil {
		return r.init()
	}
	return r.prev
}

// Move moves n % r.Len() elements backward (n < 0) or forward (n >= 0)
// in the ring and returns that ring element. r must not be empty.
//
func (r *Ring) Move(n int) *Ring { // 向前或向后移动n个
	if r.next == nil {
		return r.init()
	}
	switch {
	case n < 0:
		for ; n < 0; n++ {
			r = r.prev
		}
	case n > 0:
		for ; n > 0; n-- {
			r = r.next
		}
	}
	return r
}

// New creates a ring of n elements.
func New(n int) *Ring { // 创建一个新的环，n为初始元素的数量
	if n <= 0 {
		return nil
	}
	r := new(Ring)
	p := r
	for i := 1; i < n; i++ { // 创建环结构
		p.next = &Ring{prev: p}
		p = p.next
	}
	p.next = r
	r.prev = p
	return r
}

// Link connects ring r with ring s such that r.Next()
// becomes s and returns the original value for r.Next().
// r must not be empty.
//
// If r and s point to the same ring, linking
// them removes the elements between r and s from the ring.
// The removed elements form a subring and the result is a
// reference to that subring (if no elements were removed,
// the result is still the original value for r.Next(),
// and not nil).
//
// If r and s point to different rings, linking
// them creates a single ring with the elements of s inserted
// after r. The result points to the element following the
// last element of s after insertion.
//
func (r *Ring) Link(s *Ring) *Ring { // 将s加入环中，返回r原来的next元素
	n := r.Next()
	if s != nil {
		p := s.Prev()
		// Note: Cannot use multiple assignment because
		// evaluation order of LHS is not specified.
		r.next = s
		s.prev = r
		n.prev = p
		p.next = n
	}
	return n
}

// Unlink removes n % r.Len() elements from the ring r, starting
// at r.Next(). If n % r.Len() == 0, r remains unchanged.
// The result is the removed subring. r must not be empty.
//
func (r *Ring) Unlink(n int) *Ring { // 删除环中n个元素
	if n <= 0 {
		return nil
	}
	return r.Link(r.Move(n + 1))
}

// Len computes the number of elements in ring r.
// It executes in time proportional to the number of elements.
//
func (r *Ring) Len() int { // 计算Ring的长度
	n := 0
	if r != nil {
		n = 1
		for p := r.Next(); p != r; p = p.next {
			n++
		}
	}
	return n
}

// Do calls function f on each element of the ring, in forward order.
// The behavior of Do is undefined if f changes *r.
func (r *Ring) Do(f func(interface{})) { // 对每个元素循环调用f
	if r != nil { // 如果当前环非空
		f(r.Value)
		for p := r.Next(); p != r; p = p.next {
			f(p.Value)
		}
	}
}
