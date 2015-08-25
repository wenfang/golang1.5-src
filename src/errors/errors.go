// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package errors implements functions to manipulate errors.
package errors

// New returns an error that formats as the given text.
func New(text string) error { // 创建新的error类型
	return &errorString{text}
}

// errorString is a trivial implementation of error.
type errorString struct { // 内嵌的errorString结构，实现了Error方法
	s string
}

func (e *errorString) Error() string { // 直接返回字符串
	return e.s
}
