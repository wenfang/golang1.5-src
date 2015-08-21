// Copyright 2009 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package net

import "syscall"

func maxListenerBacklog() int {
	fd, err := open("/proc/sys/net/core/somaxconn") // 打开somaxconn文件
	if err != nil {                                 // 如果打开文件错误，返回SOMAXCONN
		return syscall.SOMAXCONN
	}
	defer fd.close()
	l, ok := fd.readLine() // 读一行数据
	if !ok {
		return syscall.SOMAXCONN
	}
	f := getFields(l)
	n, _, ok := dtoi(f[0], 0) // 转换为数字
	if n == 0 || !ok {
		return syscall.SOMAXCONN
	}
	// Linux stores the backlog in a uint16.
	// Truncate number to avoid wrapping.
	// See issue 5030.
	if n > 1<<16-1 { // 限制不能过大
		n = 1<<16 - 1
	}
	return n
}
