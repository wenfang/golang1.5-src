// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Linux-specific

package os

func hostname() (name string, err error) { // 取出主机名
	f, err := Open("/proc/sys/kernel/hostname") // 从proc/sys/kernel/hostname中取出主机名
	if err != nil {
		return "", err
	}
	defer f.Close()

	var buf [512]byte         // Enough for a DNS name.
	n, err := f.Read(buf[0:]) // 读数据到buffer中
	if err != nil {
		return "", err
	}

	if n > 0 && buf[n-1] == '\n' { // 去掉最后一个\n
		n--
	}
	return string(buf[0:n]), nil
}
