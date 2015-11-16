// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build darwin dragonfly freebsd linux nacl netbsd openbsd solaris

package mime

import (
	"bufio"
	"os"
	"strings"
)

func init() {
	osInitMime = initMimeUnix
}

var typeFiles = []string{
	"/etc/mime.types",
	"/etc/apache2/mime.types",
	"/etc/apache/mime.types",
}

func loadMimeFile(filename string) { // 装载MIME文件
	f, err := os.Open(filename) // 打开文件
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f) // 创建一个Scanner，缺省为ScanLines
	for scanner.Scan() {           // 获取一行数据
		fields := strings.Fields(scanner.Text())
		if len(fields) <= 1 || fields[0][0] == '#' { // 略过空行和注释行
			continue
		}
		mimeType := fields[0]            // 获得mime类型
		for _, ext := range fields[1:] { // 获得扩展名
			if ext[0] == '#' {
				break
			}
			setExtensionType("."+ext, mimeType) // 设置扩展名和mime类型
		}
	}
	if err := scanner.Err(); err != nil { // 并不是文件到结束了，panic
		panic(err)
	}
}

func initMimeUnix() {
	for _, filename := range typeFiles { // 装载每个Mime文件
		loadMimeFile(filename)
	}
}

func initMimeForTests() map[string]string {
	typeFiles = []string{"testdata/test.types"}
	return map[string]string{
		".T1":  "application/test",
		".t2":  "text/test; charset=utf-8",
		".png": "image/png",
	}
}
