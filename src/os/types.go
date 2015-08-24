// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package os

import (
	"syscall"
	"time"
)

// Getpagesize returns the underlying system's memory page size.
func Getpagesize() int { return syscall.Getpagesize() } // 获得系统页面大小

// A FileInfo describes a file and is returned by Stat and Lstat.
type FileInfo interface { //文件信息，是一个接口
	Name() string       // base name of the file // 文件名
	Size() int64        // length in bytes for regular files; system-dependent for others // 文件大小
	Mode() FileMode     // file mode bits // 文件mode位
	ModTime() time.Time // modification time // 文件更改时间
	IsDir() bool        // abbreviation for Mode().IsDir() // 是否是目录
	Sys() interface{}   // underlying data source (can return nil) // 底层的数据源
}

// A FileMode represents a file's mode and permission bits.
// The bits have the same definition on all systems, so that
// information about files can be moved from one system
// to another portably.  Not all bits apply to all systems.
// The only required bit is ModeDir for directories.
type FileMode uint32 // 定义文件权限位

// The defined file mode bits are the most significant bits of the FileMode.
// The nine least-significant bits are the standard Unix rwxrwxrwx permissions.
// The values of these bits should be considered part of the public API and
// may be used in wire protocols or disk representations: they must not be
// changed, although new bits might be added.
const (
	// The single letters are the abbreviations
	// used by the String method's formatting.
	ModeDir        FileMode = 1 << (32 - 1 - iota) // d: is a directory
	ModeAppend                                     // a: append-only
	ModeExclusive                                  // l: exclusive use
	ModeTemporary                                  // T: temporary file (not backed up)
	ModeSymlink                                    // L: symbolic link
	ModeDevice                                     // D: device file
	ModeNamedPipe                                  // p: named pipe (FIFO)
	ModeSocket                                     // S: Unix domain socket
	ModeSetuid                                     // u: setuid
	ModeSetgid                                     // g: setgid
	ModeCharDevice                                 // c: Unix character device, when ModeDevice is set
	ModeSticky                                     // t: sticky

	// Mask for the type bits. For regular files, none will be set.
	ModeType = ModeDir | ModeSymlink | ModeNamedPipe | ModeSocket | ModeDevice

	ModePerm FileMode = 0777 // Unix permission bits
) // 文件模式标志位

func (m FileMode) String() string { // 将文件模式变为字符串
	const str = "dalTLDpSugct" // 文件模式可选的字符串
	var buf [32]byte           // Mode is uint32.
	w := 0
	for i, c := range str {
		if m&(1<<uint(32-1-i)) != 0 {
			buf[w] = byte(c)
			w++
		}
	}
	if w == 0 {
		buf[w] = '-'
		w++
	}
	const rwx = "rwxrwxrwx"
	for i, c := range rwx {
		if m&(1<<uint(9-1-i)) != 0 {
			buf[w] = byte(c)
		} else {
			buf[w] = '-'
		}
		w++
	}
	return string(buf[:w])
}

// IsDir reports whether m describes a directory.
// That is, it tests for the ModeDir bit being set in m.
func (m FileMode) IsDir() bool { // 检查文件是否是一个目录
	return m&ModeDir != 0
}

// IsRegular reports whether m describes a regular file.
// That is, it tests that no mode type bits are set.
func (m FileMode) IsRegular() bool { // 检查是否为普通文件
	return m&ModeType == 0
}

// Perm returns the Unix permission bits in m.
func (m FileMode) Perm() FileMode { // 返回权限位
	return m & ModePerm
}

func (fs *fileStat) Name() string { return fs.name }
func (fs *fileStat) IsDir() bool  { return fs.Mode().IsDir() }

// SameFile reports whether fi1 and fi2 describe the same file.
// For example, on Unix this means that the device and inode fields
// of the two underlying structures are identical; on other systems
// the decision may be based on the path names.
// SameFile only applies to results returned by this package's Stat.
// It returns false in other cases.
func SameFile(fi1, fi2 FileInfo) bool { // 查看fi1和fi2是否为同一个文件
	fs1, ok1 := fi1.(*fileStat)
	fs2, ok2 := fi2.(*fileStat)
	if !ok1 || !ok2 { // 先将FileInfo接口转换为fileStat结构，看是否成功
		return false
	}
	return sameFile(fs1, fs2)
}
