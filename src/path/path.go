// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package path implements utility routines for manipulating slash-separated
// paths.
package path

import (
	"strings"
)

// A lazybuf is a lazily constructed path buffer.
// It supports append, reading previously appended bytes,
// and retrieving the final string. It does not allocate a buffer
// to hold the output until that output diverges from s.
type lazybuf struct {
	s   string
	buf []byte
	w   int // 当前写到的位置
}

func (b *lazybuf) index(i int) byte { // 查找buf中i位置的byte，如果buf为空，返回string中i位置的byte
	if b.buf != nil {
		return b.buf[i]
	}
	return b.s[i]
}

func (b *lazybuf) append(c byte) { // 向buf中添加byte c
	if b.buf == nil { // 如果buffer为空
		if b.w < len(b.s) && b.s[b.w] == c { // 如果c与b.s中对应位置的字符相同，还是使用b.s
			b.w++
			return
		}
		b.buf = make([]byte, len(b.s)) // 创建一个buffer，长度为b.s
		copy(b.buf, b.s[:b.w])         // 将string中前b.w个字符拷贝过来
	}
	b.buf[b.w] = c
	b.w++
}

func (b *lazybuf) string() string { // 返回转化后的字符串
	if b.buf == nil {
		return b.s[:b.w]
	}
	return string(b.buf[:b.w])
}

// Clean returns the shortest path name equivalent to path
// by purely lexical processing.  It applies the following rules
// iteratively until no further processing can be done:
//
//	1. Replace multiple slashes with a single slash.
//	2. Eliminate each . path name element (the current directory).
//	3. Eliminate each inner .. path name element (the parent directory)
//	   along with the non-.. element that precedes it.
//	4. Eliminate .. elements that begin a rooted path:
//	   that is, replace "/.." by "/" at the beginning of a path.
//
// The returned path ends in a slash only if it is the root "/".
//
// If the result of this process is an empty string, Clean
// returns the string ".".
//
// See also Rob Pike, ``Lexical File Names in Plan 9 or
// Getting Dot-Dot Right,''
// http://plan9.bell-labs.com/sys/doc/lexnames.html
func Clean(path string) string { // 返回与path相同的最短路径字符串
	if path == "" { // 如果path为空，返回当前目录
		return "."
	}

	rooted := path[0] == '/' // 查看是否是以/开头的绝对路径
	n := len(path)           // 获得路径的长度

	// Invariants:
	//	reading from path; r is index of next byte to process.
	//	writing to buf; w is index of next byte to write.
	//	dotdot is index in buf where .. must stop, either because
	//		it is the leading slash or it is a leading ../../.. prefix.
	out := lazybuf{s: path} // 创建一个lazybuf结构
	r, dotdot := 0, 0
	if rooted {
		out.append('/')
		r, dotdot = 1, 1
	}

	for r < n {
		switch {
		case path[r] == '/':
			// empty path element
			r++
		case path[r] == '.' && (r+1 == n || path[r+1] == '/'):
			// . element
			r++
		case path[r] == '.' && path[r+1] == '.' && (r+2 == n || path[r+2] == '/'):
			// .. element: remove to last /
			r += 2
			switch {
			case out.w > dotdot:
				// can backtrack
				out.w--
				for out.w > dotdot && out.index(out.w) != '/' {
					out.w--
				}
			case !rooted:
				// cannot backtrack, but not rooted, so append .. element.
				if out.w > 0 {
					out.append('/')
				}
				out.append('.')
				out.append('.')
				dotdot = out.w
			}
		default:
			// real path element.
			// add slash if needed
			if rooted && out.w != 1 || !rooted && out.w != 0 {
				out.append('/')
			}
			// copy element
			for ; r < n && path[r] != '/'; r++ {
				out.append(path[r])
			}
		}
	}

	// Turn empty string into "."
	if out.w == 0 {
		return "."
	}

	return out.string()
}

// Split splits path immediately following the final slash,
// separating it into a directory and file name component.
// If there is no slash path, Split returns an empty dir and
// file set to path.
// The returned values have the property that path = dir+file.
func Split(path string) (dir, file string) {
	i := strings.LastIndex(path, "/")
	return path[:i+1], path[i+1:]
}

// Join joins any number of path elements into a single path, adding a
// separating slash if necessary. The result is Cleaned; in particular,
// all empty strings are ignored.
func Join(elem ...string) string {
	for i, e := range elem {
		if e != "" {
			return Clean(strings.Join(elem[i:], "/"))
		}
	}
	return ""
}

// Ext returns the file name extension used by path.
// The extension is the suffix beginning at the final dot
// in the final slash-separated element of path;
// it is empty if there is no dot.
func Ext(path string) string {
	for i := len(path) - 1; i >= 0 && path[i] != '/'; i-- {
		if path[i] == '.' {
			return path[i:]
		}
	}
	return ""
}

// Base returns the last element of path.
// Trailing slashes are removed before extracting the last element.
// If the path is empty, Base returns ".".
// If the path consists entirely of slashes, Base returns "/".
func Base(path string) string { // 路径中最后一级的名称
	if path == "" {
		return "."
	}
	// Strip trailing slashes.
	for len(path) > 0 && path[len(path)-1] == '/' { // 去掉位于最后的/
		path = path[0 : len(path)-1]
	}
	// Find the last element
	if i := strings.LastIndex(path, "/"); i >= 0 { // 查找最后一个/
		path = path[i+1:]
	}
	// If empty now, it had only slashes.
	if path == "" {
		return "/"
	}
	return path
}

// IsAbs reports whether the path is absolute.
func IsAbs(path string) bool { // 判断路径path是否为绝对路径
	return len(path) > 0 && path[0] == '/'
}

// Dir returns all but the last element of path, typically the path's directory.
// After dropping the final element using Split, the path is Cleaned and trailing
// slashes are removed.
// If the path is empty, Dir returns ".".
// If the path consists entirely of slashes followed by non-slash bytes, Dir
// returns a single slash. In any other case, the returned path does not end in a
// slash.
func Dir(path string) string { // 返回目录名
	dir, _ := Split(path)
	return Clean(dir)
}
