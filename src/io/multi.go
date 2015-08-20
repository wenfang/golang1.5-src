// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package io

type multiReader struct { // MultiReader结构，实现Reader接口，封装了多个Reader
	readers []Reader
}

func (mr *multiReader) Read(p []byte) (n int, err error) { // 依此从每个Reader中读取，读完一个再读下一个
	for len(mr.readers) > 0 {
		n, err = mr.readers[0].Read(p) // 先读列表中第一个
		if n > 0 || err != EOF {       // 读到数据，返回，直到第一个读完，再顺序读第二个
			if err == EOF {
				// Don't return EOF yet. There may be more bytes
				// in the remaining readers.
				err = nil
			}
			return
		}
		mr.readers = mr.readers[1:] // 从reader slice中去掉第一个
	}
	return 0, EOF
}

// MultiReader returns a Reader that's the logical concatenation of
// the provided input readers.  They're read sequentially.  Once all
// inputs have returned EOF, Read will return EOF.  If any of the readers
// return a non-nil, non-EOF error, Read will return that error.
func MultiReader(readers ...Reader) Reader { // 创建MultiReader结构
	r := make([]Reader, len(readers))
	copy(r, readers)
	return &multiReader{r}
}

type multiWriter struct { // multiWriter结构，多个writer的封装
	writers []Writer
}

func (t *multiWriter) Write(p []byte) (n int, err error) { // multiwriter，依此对所有的writer写
	for _, w := range t.writers { // 遍历所有的writer，每个写入p，一个出错则整体出错
		n, err = w.Write(p)
		if err != nil {
			return
		}
		if n != len(p) {
			err = ErrShortWrite
			return
		}
	}
	return len(p), nil
}

// MultiWriter creates a writer that duplicates its writes to all the
// provided writers, similar to the Unix tee(1) command.
func MultiWriter(writers ...Writer) Writer { // 创建MultiWriter结构
	w := make([]Writer, len(writers))
	copy(w, writers)
	return &multiWriter{w}
}
