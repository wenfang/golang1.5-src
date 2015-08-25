// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package multipart

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/textproto"
	"os"
)

// TODO(adg,bradfitz): find a way to unify the DoS-prevention strategy here
// with that of the http package's ParseForm.

// ReadForm parses an entire multipart message whose parts have
// a Content-Disposition of "form-data".
// It stores up to maxMemory bytes of the file parts in memory
// and the remainder on disk in temporary files.
func (r *Reader) ReadForm(maxMemory int64) (f *Form, err error) { // 读出一个Form，maxMemory的数据在内存中，其余在硬盘中
	form := &Form{make(map[string][]string), make(map[string][]*FileHeader)} // 创建From结构
	defer func() {                                                           // 创建defer，函数执行完成后，运行RemoveAll
		if err != nil {
			form.RemoveAll()
		}
	}()

	maxValueBytes := int64(10 << 20) // 10 MB is a lot of text.
	for {
		p, err := r.NextPart() // 获得一个part
		if err == io.EOF {     // 如果读出结束，跳出
			break
		}
		if err != nil { // 如果出错，返回
			return nil, err
		}

		name := p.FormName() // 取出form的name
		if name == "" {      // form没有名称，继续读下一个Part
			continue
		}
		filename := p.FileName() // 取出form的filename

		var b bytes.Buffer // 声明一个byte的Buffer

		if filename == "" { // 没有文件名
			// value, store as string in memory 把内容保存在内存中，当做字符串
			n, err := io.CopyN(&b, p, maxValueBytes) // 从p中拷贝数据到b中
			if err != nil && err != io.EOF {         // 拷贝数据出错，返回
				return nil, err
			}
			maxValueBytes -= n
			if maxValueBytes == 0 { // 达到了10M的限制，返回消息过长
				return nil, errors.New("multipart: message too large")
			}
			form.Value[name] = append(form.Value[name], b.String()) // 设定form的值
			continue
		}

		// file, store in memory or on disk
		fh := &FileHeader{
			Filename: filename,
			Header:   p.Header,
		} // 创建一个FileHeader结构
		n, err := io.CopyN(&b, p, maxMemory+1) // 将数据拷贝到b中
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n > maxMemory { // 如果长度大于maxMemory
			// too big, write to disk and flush buffer
			file, err := ioutil.TempFile("", "multipart-") // 太大了，创建一个临时文件
			if err != nil {
				return nil, err
			}
			defer file.Close()
			_, err = io.Copy(file, io.MultiReader(&b, p))
			if err != nil {
				os.Remove(file.Name())
				return nil, err
			}
			fh.tmpfile = file.Name()
		} else { // 内存中可全部保存，将数据放入content中
			fh.content = b.Bytes()
			maxMemory -= n
		}
		form.File[name] = append(form.File[name], fh) // 返回文件的handler
	}

	return form, nil
}

// Form is a parsed multipart form. 表示被解析的multipart form
// Its File parts are stored either in memory or on disk, 文件部分或者保持在内存或保存在磁盘上
// and are accessible via the *FileHeader's Open method. 通过FileHeader的Open方法可以访问
// Its Value parts are stored as strings.
// Both are keyed by field name.
type Form struct { // multipart中form结构，File部分或者存储在内存中或者存储在磁盘上
	Value map[string][]string      // value部分存储在字符串上
	File  map[string][]*FileHeader // 或者在内存中，或者在文件上
}

// RemoveAll removes any temporary files associated with a Form.
func (f *Form) RemoveAll() error { // 清除该Form中的所有临时文件
	var err error
	for _, fhs := range f.File { // 遍历所有的FileHeader数组
		for _, fh := range fhs { // 遍历FileHeader数组中所有的FileHeader
			if fh.tmpfile != "" { // 如果有临时文件
				e := os.Remove(fh.tmpfile) // 删除临时文件
				if e != nil && err == nil {
					err = e
				}
			}
		}
	}
	return err
}

// A FileHeader describes a file part of a multipart request.
type FileHeader struct { // 描述一个multipart请求的文件部分，可能只在内存中content，也可能在磁盘上
	Filename string               // 文件名
	Header   textproto.MIMEHeader // MIMEHeader

	content []byte // 如果文件内容在内存中，保存在content里
	tmpfile string // 如果文件内容在磁盘上，指向磁盘文件
}

// Open opens and returns the FileHeader's associated File.
func (fh *FileHeader) Open() (File, error) { // 打开FileHeader获得一个文件接口
	if b := fh.content; b != nil { // 如果内容在内存上
		r := io.NewSectionReader(bytes.NewReader(b), 0, int64(len(b))) // 创建一个sectionReader
		return sectionReadCloser{r}, nil
	}
	return os.Open(fh.tmpfile)
}

// File is an interface to access the file part of a multipart message.
// Its contents may be either stored in memory or on disk.
// If stored on disk, the File's underlying concrete type will be an *os.File.
type File interface { // File接口
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

// helper types to turn a []byte into a File

type sectionReadCloser struct { // 为section Reader加上Close
	*io.SectionReader
}

func (rc sectionReadCloser) Close() error {
	return nil
}
