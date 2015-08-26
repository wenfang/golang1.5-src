// Copyright 2010 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package textproto

// A MIMEHeader represents a MIME-style header mapping
// keys to sets of values.
type MIMEHeader map[string][]string

// Add adds the key, value pair to the header.
// It appends to any existing values associated with key.
// 将key和value加入到MIMEHeader中
func (h MIMEHeader) Add(key, value string) { // 将value加入到key对应的string列表中
	// 先把key进行正则化
	key = CanonicalMIMEHeaderKey(key)
	h[key] = append(h[key], value) // 添加到头部的map中，map的value为列表
}

// Set sets the header entries associated with key to
// the single element value.  It replaces any existing
// values associated with key.
func (h MIMEHeader) Set(key, value string) { // 设置key，将key进行正则化后再设置
	h[CanonicalMIMEHeaderKey(key)] = []string{value}
}

// Get gets the first value associated with the given key.
// If there are no values associated with the key, Get returns "".
// Get is a convenience method.  For more complex queries,
// access the map directly.
func (h MIMEHeader) Get(key string) string { // 只返回列表中的第一个
	if h == nil {
		return ""
	}
	v := h[CanonicalMIMEHeaderKey(key)]
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

// Del deletes the values associated with key.
func (h MIMEHeader) Del(key string) { // 删除对应key的MIME头部
	delete(h, CanonicalMIMEHeaderKey(key))
}
