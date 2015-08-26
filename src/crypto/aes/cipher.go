// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aes

import (
	"crypto/cipher"
	"strconv"
)

// The AES block size in bytes.
const BlockSize = 16

// A cipher is an instance of AES encryption using a particular key.
type aesCipher struct {
	enc []uint32
	dec []uint32
}

type KeySizeError int

func (k KeySizeError) Error() string { // 错误消息
	return "crypto/aes: invalid key size " + strconv.Itoa(int(k))
}

// NewCipher creates and returns a new cipher.Block.
// The key argument should be the AES key,
// either 16, 24, or 32 bytes to select
// AES-128, AES-192, or AES-256.
func NewCipher(key []byte) (cipher.Block, error) { // AES的加密强度
	k := len(key) // 取得key的长度
	switch k {
	default:
		return nil, KeySizeError(k) // key的长度不合法
	case 16, 24, 32: // key的长度必须为16、24或32
		break
	}

	n := k + 28
	c := &aesCipher{make([]uint32, n), make([]uint32, n)} // 创建aesCipher结构
	expandKey(key, c.enc, c.dec)
	return c, nil
}

func (c *aesCipher) BlockSize() int { return BlockSize } // 返回块大小

func (c *aesCipher) Encrypt(dst, src []byte) { // 执行加密
	if len(src) < BlockSize {
		panic("crypto/aes: input not full block")
	}
	if len(dst) < BlockSize {
		panic("crypto/aes: output not full block")
	}
	encryptBlock(c.enc, dst, src)
}

func (c *aesCipher) Decrypt(dst, src []byte) { // 执行解密
	if len(src) < BlockSize {
		panic("crypto/aes: input not full block")
	}
	if len(dst) < BlockSize {
		panic("crypto/aes: output not full block")
	}
	decryptBlock(c.dec, dst, src)
}
