// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cipher implements standard block cipher modes that can be wrapped
// around low-level block cipher implementations.
// See http://csrc.nist.gov/groups/ST/toolkit/BCM/current_modes.html
// and NIST Special Publication 800-38A.
package cipher

// A Block represents an implementation of block cipher
// using a given key.  It provides the capability to encrypt
// or decrypt individual blocks.  The mode implementations
// extend that capability to streams of blocks.
type Block interface { // 块接口，定义加解密接口
	// BlockSize returns the cipher's block size.
	BlockSize() int // 返回cipher块大小

	// Encrypt encrypts the first block in src into dst.
	// Dst and src may point at the same memory.
	Encrypt(dst, src []byte) // 执行加密

	// Decrypt decrypts the first block in src into dst.
	// Dst and src may point at the same memory.
	Decrypt(dst, src []byte) // 执行解密
}

// A Stream represents a stream cipher.
type Stream interface { // 代表流式加密解密接口
	// XORKeyStream XORs each byte in the given slice with a byte from the
	// cipher's key stream. Dst and src may point to the same memory.
	// If len(dst) < len(src), XORKeyStream should panic. It is acceptable
	// to pass a dst bigger than src, and in that case, XORKeyStream will
	// only update dst[:len(src)] and will not touch the rest of dst.
	XORKeyStream(dst, src []byte)
}

// A BlockMode represents a block cipher running in a block-based mode (CBC,
// ECB etc).
type BlockMode interface { // 块模式接口
	// BlockSize returns the mode's block size.
	BlockSize() int // 返回模式的块大小

	// CryptBlocks encrypts or decrypts a number of blocks. The length of
	// src must be a multiple of the block size. Dst and src may point to
	// the same memory.
	CryptBlocks(dst, src []byte)
}

// Utility routines

func dup(p []byte) []byte {
	q := make([]byte, len(p))
	copy(q, p)
	return q
}
