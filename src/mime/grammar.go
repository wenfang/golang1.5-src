// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mime

import (
	"strings"
)

// isTSpecial reports whether rune is in 'tspecials' as defined by RFC
// 1521 and RFC 2045.
func isTSpecial(r rune) bool { // 检查是否为特殊字符
	return strings.IndexRune(`()<>@,;:\"/[]?=`, r) != -1
}

// isTokenChar reports whether rune is in 'token' as defined by RFC
// 1521 and RFC 2045.
func isTokenChar(r rune) bool { // 检查是否为token字符
	// token := 1*<any (US-ASCII) CHAR except SPACE, CTLs,
	//             or tspecials>
	return r > 0x20 && r < 0x7f && !isTSpecial(r) // 在0x20到0x7f间，并且不是特殊字符
}

// isToken reports whether s is a 'token' as defined by RFC 1521
// and RFC 2045.
func isToken(s string) bool { // 检查字符串s是否为一个Token
	if s == "" { // 空字符串，不是token
		return false
	}
	return strings.IndexFunc(s, isNotTokenChar) < 0 // 检查是否为token的字符
}
