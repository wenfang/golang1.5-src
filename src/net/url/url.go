// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package url parses URLs and implements query escaping.
// See RFC 3986.
package url

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Error reports an error and the operation and URL that caused it.
type Error struct { // 报告对url的错误操作
	Op  string
	URL string
	Err error
}

func (e *Error) Error() string { return e.Op + " " + e.URL + ": " + e.Err.Error() }

func ishex(c byte) bool { // 判断是否是16进制
	switch {
	case '0' <= c && c <= '9':
		return true
	case 'a' <= c && c <= 'f':
		return true
	case 'A' <= c && c <= 'F':
		return true
	}
	return false
}

func unhex(c byte) byte { // 从16进制字符转换为数字
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

type encoding int

const (
	encodePath           encoding = 1 + iota // 字符出现在path上
	encodeHost                               // 字符出现在host部分
	encodeUserPassword                       // 字符出现在user password部分
	encodeQueryComponent                     // 字符出现在query部分
	encodeFragment                           // 字符出现在fragment部分
)

type EscapeError string

func (e EscapeError) Error() string { // escape错误
	return "invalid URL escape " + strconv.Quote(string(e))
}

// Return true if the specified character should be escaped when
// appearing in a URL string, according to RFC 3986.
//
// Please be informed that for now shouldEscape does not check all
// reserved characters correctly. See golang.org/issue/5684.
func shouldEscape(c byte, mode encoding) bool {
	// §2.3 Unreserved characters (alphanum)
	if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' {
		return false
	}

	if mode == encodeHost {
		// §3.2.2 Host allows
		//	sub-delims = "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="
		// as part of reg-name.
		// We add : because we include :port as part of host.
		// We add [ ] because we include [ipv6]:port as part of host
		switch c {
		case '!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '=', ':', '[', ']':
			return false
		}
	}

	switch c {
	case '-', '_', '.', '~': // §2.3 Unreserved characters (mark)
		return false

	case '$', '&', '+', ',', '/', ':', ';', '=', '?', '@': // §2.2 Reserved characters (reserved)
		// Different sections of the URL allow a few of
		// the reserved characters to appear unescaped.
		switch mode {
		case encodePath: // §3.3 在path上，只需要escape ?
			// The RFC allows : @ & = + $ but saves / ; , for assigning
			// meaning to individual path segments. This package
			// only manipulates the path as a whole, so we allow those
			// last two as well. That leaves only ? to escape.
			return c == '?'

		case encodeUserPassword: // §3.2.1 在userPassword上，只需要escape@ / ? :
			// The RFC allows ';', ':', '&', '=', '+', '$', and ',' in
			// userinfo, so we must escape only '@', '/', and '?'.
			// The parsing of userinfo treats ':' as special so we must escape
			// that too.
			return c == '@' || c == '/' || c == '?' || c == ':'

		case encodeQueryComponent: // §3.4 在查询字符串上，必须进行escape
			// The RFC reserves (so we must escape) everything.
			return true

		case encodeFragment: // §4.1 在fragment上，不需要进行escape
			// The RFC text is silent but the grammar allows
			// everything, so escape nothing.
			return false
		}
	}

	// Everything else must be escaped.
	return true // 所有其它的都需要被编码
}

// QueryUnescape does the inverse transformation of QueryEscape, converting
// %AB into the byte 0xAB and '+' into ' ' (space). It returns an error if
// any % is not followed by two hexadecimal digits.
func QueryUnescape(s string) (string, error) { // url的unescape，对查询的Unescape
	return unescape(s, encodeQueryComponent)
}

// unescape unescapes a string; the mode specifies
// which section of the URL string is being unescaped.
func unescape(s string, mode encoding) (string, error) { // 将url进行unescape
	// Count %, check that they're well-formed.
	n := 0                    // 保存%的数量
	hasPlus := false          // 是否具有+
	for i := 0; i < len(s); { // 遍历每个原字符串
		switch s[i] {
		case '%': // 如果是百分号
			n++                                                    // 百分号的数量增加
			if i+2 >= len(s) || !ishex(s[i+1]) || !ishex(s[i+2]) { // 如果%后面字符不合法
				s = s[i:]
				if len(s) > 3 {
					s = s[:3]
				}
				return "", EscapeError(s) // 返回Escape错误
			}
			i += 3
		case '+': // 如果碰到+号
			hasPlus = mode == encodeQueryComponent
			i++
		default:
			i++
		}
	}

	if n == 0 && !hasPlus { // 没有%没有+，原样输出
		return s, nil
	}

	t := make([]byte, len(s)-2*n) // 创建byte slice保存最终结果
	j := 0
	for i := 0; i < len(s); { // 遍历所有字符
		switch s[i] {
		case '%': // 碰到%进行解析
			t[j] = unhex(s[i+1])<<4 | unhex(s[i+2]) // 将16进制解析为数字
			j++
			i += 3
		case '+': // 碰到+
			if mode == encodeQueryComponent { // 如果解析的是查询部分
				t[j] = ' ' // 替换为空格
			} else {
				t[j] = '+' // 否则仍然是+
			}
			j++
			i++
		default:
			t[j] = s[i]
			j++
			i++
		}
	}
	return string(t), nil
}

// QueryEscape escapes the string so it can be safely placed
// inside a URL query.
func QueryEscape(s string) string { // url的escape，返回escape后的结果
	return escape(s, encodeQueryComponent) // escape查询部分
}

func escape(s string, mode encoding) string { // 将字符串进行escape
	spaceCount, hexCount := 0, 0
	for i := 0; i < len(s); i++ { // 遍历字符串中所有字符
		c := s[i]
		if shouldEscape(c, mode) { // 检查字符c是否需要escape
			if c == ' ' && mode == encodeQueryComponent {
				spaceCount++
			} else {
				hexCount++
			}
		}
	}

	if spaceCount == 0 && hexCount == 0 { // 如果spaceCount和hexCount的数量为0，直接返回原字符串
		return s
	}

	t := make([]byte, len(s)+2*hexCount)
	j := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == ' ' && mode == encodeQueryComponent:
			t[j] = '+'
			j++
		case shouldEscape(c, mode):
			t[j] = '%'
			t[j+1] = "0123456789ABCDEF"[c>>4]
			t[j+2] = "0123456789ABCDEF"[c&15]
			j += 3
		default:
			t[j] = s[i]
			j++
		}
	}
	return string(t)
}

// A URL represents a parsed URL (technically, a URI reference).
// The general form represented is:
//
//	scheme://[userinfo@]host/path[?query][#fragment]
//
// URLs that do not start with a slash after the scheme are interpreted as:
//
//	scheme:opaque[?query][#fragment]
//
// Note that the Path field is stored in decoded form: /%47%6f%2f becomes /Go/.
// A consequence is that it is impossible to tell which slashes in the Path were
// slashes in the raw URL and which were %2f. This distinction is rarely important,
// but when it is, code must not use Path directly.
//
// Go 1.5 introduced the RawPath field to hold the encoded form of Path.
// The Parse function sets both Path and RawPath in the URL it returns,
// and URL's String method uses RawPath if it is a valid encoding of Path,
// by calling the EncodedPath method.
//
// In earlier versions of Go, the more indirect workarounds were that an
// HTTP server could consult req.RequestURI and an HTTP client could
// construct a URL struct directly and set the Opaque field instead of Path.
// These still work as well.
type URL struct { // 代表一个解析的URL结构
	Scheme   string    // 协议类型
	Opaque   string    // encoded opaque data 经过编码的opaque数据
	User     *Userinfo // username and password information 用户信息
	Host     string    // host or host:port 主机+端口
	Path     string    // 路径
	RawPath  string    // encoded path hint (Go 1.5 and later only; see EscapedPath method)
	RawQuery string    // encoded query values, without '?' 经过编码的查询值，不包含?
	Fragment string    // fragment for references, without '#' fragment部分，不包含#
}

// User returns a Userinfo containing the provided username
// and no password set.
func User(username string) *Userinfo { // 创建一个Userinfo，密码为空
	return &Userinfo{username, "", false}
}

// UserPassword returns a Userinfo containing the provided username
// and password.
// This functionality should only be used with legacy web sites.
// RFC 2396 warns that interpreting Userinfo this way
// ``is NOT RECOMMENDED, because the passing of authentication
// information in clear text (such as URI) has proven to be a
// security risk in almost every case where it has been used.''
func UserPassword(username, password string) *Userinfo { // 创建一个UserInfo密码非空
	return &Userinfo{username, password, true}
}

// The Userinfo type is an immutable encapsulation of username and
// password details for a URL. An existing Userinfo value is guaranteed
// to have a username set (potentially empty, as allowed by RFC 2396),
// and optionally a password.
type Userinfo struct { // userinfo信息
	username    string
	password    string
	passwordSet bool // 是否设置了password
}

// Username returns the username.
func (u *Userinfo) Username() string { // 返回用户名
	return u.username
}

// Password returns the password in case it is set, and whether it is set.
func (u *Userinfo) Password() (string, bool) { // 返回passwd
	if u.passwordSet {
		return u.password, true
	}
	return "", false
}

// String returns the encoded userinfo information in the standard form
// of "username[:password]".
func (u *Userinfo) String() string { // 返回userinfo的字符串信息
	s := escape(u.username, encodeUserPassword)
	if u.passwordSet {
		s += ":" + escape(u.password, encodeUserPassword)
	}
	return s
}

// Maybe rawurl is of the form scheme:path.
// (Scheme must be [a-zA-Z][a-zA-Z0-9+-.]*)
// If so, return scheme, path; else return "", rawurl.
func getscheme(rawurl string) (scheme, path string, err error) {
	for i := 0; i < len(rawurl); i++ {
		c := rawurl[i]
		switch {
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z':
		// do nothing
		case '0' <= c && c <= '9' || c == '+' || c == '-' || c == '.':
			if i == 0 {
				return "", rawurl, nil
			}
		case c == ':':
			if i == 0 {
				return "", "", errors.New("missing protocol scheme")
			}
			return rawurl[:i], rawurl[i+1:], nil
		default:
			// we have encountered an invalid character,
			// so there is no valid scheme
			return "", rawurl, nil
		}
	}
	return "", rawurl, nil
}

// Maybe s is of the form t c u.
// If so, return t, c u (or t, u if cutc == true).
// If not, return s, "".
func split(s string, c string, cutc bool) (string, string) { // c作为分割符将s分为两部分
	i := strings.Index(s, c)
	if i < 0 {
		return s, ""
	}
	if cutc {
		return s[:i], s[i+len(c):]
	}
	return s[:i], s[i:]
}

// Parse parses rawurl into a URL structure.
// The rawurl may be relative or absolute. rawurl可能是绝对路径，也有可能是相对路径
func Parse(rawurl string) (url *URL, err error) { // 将一个url字符串解析为URL结构，不一定来自请求
	// Cut off #frag
	u, frag := split(rawurl, "#", true)         // split出Fragment部分
	if url, err = parse(u, false); err != nil { // 解析成URL结构，不包括Fragment部分
		return nil, err
	}
	if frag == "" {
		return url, nil
	}
	if url.Fragment, err = unescape(frag, encodeFragment); err != nil {
		return nil, &Error{"parse", rawurl, err}
	}
	return url, nil
}

// ParseRequestURI parses rawurl into a URL structure.  It assumes that
// rawurl was received in an HTTP request, so the rawurl is interpreted
// only as an absolute URI or an absolute path.
// The string rawurl is assumed not to have a #fragment suffix.
// (Web browsers strip #fragment before sending the URL to a web server.)
func ParseRequestURI(rawurl string) (url *URL, err error) { // 解析请求的url字符串为URL结构
	return parse(rawurl, true) // 如果url来自请求，则按照绝对路径解析
}

// parse parses a URL from a string in one of two contexts.  If
// viaRequest is true, the URL is assumed to have arrived via an HTTP request,
// in which case only absolute URLs or path-absolute relative URLs are allowed.
// If viaRequest is false, all forms of relative URLs are allowed.
func parse(rawurl string, viaRequest bool) (url *URL, err error) { // 解析url字符, viaRequest表明url是否来自请求
	var rest string

	if rawurl == "" && viaRequest { // url为空，并且来自请求，返回错误
		err = errors.New("empty url")
		goto Error
	}
	url = new(URL) // 新创建URL结构

	if rawurl == "*" {
		url.Path = "*"
		return
	}

	// Split off possible leading "http:", "mailto:", etc.
	// Cannot contain escaped characters.
	if url.Scheme, rest, err = getscheme(rawurl); err != nil { // 先获得scheme
		goto Error
	}
	url.Scheme = strings.ToLower(url.Scheme) // 将scheme变为小写

	rest, url.RawQuery = split(rest, "?", true) // 获得路径部分与查询部分

	if !strings.HasPrefix(rest, "/") { // 如果不以/开头
		if url.Scheme != "" {
			// We consider rootless paths per RFC 3986 as opaque.
			url.Opaque = rest
			return url, nil
		}
		if viaRequest { // 请求URL必须以/开头
			err = errors.New("invalid URI for request")
			goto Error
		}
	}

	if (url.Scheme != "" || !viaRequest && !strings.HasPrefix(rest, "///")) && strings.HasPrefix(rest, "//") {
		var authority string
		authority, rest = split(rest[2:], "/", false)
		url.User, url.Host, err = parseAuthority(authority)
		if err != nil {
			goto Error
		}
	}
	if url.Path, err = unescape(rest, encodePath); err != nil {
		goto Error
	}
	// RawPath is a hint as to the encoding of Path to use
	// in url.EncodedPath. If that method already gets the
	// right answer without RawPath, leave it empty.
	// This will help make sure that people don't rely on it in general.
	if url.EscapedPath() != rest && validEncodedPath(rest) {
		url.RawPath = rest
	}
	return url, nil

Error:
	return nil, &Error{"parse", rawurl, err}
}

func parseAuthority(authority string) (user *Userinfo, host string, err error) {
	i := strings.LastIndex(authority, "@")
	if i < 0 {
		host, err = parseHost(authority)
	} else {
		host, err = parseHost(authority[i+1:])
	}
	if err != nil {
		return nil, "", err
	}
	if i < 0 {
		return nil, host, nil
	}
	userinfo := authority[:i]
	if strings.Index(userinfo, ":") < 0 {
		if userinfo, err = unescape(userinfo, encodeUserPassword); err != nil {
			return nil, "", err
		}
		user = User(userinfo)
	} else {
		username, password := split(userinfo, ":", true)
		if username, err = unescape(username, encodeUserPassword); err != nil {
			return nil, "", err
		}
		if password, err = unescape(password, encodeUserPassword); err != nil {
			return nil, "", err
		}
		user = UserPassword(username, password)
	}
	return user, host, nil
}

// parseHost parses host as an authority without user
// information. That is, as host[:port].
func parseHost(host string) (string, error) {
	litOrName := host
	if strings.HasPrefix(host, "[") {
		// Parse an IP-Literal in RFC 3986 and RFC 6874.
		// E.g., "[fe80::1], "[fe80::1%25en0]"
		//
		// RFC 4007 defines "%" as a delimiter character in
		// the textual representation of IPv6 addresses.
		// Per RFC 6874, in URIs that "%" is encoded as "%25".
		i := strings.LastIndex(host, "]")
		if i < 0 {
			return "", errors.New("missing ']' in host")
		}
		colonPort := host[i+1:]
		if !validOptionalPort(colonPort) {
			return "", fmt.Errorf("invalid port %q after host", colonPort)
		}
		// Parse a host subcomponent without a ZoneID in RFC
		// 6874 because the ZoneID is allowed to use the
		// percent encoded form.
		j := strings.Index(host[:i], "%25")
		if j < 0 {
			litOrName = host[1:i]
		} else {
			litOrName = host[1:j]
		}
	}

	// A URI containing an IP-Literal without a ZoneID or
	// IPv4address in RFC 3986 and RFC 6847 must not be
	// percent-encoded.
	//
	// A URI containing a DNS registered name in RFC 3986 is
	// allowed to be percent-encoded, though we don't use it for
	// now to avoid messing up with the gap between allowed
	// characters in URI and allowed characters in DNS.
	// See golang.org/issue/7991.
	if strings.Contains(litOrName, "%") {
		return "", errors.New("percent-encoded characters in host")
	}
	var err error
	if host, err = unescape(host, encodeHost); err != nil {
		return "", err
	}
	return host, nil
}

// EscapedPath returns the escaped form of u.Path.
// In general there are multiple possible escaped forms of any path.
// EscapedPath returns u.RawPath when it is a valid escaping of u.Path.
// Otherwise EscapedPath ignores u.RawPath and computes an escaped
// form on its own.
// The String and RequestURI methods use EscapedPath to construct
// their results.
// In general, code should call EscapedPath instead of
// reading u.RawPath directly.
func (u *URL) EscapedPath() string {
	if u.RawPath != "" && validEncodedPath(u.RawPath) {
		p, err := unescape(u.RawPath, encodePath)
		if err == nil && p == u.Path {
			return u.RawPath
		}
	}
	if u.Path == "*" {
		return "*" // don't escape (Issue 11202)
	}
	return escape(u.Path, encodePath)
}

// validEncodedPath reports whether s is a valid encoded path.
// It must not contain any bytes that require escaping during path encoding.
func validEncodedPath(s string) bool {
	for i := 0; i < len(s); i++ {
		// RFC 3986, Appendix A.
		// pchar = unreserved / pct-encoded / sub-delims / ":" / "@".
		// shouldEscape is not quite compliant with the RFC,
		// so we check the sub-delims ourselves and let
		// shouldEscape handle the others.
		switch s[i] {
		case '!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '=', ':', '@':
			// ok
		case '[', ']':
			// ok - not specified in RFC 3986 but left alone by modern browsers
		case '%':
			// ok - percent encoded, will decode
		default:
			if shouldEscape(s[i], encodePath) {
				return false
			}
		}
	}
	return true
}

// validOptionalPort reports whether port is either an empty string
// or matches /^:\d+$/
func validOptionalPort(port string) bool {
	if port == "" {
		return true
	}
	if port[0] != ':' || len(port) == 1 {
		return false
	}
	for _, b := range port[1:] {
		if b < '0' || b > '9' {
			return false
		}
	}
	return true
}

// String reassembles the URL into a valid URL string.
// The general form of the result is one of:
//
//	scheme:opaque?query#fragment
//	scheme://userinfo@host/path?query#fragment
//
// If u.Opaque is non-empty, String uses the first form;
// otherwise it uses the second form.
// To obtain the path, String uses u.EncodedPath().
//
// In the second form, the following rules apply:
//	- if u.Scheme is empty, scheme: is omitted.
//	- if u.User is nil, userinfo@ is omitted.
//	- if u.Host is empty, host/ is omitted.
//	- if u.Scheme and u.Host are empty and u.User is nil,
//	   the entire scheme://userinfo@host/ is omitted.
//	- if u.Host is non-empty and u.Path begins with a /,
//	   the form host/path does not add its own /.
//	- if u.RawQuery is empty, ?query is omitted.
//	- if u.Fragment is empty, #fragment is omitted.
func (u *URL) String() string { // 转换成字符串的表示形式
	var buf bytes.Buffer
	if u.Scheme != "" {
		buf.WriteString(u.Scheme)
		buf.WriteByte(':')
	}
	if u.Opaque != "" {
		buf.WriteString(u.Opaque)
	} else {
		if u.Scheme != "" || u.Host != "" || u.User != nil {
			buf.WriteString("//")
			if ui := u.User; ui != nil {
				buf.WriteString(ui.String())
				buf.WriteByte('@')
			}
			if h := u.Host; h != "" {
				buf.WriteString(escape(h, encodeHost))
			}
		}
		path := u.EscapedPath()
		if path != "" && path[0] != '/' && u.Host != "" {
			buf.WriteByte('/')
		}
		buf.WriteString(path)
	}
	if u.RawQuery != "" {
		buf.WriteByte('?')
		buf.WriteString(u.RawQuery)
	}
	if u.Fragment != "" {
		buf.WriteByte('#')
		buf.WriteString(escape(u.Fragment, encodeFragment))
	}
	return buf.String()
}

// Values maps a string key to a list of values.
// It is typically used for query parameters and form values.
// Unlike in the http.Header map, the keys in a Values map
// are case-sensitive.
type Values map[string][]string // 映射一个字符串类型的key到string列表，用作URL查询参数解析，是大小写敏感的

// Get gets the first value associated with the given key.
// If there are no values associated with the key, Get returns
// the empty string. To access multiple values, use the map
// directly.
func (v Values) Get(key string) string { // 获得某个key的值，只获取第一个
	if v == nil {
		return ""
	}
	vs, ok := v[key]
	if !ok || len(vs) == 0 {
		return ""
	}
	return vs[0]
}

// Set sets the key to value. It replaces any existing
// values.
func (v Values) Set(key, value string) { // 设置某个key的值
	v[key] = []string{value}
}

// Add adds the value to key. It appends to any existing
// values associated with key.
func (v Values) Add(key, value string) { // 将某个key的值添加到列表中
	v[key] = append(v[key], value)
}

// Del deletes the values associated with key.
func (v Values) Del(key string) { // 删除某个key的值
	delete(v, key)
}

// ParseQuery parses the URL-encoded query string and returns
// a map listing the values specified for each key.
// ParseQuery always returns a non-nil map containing all the
// valid query parameters found; err describes the first decoding error
// encountered, if any.
func ParseQuery(query string) (m Values, err error) { // 解析查询为Key，value的形式，解析查询字符串
	m = make(Values)
	err = parseQuery(m, query)
	return
}

func parseQuery(m Values, query string) (err error) { // 解析查询，获得key value的形式
	for query != "" {
		key := query
		if i := strings.IndexAny(key, "&;"); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = ""
		}
		if key == "" {
			continue
		}
		value := ""
		if i := strings.Index(key, "="); i >= 0 {
			key, value = key[:i], key[i+1:]
		}
		key, err1 := QueryUnescape(key)
		if err1 != nil {
			if err == nil {
				err = err1
			}
			continue
		}
		value, err1 = QueryUnescape(value)
		if err1 != nil {
			if err == nil {
				err = err1
			}
			continue
		}
		m[key] = append(m[key], value)
	}
	return err
}

// Encode encodes the values into ``URL encoded'' form
// ("bar=baz&foo=quux") sorted by key.
func (v Values) Encode() string {
	if v == nil {
		return ""
	}
	var buf bytes.Buffer
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vs := v[k]
		prefix := QueryEscape(k) + "="
		for _, v := range vs {
			if buf.Len() > 0 {
				buf.WriteByte('&')
			}
			buf.WriteString(prefix)
			buf.WriteString(QueryEscape(v))
		}
	}
	return buf.String()
}

// resolvePath applies special path segments from refs and applies
// them to base, per RFC 3986.
func resolvePath(base, ref string) string {
	var full string
	if ref == "" {
		full = base
	} else if ref[0] != '/' {
		i := strings.LastIndex(base, "/")
		full = base[:i+1] + ref
	} else {
		full = ref
	}
	if full == "" {
		return ""
	}
	var dst []string
	src := strings.Split(full, "/")
	for _, elem := range src {
		switch elem {
		case ".":
			// drop
		case "..":
			if len(dst) > 0 {
				dst = dst[:len(dst)-1]
			}
		default:
			dst = append(dst, elem)
		}
	}
	if last := src[len(src)-1]; last == "." || last == ".." {
		// Add final slash to the joined path.
		dst = append(dst, "")
	}
	return "/" + strings.TrimLeft(strings.Join(dst, "/"), "/")
}

// IsAbs reports whether the URL is absolute.
func (u *URL) IsAbs() bool { // url是否为绝对路径
	return u.Scheme != ""
}

// Parse parses a URL in the context of the receiver.  The provided URL
// may be relative or absolute.  Parse returns nil, err on parse
// failure, otherwise its return value is the same as ResolveReference.
func (u *URL) Parse(ref string) (*URL, error) { // 解析url
	refurl, err := Parse(ref)
	if err != nil {
		return nil, err
	}
	return u.ResolveReference(refurl), nil
}

// ResolveReference resolves a URI reference to an absolute URI from
// an absolute base URI, per RFC 3986 Section 5.2.  The URI reference
// may be relative or absolute.  ResolveReference always returns a new
// URL instance, even if the returned URL is identical to either the
// base or reference. If ref is an absolute URL, then ResolveReference
// ignores base and returns a copy of ref.
func (u *URL) ResolveReference(ref *URL) *URL { // 解析引用
	url := *ref
	if ref.Scheme == "" {
		url.Scheme = u.Scheme
	}
	if ref.Scheme != "" || ref.Host != "" || ref.User != nil {
		// The "absoluteURI" or "net_path" cases.
		url.Path = resolvePath(ref.Path, "")
		return &url
	}
	if ref.Opaque != "" {
		url.User = nil
		url.Host = ""
		url.Path = ""
		return &url
	}
	if ref.Path == "" {
		if ref.RawQuery == "" {
			url.RawQuery = u.RawQuery
			if ref.Fragment == "" {
				url.Fragment = u.Fragment
			}
		}
	}
	// The "abs_path" or "rel_path" cases.
	url.Host = u.Host
	url.User = u.User
	url.Path = resolvePath(u.Path, ref.Path)
	return &url
}

// Query parses RawQuery and returns the corresponding values.
func (u *URL) Query() Values { // 返回查询的Values，解析查询字符串
	v, _ := ParseQuery(u.RawQuery)
	return v
}

// RequestURI returns the encoded path?query or opaque?query
// string that would be used in an HTTP request for u.
func (u *URL) RequestURI() string { // 返回请求的url字符串
	result := u.Opaque
	if result == "" {
		result = u.EscapedPath()
		if result == "" {
			result = "/"
		}
	} else {
		if strings.HasPrefix(result, "//") {
			result = u.Scheme + ":" + result
		}
	}
	if u.RawQuery != "" {
		result += "?" + u.RawQuery
	}
	return result
}
