// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tls partially implements TLS 1.2, as specified in RFC 5246.
package tls

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"net"
	"strings"
	"time"
)

// Server returns a new TLS server side connection
// using conn as the underlying transport.
// The configuration config must be non-nil and must have
// at least one certificate.
func Server(conn net.Conn, config *Config) *Conn { // 返回tls server端连接，将conn进行包装
	return &Conn{conn: conn, config: config}
}

// Client returns a new TLS client side connection
// using conn as the underlying transport.
// The config cannot be nil: users must set either ServerName or
// InsecureSkipVerify in the config.
func Client(conn net.Conn, config *Config) *Conn { // 返回tls client端连接
	return &Conn{conn: conn, config: config, isClient: true}
}

// A listener implements a network listener (net.Listener) for TLS connections.
type listener struct { // tls的listener结构，实现了net.Listener接口
	net.Listener         // 对net.Listener的封装
	config       *Config // 增加了一个tls.Config
}

// Accept waits for and returns the next incoming TLS connection.
// The returned connection c is a *tls.Conn.
func (l *listener) Accept() (c net.Conn, err error) { // 返回server端连接，实现了net.Conn接口
	c, err = l.Listener.Accept()
	if err != nil {
		return
	}
	c = Server(c, l.config) // 返回一个net.Conn包含tls配置
	return
}

// NewListener creates a Listener which accepts connections from an inner
// Listener and wraps each connection with Server.
// The configuration config must be non-nil and must have
// at least one certificate.
func NewListener(inner net.Listener, config *Config) net.Listener { // 包装Listener，根据Config创建新的Listener
	l := new(listener)
	l.Listener = inner
	l.config = config
	return l
}

// Listen creates a TLS listener accepting connections on the
// given network address using net.Listen.
// The configuration config must be non-nil and must have
// at least one certificate.
func Listen(network, laddr string, config *Config) (net.Listener, error) { // Listen一个网络地址，但加入TLS配置
	if config == nil || len(config.Certificates) == 0 { // config和证书必须有效
		return nil, errors.New("tls.Listen: no certificates in configuration")
	}
	l, err := net.Listen(network, laddr) // 先创建一个不安全的Listener
	if err != nil {
		return nil, err
	}
	return NewListener(l, config), nil // 将不安全的Listener封装为安全的Listener
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "tls: DialWithDialer timed out" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// DialWithDialer connects to the given network address using dialer.Dial and
// then initiates a TLS handshake, returning the resulting TLS connection. Any
// timeout or deadline given in the dialer apply to connection and TLS
// handshake as a whole.
//
// DialWithDialer interprets a nil configuration as equivalent to the zero
// configuration; see the documentation of Config for the defaults.
func DialWithDialer(dialer *net.Dialer, network, addr string, config *Config) (*Conn, error) { // 作为client端dial一个连接
	// We want the Timeout and Deadline values from dialer to cover the
	// whole process: TCP connection and TLS handshake. This means that we
	// also need to start our own timers now.
	timeout := dialer.Timeout // 获得dialer的Timeout

	if !dialer.Deadline.IsZero() {
		deadlineTimeout := dialer.Deadline.Sub(time.Now())
		if timeout == 0 || deadlineTimeout < timeout {
			timeout = deadlineTimeout
		}
	}

	var errChannel chan error

	if timeout != 0 {
		errChannel = make(chan error, 2)
		time.AfterFunc(timeout, func() { // 创建超时机制，超时后向errChannel管道发送超时错误
			errChannel <- timeoutError{}
		})
	}

	rawConn, err := dialer.Dial(network, addr) // Dial连接，返回连接结构
	if err != nil {
		return nil, err
	}

	colonPos := strings.LastIndex(addr, ":")
	if colonPos == -1 {
		colonPos = len(addr)
	}
	hostname := addr[:colonPos]

	if config == nil { // 如果没有设置config，使用缺省Config
		config = defaultConfig()
	}
	// If no ServerName is set, infer the ServerName
	// from the hostname we're connecting to.
	if config.ServerName == "" { // 如果没有设置服务器名
		// Make a copy to avoid polluting argument or default.
		c := *config
		c.ServerName = hostname
		config = &c
	}

	conn := Client(rawConn, config)

	if timeout == 0 {
		err = conn.Handshake() // 握手连接
	} else {
		go func() { // 否则在一个goroutine中进行连接握手
			errChannel <- conn.Handshake()
		}()

		err = <-errChannel
	}

	if err != nil {
		rawConn.Close()
		return nil, err
	}

	return conn, nil
}

// Dial connects to the given network address using net.Dial
// and then initiates a TLS handshake, returning the resulting
// TLS connection.
// Dial interprets a nil configuration as equivalent to
// the zero configuration; see the documentation of Config
// for the defaults.
func Dial(network, addr string, config *Config) (*Conn, error) { // 根据config配置的安全选项，连接新地址
	return DialWithDialer(new(net.Dialer), network, addr, config)
}

// LoadX509KeyPair reads and parses a public/private key pair from a pair of
// files. The files must contain PEM encoded data.
func LoadX509KeyPair(certFile, keyFile string) (Certificate, error) {
	certPEMBlock, err := ioutil.ReadFile(certFile) // 读公钥文件
	if err != nil {
		return Certificate{}, err
	}
	keyPEMBlock, err := ioutil.ReadFile(keyFile) // 读私钥文件
	if err != nil {
		return Certificate{}, err
	}
	return X509KeyPair(certPEMBlock, keyPEMBlock)
}

// X509KeyPair parses a public/private key pair from a pair of
// PEM encoded data.
func X509KeyPair(certPEMBlock, keyPEMBlock []byte) (Certificate, error) {
	var cert Certificate
	var certDERBlock *pem.Block
	fail := func(err error) (Certificate, error) { return Certificate{}, err }
	for {
		certDERBlock, certPEMBlock = pem.Decode(certPEMBlock) // 解码公钥块
		if certDERBlock == nil {
			break
		}
		if certDERBlock.Type == "CERTIFICATE" { // 添加证书，如果类型为证书
			cert.Certificate = append(cert.Certificate, certDERBlock.Bytes)
		}
	}

	if len(cert.Certificate) == 0 { // 如果没有证书，返回错误
		return fail(errors.New("crypto/tls: failed to parse certificate PEM data"))
	}

	var keyDERBlock *pem.Block
	for {
		keyDERBlock, keyPEMBlock = pem.Decode(keyPEMBlock) // 解码私钥块
		if keyDERBlock == nil {                            // 解码私钥块失败
			return fail(errors.New("crypto/tls: failed to parse key PEM data"))
		}
		if keyDERBlock.Type == "PRIVATE KEY" || strings.HasSuffix(keyDERBlock.Type, " PRIVATE KEY") {
			break
		}
	}

	var err error
	cert.PrivateKey, err = parsePrivateKey(keyDERBlock.Bytes) // 解析私钥，获得证书私钥
	if err != nil {
		return fail(err)
	}

	// We don't need to parse the public key for TLS, but we so do anyway
	// to check that it looks sane and matches the private key.
	x509Cert, err := x509.ParseCertificate(cert.Certificate[0]) // 解析证书，获得证书结构
	if err != nil {
		return fail(err)
	}

	switch pub := x509Cert.PublicKey.(type) { // 根据公钥的类型判断私钥，必须类型一致
	case *rsa.PublicKey: // rsa公钥
		priv, ok := cert.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			return fail(errors.New("crypto/tls: private key type does not match public key type"))
		}
		if pub.N.Cmp(priv.N) != 0 {
			return fail(errors.New("crypto/tls: private key does not match public key"))
		}
	case *ecdsa.PublicKey:
		priv, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
		if !ok {
			return fail(errors.New("crypto/tls: private key type does not match public key type"))

		}
		if pub.X.Cmp(priv.X) != 0 || pub.Y.Cmp(priv.Y) != 0 {
			return fail(errors.New("crypto/tls: private key does not match public key"))
		}
	default: // 未知的公钥算法
		return fail(errors.New("crypto/tls: unknown public key algorithm"))
	}

	return cert, nil
}

// Attempt to parse the given private key DER block. OpenSSL 0.9.8 generates
// PKCS#1 private keys by default, while OpenSSL 1.0.0 generates PKCS#8 keys.
// OpenSSL ecparam generates SEC1 EC private keys for ECDSA. We try all three.
func parsePrivateKey(der []byte) (crypto.PrivateKey, error) { // 解析私钥key
	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		switch key := key.(type) {
		case *rsa.PrivateKey, *ecdsa.PrivateKey:
			return key, nil
		default:
			return nil, errors.New("crypto/tls: found unknown private key type in PKCS#8 wrapping")
		}
	}
	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key, nil
	}

	return nil, errors.New("crypto/tls: failed to parse private key")
}
