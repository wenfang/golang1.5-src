// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package expvar provides a standardized interface to public variables, such
// as operation counters in servers. It exposes these variables via HTTP at
// /debug/vars in JSON format.
//
// Operations to set or modify these public variables are atomic.
//
// In addition to adding the HTTP handler, this package registers the
// following variables:
//
//	cmdline   os.Args
//	memstats  runtime.Memstats
//
// The package is sometimes only imported for the side effect of
// registering its HTTP handler and the above variables.  To use it
// this way, link this package into your program:
//	import _ "expvar"
//
package expvar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

// Var is an abstract type for all exported variables.
type Var interface { // 作为所有导出变量的抽象类型，接口类型
	String() string
}

// Int is a 64-bit integer variable that satisfies the Var interface.
type Int struct { // 64位的Int类型的值
	i int64
}

func (v *Int) String() string { // 实现了Var接口
	return strconv.FormatInt(atomic.LoadInt64(&v.i), 10)
}

func (v *Int) Add(delta int64) { // 可以对Int进行Add
	atomic.AddInt64(&v.i, delta)
}

func (v *Int) Set(value int64) { // 可以对Int进行Set
	atomic.StoreInt64(&v.i, value)
}

// Float is a 64-bit float variable that satisfies the Var interface.
type Float struct { // Float值
	f uint64
}

func (v *Float) String() string { // 实现了Var接口
	return strconv.FormatFloat(
		math.Float64frombits(atomic.LoadUint64(&v.f)), 'g', -1, 64)
}

// Add adds delta to v.
func (v *Float) Add(delta float64) { // 增加delta值到v
	for {
		cur := atomic.LoadUint64(&v.f)
		curVal := math.Float64frombits(cur)
		nxtVal := curVal + delta
		nxt := math.Float64bits(nxtVal)
		if atomic.CompareAndSwapUint64(&v.f, cur, nxt) {
			return
		}
	}
}

// Set sets v to value.
func (v *Float) Set(value float64) { // 设置value值
	atomic.StoreUint64(&v.f, math.Float64bits(value))
}

// Map is a string-to-Var map variable that satisfies the Var interface.
type Map struct {
	mu   sync.RWMutex
	m    map[string]Var
	keys []string // sorted
}

// KeyValue represents a single entry in a Map.
type KeyValue struct {
	Key   string
	Value Var
}

func (v *Map) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	var b bytes.Buffer
	fmt.Fprintf(&b, "{")
	first := true
	v.doLocked(func(kv KeyValue) {
		if !first {
			fmt.Fprintf(&b, ", ")
		}
		fmt.Fprintf(&b, "%q: %v", kv.Key, kv.Value)
		first = false
	})
	fmt.Fprintf(&b, "}")
	return b.String()
}

func (v *Map) Init() *Map {
	v.m = make(map[string]Var)
	return v
}

// updateKeys updates the sorted list of keys in v.keys.
// must be called with v.mu held.
func (v *Map) updateKeys() {
	if len(v.m) == len(v.keys) {
		// No new key.
		return
	}
	v.keys = v.keys[:0]
	for k := range v.m {
		v.keys = append(v.keys, k)
	}
	sort.Strings(v.keys)
}

func (v *Map) Get(key string) Var {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.m[key]
}

func (v *Map) Set(key string, av Var) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.m[key] = av
	v.updateKeys()
}

func (v *Map) Add(key string, delta int64) {
	v.mu.RLock()
	av, ok := v.m[key]
	v.mu.RUnlock()
	if !ok {
		// check again under the write lock
		v.mu.Lock()
		av, ok = v.m[key]
		if !ok {
			av = new(Int)
			v.m[key] = av
			v.updateKeys()
		}
		v.mu.Unlock()
	}

	// Add to Int; ignore otherwise.
	if iv, ok := av.(*Int); ok {
		iv.Add(delta)
	}
}

// AddFloat adds delta to the *Float value stored under the given map key.
func (v *Map) AddFloat(key string, delta float64) {
	v.mu.RLock()
	av, ok := v.m[key]
	v.mu.RUnlock()
	if !ok {
		// check again under the write lock
		v.mu.Lock()
		av, ok = v.m[key]
		if !ok {
			av = new(Float)
			v.m[key] = av
			v.updateKeys()
		}
		v.mu.Unlock()
	}

	// Add to Float; ignore otherwise.
	if iv, ok := av.(*Float); ok {
		iv.Add(delta)
	}
}

// Do calls f for each entry in the map.
// The map is locked during the iteration,
// but existing entries may be concurrently updated.
func (v *Map) Do(f func(KeyValue)) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	v.doLocked(f)
}

// doLocked calls f for each entry in the map.
// v.mu must be held for reads.
func (v *Map) doLocked(f func(KeyValue)) {
	for _, k := range v.keys {
		f(KeyValue{k, v.m[k]})
	}
}

// String is a string variable, and satisfies the Var interface.
type String struct {
	mu sync.RWMutex
	s  string
}

func (v *String) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return strconv.Quote(v.s)
}

func (v *String) Set(value string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.s = value
}

// Func implements Var by calling the function
// and formatting the returned value using JSON.
type Func func() interface{} // 将函数封装为Var

func (f Func) String() string { // 将函数的返回结果变为json
	v, _ := json.Marshal(f())
	return string(v)
}

// All published variables.
var (
	mutex   sync.RWMutex           // 用于保护变量的读写锁
	vars    = make(map[string]Var) // 变量名和变量的对应关系
	varKeys []string               // sorted 排序后的变量key
)

// Publish declares a named exported variable. This should be called from a
// package's init function when it creates its Vars. If the name is already
// registered then this will log.Panic.
func Publish(name string, v Var) { // 声明一个命名的导出变量，注册进vars map中
	mutex.Lock()
	defer mutex.Unlock()
	if _, existing := vars[name]; existing { // 遍历所有要导出的Var，查找重复
		log.Panicln("Reuse of exported var name:", name) // 导出名重复
	}
	vars[name] = v
	varKeys = append(varKeys, name) // 将name添加到varKeys的slice中
	sort.Strings(varKeys)
}

// Get retrieves a named exported variable.
func Get(name string) Var {
	mutex.RLock()
	defer mutex.RUnlock()
	return vars[name]
}

// Convenience functions for creating new exported variables.

func NewInt(name string) *Int { // 新Publish一个Int值
	v := new(Int)
	Publish(name, v)
	return v
}

func NewFloat(name string) *Float { // 新Publish一个Float值
	v := new(Float)
	Publish(name, v)
	return v
}

func NewMap(name string) *Map { // 新Publish一个Map
	v := new(Map).Init()
	Publish(name, v)
	return v
}

func NewString(name string) *String { // 新Publish一个String
	v := new(String)
	Publish(name, v)
	return v
}

// Do calls f for each exported variable.
// The global variable map is locked during the iteration,
// but existing entries may be concurrently updated.
func Do(f func(KeyValue)) { // 对vars遍历调用函数f，可用使用Do处理当前的导出变量
	mutex.RLock()
	defer mutex.RUnlock()
	for _, k := range varKeys { // 遍历所有的变量key
		f(KeyValue{k, vars[k]})
	}
}

func expvarHandler(w http.ResponseWriter, r *http.Request) { // 处理函数，默认的http的处理
	w.Header().Set("Content-Type", "application/json; charset=utf-8") // 设置输出类型为json
	fmt.Fprintf(w, "{\n")
	first := true
	Do(func(kv KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

func cmdline() interface{} { // 返回命令行参数
	return os.Args
}

func memstats() interface{} { // 返回MemStats结构，会被Func转换为字符串
	stats := new(runtime.MemStats)
	runtime.ReadMemStats(stats)
	return *stats
}

func init() { // 导入该包时创建
	http.HandleFunc("/debug/vars", expvarHandler) // 注册debug/vars目录，处理函数为expvarHandler，自动导出一个http接口
	Publish("cmdline", Func(cmdline))
	Publish("memstats", Func(memstats))
}
