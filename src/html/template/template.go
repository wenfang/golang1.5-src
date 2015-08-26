// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package template

import (
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync"
	"text/template"
	"text/template/parse"
)

// Template is a specialized Template from "text/template" that produces a safe
// HTML document fragment.
type Template struct { // 模板结构
	// Sticky error if escaping fails.
	escapeErr error // escape时是否发生错误
	// We could embed the text/template field, but it's safer not to because
	// we need to keep our version of the name space and the underlying
	// template's in sync.
	text *template.Template // 内部嵌入text/template
	// The underlying template's parse tree, updated to be HTML-safe.
	Tree       *parse.Tree // 解析树
	*nameSpace             // common to all associated templates 该模板属于的命名空间
}

// escapeOK is a sentinel value used to indicate valid escaping.
var escapeOK = fmt.Errorf("template escaped correctly")

// nameSpace is the data structure shared by all templates in an association.
type nameSpace struct { // 所有关联的数据结构共享同一命名空间
	mu  sync.Mutex
	set map[string]*Template // 命名空间内所有template的集合
}

// Templates returns a slice of the templates associated with t, including t
// itself.
func (t *Template) Templates() []*Template { // 返回与t模板属于同一命名空间的所有模板
	ns := t.nameSpace
	ns.mu.Lock()
	defer ns.mu.Unlock()
	// Return a slice so we don't expose the map.
	m := make([]*Template, 0, len(ns.set))
	for _, v := range ns.set {
		m = append(m, v)
	}
	return m
}

// Option sets options for the template. Options are described by
// strings, either a simple string or "key=value". There can be at
// most one equals sign in an option string. If the option string
// is unrecognized or otherwise invalid, Option panics.
//
// Known options:
//
// missingkey: Control the behavior during execution if a map is
// indexed with a key that is not present in the map.
//	"missingkey=default" or "missingkey=invalid"
//		The default behavior: Do nothing and continue execution.
//		If printed, the result of the index operation is the string
//		"<no value>".
//	"missingkey=zero"
//		The operation returns the zero value for the map type's element.
//	"missingkey=error"
//		Execution stops immediately with an error.
//
func (t *Template) Option(opt ...string) *Template {
	t.text.Option(opt...)
	return t
}

// escape escapes all associated templates.
func (t *Template) escape() error { // escape所有相关模板
	t.nameSpace.mu.Lock()
	defer t.nameSpace.mu.Unlock()
	if t.escapeErr == nil {
		if t.Tree == nil {
			return fmt.Errorf("template: %q is an incomplete or empty template%s", t.Name(), t.text.DefinedTemplates())
		}
		if err := escapeTemplate(t, t.text.Root, t.Name()); err != nil {
			return err
		}
	} else if t.escapeErr != escapeOK {
		return t.escapeErr
	}
	return nil
}

// Execute applies a parsed template to the specified data object,
// writing the output to wr.
// If an error occurs executing the template or writing its output,
// execution stops, but partial results may already have been written to
// the output writer.
// A template may be executed safely in parallel.
func (t *Template) Execute(wr io.Writer, data interface{}) error { // 执行Template的主模板
	if err := t.escape(); err != nil {
		return err
	}
	return t.text.Execute(wr, data)
}

// ExecuteTemplate applies the template associated with t that has the given
// name to the specified data object and writes the output to wr.
// If an error occurs executing the template or writing its output,
// execution stops, but partial results may already have been written to
// the output writer.
// A template may be executed safely in parallel.
func (t *Template) ExecuteTemplate(wr io.Writer, name string, data interface{}) error { // 执行名字为name的模板
	tmpl, err := t.lookupAndEscapeTemplate(name) // 查找名字为name的模板
	if err != nil {
		return err
	}
	return tmpl.text.Execute(wr, data)
}

// lookupAndEscapeTemplate guarantees that the template with the given name
// is escaped, or returns an error if it cannot be. It returns the named
// template.
func (t *Template) lookupAndEscapeTemplate(name string) (tmpl *Template, err error) {
	t.nameSpace.mu.Lock()
	defer t.nameSpace.mu.Unlock()
	tmpl = t.set[name] // 先从namespace中取得模板结构
	if tmpl == nil {
		return nil, fmt.Errorf("html/template: %q is undefined", name)
	}
	if tmpl.escapeErr != nil && tmpl.escapeErr != escapeOK {
		return nil, tmpl.escapeErr
	}
	if tmpl.text.Tree == nil || tmpl.text.Root == nil {
		return nil, fmt.Errorf("html/template: %q is an incomplete template", name)
	}
	if t.text.Lookup(name) == nil { // 在主模板中查找名字
		panic("html/template internal error: template escaping out of sync")
	}
	if tmpl.escapeErr == nil {
		err = escapeTemplate(tmpl, tmpl.text.Root, name)
	}
	return tmpl, err // 返回模板结构
}

// Parse parses a string into a template. Nested template definitions
// will be associated with the top-level template t. Parse may be
// called multiple times to parse definitions of templates to associate
// with t. It is an error if a resulting template is non-empty (contains
// content other than template definitions) and would replace a
// non-empty template with the same name.  (In multiple calls to Parse
// with the same receiver template, only one call can contain text
// other than space, comments, and template definitions.)
func (t *Template) Parse(src string) (*Template, error) { // 解析字符串
	t.nameSpace.mu.Lock()
	t.escapeErr = nil
	t.nameSpace.mu.Unlock()
	ret, err := t.text.Parse(src) // 调用text的Parse
	if err != nil {
		return nil, err
	}
	// In general, all the named templates might have changed underfoot.
	// Regardless, some new ones may have been defined.
	// The template.Template set has been updated; update ours.
	t.nameSpace.mu.Lock()
	defer t.nameSpace.mu.Unlock()
	for _, v := range ret.Templates() { // 把所有相关的嵌套模板也加入集合中
		name := v.Name()
		tmpl := t.set[name]
		if tmpl == nil {
			tmpl = t.new(name)
		}
		// Restore our record of this text/template to its unescaped original state.
		tmpl.escapeErr = nil
		tmpl.text = v
		tmpl.Tree = v.Tree
	}
	return t, nil
}

// AddParseTree creates a new template with the name and parse tree
// and associates it with t.
//
// It returns an error if t has already been executed.
func (t *Template) AddParseTree(name string, tree *parse.Tree) (*Template, error) {
	t.nameSpace.mu.Lock()
	defer t.nameSpace.mu.Unlock()
	if t.escapeErr != nil {
		return nil, fmt.Errorf("html/template: cannot AddParseTree to %q after it has executed", t.Name())
	}
	text, err := t.text.AddParseTree(name, tree)
	if err != nil {
		return nil, err
	}
	ret := &Template{
		nil,
		text,
		text.Tree,
		t.nameSpace,
	}
	t.set[name] = ret
	return ret, nil
}

// Clone returns a duplicate of the template, including all associated
// templates. The actual representation is not copied, but the name space of
// associated templates is, so further calls to Parse in the copy will add
// templates to the copy but not to the original. Clone can be used to prepare
// common templates and use them with variant definitions for other templates
// by adding the variants after the clone is made.
//
// It returns an error if t has already been executed.
func (t *Template) Clone() (*Template, error) {
	t.nameSpace.mu.Lock()
	defer t.nameSpace.mu.Unlock()
	if t.escapeErr != nil {
		return nil, fmt.Errorf("html/template: cannot Clone %q after it has executed", t.Name())
	}
	textClone, err := t.text.Clone()
	if err != nil {
		return nil, err
	}
	ret := &Template{
		nil,
		textClone,
		textClone.Tree,
		&nameSpace{
			set: make(map[string]*Template),
		},
	}
	for _, x := range textClone.Templates() {
		name := x.Name()
		src := t.set[name]
		if src == nil || src.escapeErr != nil {
			return nil, fmt.Errorf("html/template: cannot Clone %q after it has executed", t.Name())
		}
		x.Tree = x.Tree.Copy()
		ret.set[name] = &Template{
			nil,
			x,
			x.Tree,
			ret.nameSpace,
		}
	}
	return ret, nil
}

// New allocates a new HTML template with the given name.
func New(name string) *Template { // 创建一个新的模板结构，为模板设定一个名字
	tmpl := &Template{ // 创建一个Template结构
		nil,
		template.New(name), // 生成一个text的template
		nil,
		&nameSpace{
			set: make(map[string]*Template),
		},
	}
	tmpl.set[name] = tmpl // 在命名空间中设置模板
	return tmpl
}

// New allocates a new HTML template associated with the given one
// and with the same delimiters. The association, which is transitive,
// allows one template to invoke another with a {{template}} action.
func (t *Template) New(name string) *Template { // 在t模板的基础上，创建一个新模板
	t.nameSpace.mu.Lock()
	defer t.nameSpace.mu.Unlock()
	return t.new(name)
}

// new is the implementation of New, without the lock.
func (t *Template) new(name string) *Template { // 在t的命名空间中创建一个新模板
	tmpl := &Template{ // 创建一个新模板，但是复用同一个命名空间，这样就把模板集合到了一起
		nil,
		t.text.New(name),
		nil,
		t.nameSpace, // 复用namespace
	}
	tmpl.set[name] = tmpl
	return tmpl
}

// Name returns the name of the template.
func (t *Template) Name() string { // 返回模板名
	return t.text.Name()
}

// FuncMap is the type of the map defining the mapping from names to
// functions. Each function must have either a single return value, or two
// return values of which the second has type error. In that case, if the
// second (error) argument evaluates to non-nil during execution, execution
// terminates and Execute returns that error. FuncMap has the same base type
// as FuncMap in "text/template", copied here so clients need not import
// "text/template".
type FuncMap map[string]interface{}

// Funcs adds the elements of the argument map to the template's function map.
// It panics if a value in the map is not a function with appropriate return
// type. However, it is legal to overwrite elements of the map. The return
// value is the template, so calls can be chained.
func (t *Template) Funcs(funcMap FuncMap) *Template {
	t.text.Funcs(template.FuncMap(funcMap))
	return t
}

// Delims sets the action delimiters to the specified strings, to be used in
// subsequent calls to Parse, ParseFiles, or ParseGlob. Nested template
// definitions will inherit the settings. An empty delimiter stands for the
// corresponding default: {{ or }}.
// The return value is the template, so calls can be chained.
func (t *Template) Delims(left, right string) *Template {
	t.text.Delims(left, right)
	return t
}

// Lookup returns the template with the given name that is associated with t,
// or nil if there is no such template.
func (t *Template) Lookup(name string) *Template {
	t.nameSpace.mu.Lock()
	defer t.nameSpace.mu.Unlock()
	return t.set[name]
}

// Must is a helper that wraps a call to a function returning (*Template, error)
// and panics if the error is non-nil. It is intended for use in variable initializations
// such as
//	var t = template.Must(template.New("name").Parse("html"))
func Must(t *Template, err error) *Template { // 要求模板必须创建成功，否则抛出异常
	if err != nil {
		panic(err)
	}
	return t
}

// ParseFiles creates a new Template and parses the template definitions from
// the named files. The returned template's name will have the (base) name and
// (parsed) contents of the first file. There must be at least one file.
// If an error occurs, parsing stops and the returned *Template is nil.
func ParseFiles(filenames ...string) (*Template, error) { // 解析一系列文件，生成模板
	return parseFiles(nil, filenames...)
}

// ParseFiles parses the named files and associates the resulting templates with
// t. If an error occurs, parsing stops and the returned template is nil;
// otherwise it is t. There must be at least one file.
func (t *Template) ParseFiles(filenames ...string) (*Template, error) {
	return parseFiles(t, filenames...)
}

// parseFiles is the helper for the method and function. If the argument
// template is nil, it is created from the first file.
func parseFiles(t *Template, filenames ...string) (*Template, error) { // 解析文件名，返回模板，其实是一系列的模板
	if len(filenames) == 0 { // 文件名slice长度为0，不合法，必须至少有一个文件
		// Not really a problem, but be consistent.
		return nil, fmt.Errorf("html/template: no files named in call to ParseFiles")
	}
	for _, filename := range filenames { // 遍历每个文件名
		b, err := ioutil.ReadFile(filename) // 读文件内容
		if err != nil {
			return nil, err
		}
		s := string(b)                  // 将文件内容转换成字符串
		name := filepath.Base(filename) // 读取最后一项name作为模板的name
		// First template becomes return value if not already defined,
		// and we use that one for subsequent New calls to associate
		// all the templates together. Also, if this file has the same name
		// as t, this file becomes the contents of t, so
		//  t, err := New(name).Funcs(xxx).ParseFiles(name)
		// works. Otherwise we create a new template associated with t.
		var tmpl *Template
		if t == nil { // 没有模板创建新的
			t = New(name)
		}
		if name == t.Name() { // 如果模板名相同，赋给tmpl
			tmpl = t
		} else {
			tmpl = t.New(name) // 如果模板名不同，根据文件名，在t的基础上创建新模板
		}
		_, err = tmpl.Parse(s) // 解析文件内容
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}

// ParseGlob creates a new Template and parses the template definitions from the
// files identified by the pattern, which must match at least one file. The
// returned template will have the (base) name and (parsed) contents of the
// first file matched by the pattern. ParseGlob is equivalent to calling
// ParseFiles with the list of files matched by the pattern.
func ParseGlob(pattern string) (*Template, error) {
	return parseGlob(nil, pattern)
}

// ParseGlob parses the template definitions in the files identified by the
// pattern and associates the resulting templates with t. The pattern is
// processed by filepath.Glob and must match at least one file. ParseGlob is
// equivalent to calling t.ParseFiles with the list of files matched by the
// pattern.
func (t *Template) ParseGlob(pattern string) (*Template, error) {
	return parseGlob(t, pattern)
}

// parseGlob is the implementation of the function and method ParseGlob.
func parseGlob(t *Template, pattern string) (*Template, error) {
	filenames, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(filenames) == 0 {
		return nil, fmt.Errorf("html/template: pattern matches no files: %#q", pattern)
	}
	return parseFiles(t, filenames...)
}
