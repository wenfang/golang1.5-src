// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build linux darwin dragonfly freebsd openbsd netbsd solaris

package tar

import (
	"os"
	"syscall"
)

func init() {
	sysStat = statUnix // 初始化将sysStat赋值为statUnix
}

func statUnix(fi os.FileInfo, h *Header) error {
	sys, ok := fi.Sys().(*syscall.Stat_t) // 获得Stat_t结构
	if !ok {
		return nil
	}
	h.Uid = int(sys.Uid) // 获得Uid
	h.Gid = int(sys.Gid) // 获得Gid
	// TODO(bradfitz): populate username & group.  os/user
	// doesn't cache LookupId lookups, and lacks group
	// lookup functions.
	h.AccessTime = statAtime(sys) // 获得访问时间
	h.ChangeTime = statCtime(sys)
	// TODO(bradfitz): major/minor device numbers?
	return nil
}
