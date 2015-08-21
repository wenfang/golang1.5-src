// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build darwin,386 darwin,amd64 dragonfly freebsd linux nacl netbsd openbsd solaris

// Parse "zoneinfo" time zone file.
// This is a fairly standard file format used on OS X, Linux, BSD, Sun, and others.
// See tzfile(5), http://en.wikipedia.org/wiki/Zoneinfo,
// and ftp://munnari.oz.au/pub/oldtz/

package time

import (
	"errors"
	"runtime"
	"syscall"
)

func initTestingZone() {
	z, err := loadZoneFile(runtime.GOROOT()+"/lib/time/zoneinfo.zip", "America/Los_Angeles")
	if err != nil {
		panic("cannot load America/Los_Angeles for testing: " + err.Error())
	}
	z.name = "Local"
	localLoc = *z
}

// Many systems use /usr/share/zoneinfo, Solaris 2 has
// /usr/share/lib/zoneinfo, IRIX 6 has /usr/lib/locale/TZ.
var zoneDirs = []string{ // zone 遍历目录
	"/usr/share/zoneinfo/",
	"/usr/share/lib/zoneinfo/",
	"/usr/lib/locale/TZ/",
	runtime.GOROOT() + "/lib/time/zoneinfo.zip",
}

var origZoneDirs = zoneDirs

func forceZipFileForTesting(zipOnly bool) {
	zoneDirs = make([]string, len(origZoneDirs))
	copy(zoneDirs, origZoneDirs)
	if zipOnly {
		for i := 0; i < len(zoneDirs)-1; i++ {
			zoneDirs[i] = "/XXXNOEXIST"
		}
	}
}

func initLocal() { // 初始化location
	// consult $TZ to find the time zone to use.
	// no $TZ means use the system default /etc/localtime.
	// $TZ="" means use UTC.
	// $TZ="foo" means use /usr/share/zoneinfo/foo.

	tz, ok := syscall.Getenv("TZ") // 查询TZ变量
	switch {
	case !ok: // 如果不存在TZ变量
		z, err := loadZoneFile("", "/etc/localtime") // 从etc载入localtime文件
		if err == nil {                              // 如果没有错误
			localLoc = *z           // 设置localLoc
			localLoc.name = "Local" // 设置location名
			return
		}
	case tz != "" && tz != "UTC": // 存在tz变量，并且不等于空和UTC
		if z, err := loadLocation(tz); err == nil {
			localLoc = *z
			return
		}
	}

	// Fall back to UTC.
	localLoc.name = "UTC" // 设置location为UTC
}

func loadLocation(name string) (*Location, error) { // 装载Location信息
	var firstErr error
	for _, zoneDir := range zoneDirs { // 遍历zone目录
		if z, err := loadZoneFile(zoneDir, name); err == nil { // 装载Zone文件
			z.name = name
			return z, nil
		} else if firstErr == nil && !isNotExist(err) {
			firstErr = err
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, errors.New("unknown time zone " + name)
}
