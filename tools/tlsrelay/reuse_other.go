//go:build !linux

package main

import "syscall"

func reusePortControl(_, _ string, _ syscall.RawConn) error { return nil }
