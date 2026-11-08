//go:build linux

package main

import "syscall"

// reusePortControl sets SO_REUSEPORT so a bridge instance can bind the same
// ports while the draining process still holds them.
// SO_REUSEPORT = 15 on Linux (stable ABI; syscall.SO_REUSEPORT is not
// exported on every toolchain).
func reusePortControl(_, _ string, c syscall.RawConn) error {
	var serr error
	err := c.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, 0x0F, 1)
	})
	if err != nil {
		return err
	}
	return serr
}
