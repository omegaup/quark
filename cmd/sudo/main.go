package main

import (
	"fmt"
	"os"
	"runtime"
	"syscall"
)

func init() {
	// Force this program to be single-threaded. Otherwise setuid/setgid will not
	// behave correctly, since they only affect the current thread and not the
	// whole process.
	runtime.GOMAXPROCS(1)
	runtime.LockOSThread()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(
			os.Stderr,
			"Usage: %s [--setup-setuid] /var/lib/omegajail/bin/omegajail args...\n",
			os.Args[0],
		)
		os.Exit(1)
	}

	if os.Args[1] == "--setup-setuid" {
		// Since the container does not have any way of changing permissions
		// because it does not have any binary, let this program set the
		// setuid/setgid bits on itself.
		if err := os.Chmod(os.Args[0], os.ModeSetgid|os.ModeSetuid|0755); err != nil {
			panic(err)
		}
		os.Exit(0)
	} else if os.Args[1] != "/var/lib/omegajail/bin/omegajail" {
		fmt.Fprintf(os.Stderr, "Only omegajail is supported\n")
		os.Exit(2)
	}

	// syscall.{Setuid,Setgid} are currently unimplemented, so they need to be
	// invoked using RawSyscall.
	if _, _, err := syscall.RawSyscall(syscall.SYS_SETUID, 0, 0, 0); err != 0 {
		fmt.Fprintf(os.Stderr, "setuid: %s\n", syscall.Errno(err).Error())
	}
	if _, _, err := syscall.RawSyscall(syscall.SYS_SETGID, 0, 0, 0); err != 0 {
		fmt.Fprintf(os.Stderr, "setgid: %s\n", syscall.Errno(err).Error())
	}

	env := []string{"SUDO_USER=nobody"}
	if err := syscall.Exec(os.Args[1], os.Args[1:], env); err != nil {
		panic(err)
	}
}
