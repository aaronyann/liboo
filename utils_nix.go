//go:build darwin || freebsd || linux || !windows
// +build darwin freebsd linux !windows

package oo

import (
	"fmt"
	"syscall"
)

const SigUsr1 = syscall.SIGUSR1
const SigUsr2 = syscall.SIGUSR2

func SetLimit(nFile uint64) error {
	var rLimit syscall.Rlimit
	syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	save_max := rLimit.Max

	rLimit.Max = nFile
	rLimit.Cur = nFile
	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("Error Setting Rlimit ", err)
		rLimit.Max = save_max
		rLimit.Cur = save_max
		err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	}

	syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)

	fmt.Println("Rlimit result.", rLimit, err)

	return err
}
