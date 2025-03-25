//go:build windows
// +build windows

package oo

import ()

const SigUsr1 = 0
const SigUsr2 = 0

func SetLimit(nFile uint64) error {
	return nil
}
