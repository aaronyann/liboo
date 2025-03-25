package oo

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

const SigTerm = syscall.SIGTERM
const SigHup = syscall.SIGHUP

// const SigUsr1 = syscall.SIGUSR1
// const SigUsr2 = syscall.SIGUSR2

type SignalHandler struct {
	sig syscall.Signal
	ch  chan os.Signal
}

func NewSignalHandler(sig syscall.Signal) *SignalHandler {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, sig)

	h := &SignalHandler{sig: sig, ch: ch}
	return h
}
func (h *SignalHandler) GetChan() <-chan os.Signal {
	return h.ch
}

func GoRunProc(core int) {
	if core < 1 {
		core = 1
	}
	if core > runtime.NumCPU() {
		core = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(core)
}

var FnSigUsr1 = (func())(nil)
var FnSigHup = (func())(nil)

func WaitExitSignal() {
	sig := NewSignalHandler(SigTerm)
	sigusr1 := NewSignalHandler(SigUsr1)
	sigusr2 := NewSignalHandler(SigUsr2)
	sighup := NewSignalHandler(SigHup)
	prof_flag := false
	for {
		select {
		case <-sig.GetChan():
			os.Exit(0)
		case <-sighup.GetChan(): //
			// GDebug = 1 - GDebug //
			if FnSigHup != nil {
				FnSigHup()
			}
		case <-sigusr1.GetChan():
			if FnSigUsr1 != nil {
				FnSigUsr1()
			}
		case <-sigusr2.GetChan():
			if !prof_flag {
				SaveStacks()
				OpenCpuProfiling()
			} else {
				CloseCpuProfiling()
				SaveStacks()
			}
			prof_flag = !prof_flag
		}
	}
}
func SaveStacks() {
	fname := fmt.Sprintf("%s.stack.log", filepath.Base(os.Args[0]))
	buf := make([]byte, 16384000)
	buf = buf[:runtime.Stack(buf, true)]

	fd, _ := os.OpenFile(fname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)

	now := time.Now().Format("2006-01-02 15:04:05")
	fd.WriteString("\n\n\n\n\n")
	fd.WriteString(now + " stdout:" + "\n\n")
	fd.Write(buf)
	fd.Close()
}

var glof *os.File

func OpenCpuProfiling() {
	fname := fmt.Sprintf("%s.cpu.log", filepath.Base(os.Args[0]))
	if f, err := os.Create(fname); err == nil {
		glof = f
		//  CPU profiling
		pprof.StartCPUProfile(f)
	}
}

func CloseCpuProfiling() {
	if glof != nil {
		pprof.StopCPUProfile()
		glof.Close()
		glof = nil
	}

	fname := fmt.Sprintf("%s.mem.log", filepath.Base(os.Args[0]))
	if f, err := os.Create(fname); err == nil {
		runtime.GC()
		// pprof.WriteHeapProfile(f)
		pprof.Lookup("heap").WriteTo(f, 1)
		f.Close()
	}
}
