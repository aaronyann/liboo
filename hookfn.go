package oo

import (
	"reflect"
	"sort"
	"sync"
)

const HOOK_PRIO_FIRST = 0
const HOOK_PRIO_NORMAL = 10
const HOOK_PRIO_LAST = 99

type hook struct {
	Prio int
	Fn   interface{}
}

var gHookMap *sync.Map = new(sync.Map)

func AddHook(hookname string, prio int, Fn interface{}) {
	hltmp, _ := gHookMap.LoadOrStore(hookname, []hook{})
	hl, _ := hltmp.([]hook)
	hl = append(hl, hook{
		Prio: prio,
		Fn:   Fn,
	})
	sort.Slice(hl, func(i, j int) bool {
		return hl[i].Prio < hl[j].Prio
	})
	gHookMap.Store(hookname, hl)
}

func RunHookAll(hookname string, args ...interface{}) {
	if hltmp, ok := gHookMap.Load(hookname); ok {
		hl, _ := hltmp.([]hook)
		in := make([]reflect.Value, len(args))
		for k, v := range args {
			in[k] = reflect.ValueOf(v)
		}
		for _, post := range hl {
			f := reflect.ValueOf(post.Fn)
			f.Call(in)
		}
	}
}

func RunHookToError(hookname string, args ...interface{}) error {
	if hltmp, ok := gHookMap.Load(hookname); ok {
		hl, _ := hltmp.([]hook)
		in := make([]reflect.Value, len(args))
		for k, v := range args {
			in[k] = reflect.ValueOf(v)
		}
		for _, post := range hl {
			f := reflect.ValueOf(post.Fn)
			retarr := f.Call(in)
			iret := len(retarr)
			if iret > 0 && !retarr[iret-1].IsNil() {
				err, _ := retarr[iret-1].Interface().(error)
				return err
			}
		}
	}

	return nil
}

func RunHookToSuccess(hookname string, args ...interface{}) bool {
	if hltmp, ok := gHookMap.Load(hookname); ok {
		hl, _ := hltmp.([]hook)
		in := make([]reflect.Value, len(args))
		for k, v := range args {
			in[k] = reflect.ValueOf(v)
		}
		for _, post := range hl {
			f := reflect.ValueOf(post.Fn)
			retarr := f.Call(in)
			iret := len(retarr)
			if iret == 0 || retarr[iret-1].IsNil() {
				return true
			}
		}
	}

	return false
}
