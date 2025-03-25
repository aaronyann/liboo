package oo

import (
	"fmt"
	"sort"
	"strings"
	// "sync"
)

type ReqCtxHandler = func(*ReqCtx, *RpcMsg) (*RpcMsg, error)

type ReqCtxFilter = func(*ReqCtx, *RpcMsg, int64) error

type ctxFilter = struct {
	Prio int
	Fn   ReqCtxFilter
}

type ctxHandler = struct {
	hFlag int64
	Fn    ReqCtxHandler
}

type ReqDispatcher struct {
	hName      string
	inFilters  []*ctxFilter
	outFilters []*ctxFilter
	// handlersMap *sync.Map //cmd-->[]*ctxHandler
	handlersMap map[string]([]*ctxHandler)
	defHandler  ReqCtxHandler //
}

func CreateReqDispatcher(name ...string) (cmap *ReqDispatcher) {
	cmap = &ReqDispatcher{}
	if len(name) > 0 {
		cmap.hName = name[0]
	}
	// cmap.handlersMap = new(sync.Map)
	cmap.handlersMap = map[string]([]*ctxHandler){}
	return
}
func MergeReqDispatcher(unique bool, cmaps ...*ReqDispatcher) (cmap *ReqDispatcher) {
	cmap = CreateReqDispatcher()
	cmap.Merge(unique, cmaps...)
	return
}
func (cmap *ReqDispatcher) Merge(unique bool, cmaps ...*ReqDispatcher) (cmap_ret *ReqDispatcher) {
	for _, cmap1 := range cmaps {
		cmap.hName += fmt.Sprintf(".%s", cmap1.hName)
		for cmd := range cmap1.handlersMap {
			if !unique || cmap.handlersMap[cmd] == nil {
				//
				//
				cmap.AddHandler(cmd, cmap1.RunHandler)
			}
		}

		if cmap.defHandler == nil {
			cmap.defHandler = cmap1.defHandler
		}
	}
	cmap_ret = cmap
	return
	// return MergeReqDispatcher(unique, append([]*ReqDispatcher{cmap}, cmaps...)...)
}

func (cmap *ReqDispatcher) AddHandler(cmd string, fn ReqCtxHandler, hFlags ...int64) {
	h := ctxHandler{
		Fn: fn,
	}
	if len(hFlags) > 0 {
		h.hFlag = hFlags[0]
	}
	if hs, ok := cmap.handlersMap[cmd]; ok {
		hs = append(hs, &h)
		cmap.handlersMap[cmd] = hs
	} else {
		cmap.handlersMap[cmd] = []*ctxHandler{&h}
	}
}

func (cmap *ReqDispatcher) SetDefHandler(fn ReqCtxHandler) {
	cmap.defHandler = fn
}
func (cmap *ReqDispatcher) AddInFilter(prio int, fn ReqCtxFilter) {
	cmap.inFilters = append(cmap.inFilters, &ctxFilter{
		Prio: prio,
		Fn:   fn,
	})
	sort.Slice(cmap.inFilters, func(i, j int) bool {
		return cmap.inFilters[i].Prio < cmap.inFilters[j].Prio
	})
}
func (cmap *ReqDispatcher) AddOutFilter(prio int, fn ReqCtxFilter) {
	cmap.outFilters = append(cmap.outFilters, &ctxFilter{
		Prio: prio,
		Fn:   fn,
	})
	sort.Slice(cmap.outFilters, func(i, j int) bool {
		return cmap.outFilters[i].Prio < cmap.outFilters[j].Prio
	})
}
func (cmap *ReqDispatcher) AppendDispatcher(cmaps ...*ReqDispatcher) {
	for _, cmap1 := range cmaps {
		cmap.hName += fmt.Sprintf(".%s", cmap1.hName)
		for cmd := range cmap1.handlersMap {
			if _, ok := cmap.handlersMap[cmd]; !ok {
				//
				cmap.AddHandler(cmd, cmap1.RunHandler)
			}
		}
		if cmap.defHandler == nil {
			cmap.defHandler = cmap1.defHandler
		}
	}
}

func (cmap *ReqDispatcher) runOneHanler(ph *ctxHandler, ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
	for _, ff := range cmap.inFilters {
		//
		if err = ff.Fn(ctx, reqmsg, ph.hFlag); err != nil {
			return
		}
	}

	if rspmsg, err = ph.Fn(ctx, reqmsg); err == nil && rspmsg != nil {
		//
		for _, ff := range cmap.outFilters {
			//
			if err = ff.Fn(ctx, rspmsg, ph.hFlag); err != nil {
				return
			}
		}
	}
	return
}

func (cmap *ReqDispatcher) RunHandlerEx(cmd string, ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
	//defer
	if phs, ok := cmap.handlersMap[cmd]; ok {
		if len(phs) == 1 {
			rspmsg, err = cmap.runOneHanler(phs[0], ctx, reqmsg)
			return
		}

		paras := []interface{}{}
		for i, ph := range phs {
			reqmsg1 := *reqmsg
			rspmsg, err = cmap.runOneHanler(ph, ctx, &reqmsg1)
			if err == nil && rspmsg != nil && len(rspmsg.Para) > 0 {
				paras = append(paras, rspmsg.Para)
			}
			if err != nil {

				LogD("%s run %s[%d] flag %x err: %v", cmap.hName, cmd, i, ph.hFlag, err)
			}
		}

		err = nil
		rspmsg = PackRspMsg(reqmsg, ESUCC, paras)
	} else if cmap.defHandler != nil {
		rspmsg, err = cmap.runOneHanler(&ctxHandler{Fn: cmap.defHandler}, ctx, reqmsg)
	} else {
		LogD("%s skip cmd %s", cmap.hName, cmd)
	}

	return
}

func (cmap *ReqDispatcher) RunHandler(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
	return cmap.RunHandlerEx(reqmsg.Cmd, ctx, reqmsg)
}

func (cmap *ReqDispatcher) RunHandlerMark(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
	return cmap.RunHandlerEx(reqmsg.Mark, ctx, reqmsg)
}

func (cmap *ReqDispatcher) GetHandlersMap() map[string]([]*ctxHandler) {
	return cmap.handlersMap
}

func (cmap *ReqDispatcher) Print() (s string) {
	for _, f := range cmap.inFilters {
		s += fmt.Sprintf("infilter %d, %p\n", f.Prio, f.Fn)
	}

	for cmd, phs := range cmap.handlersMap {
		hans := []string{}
		for i, ph := range phs {
			hans = append(hans, fmt.Sprintf("(%d,0x%08x,%p)", i, ph.hFlag, ph.Fn))
		}
		s += fmt.Sprintf("handler %s-->[%s]\n", cmd, strings.Join(hans, ","))
	}

	for _, f := range cmap.outFilters {
		s += fmt.Sprintf("outfilter %d, %p\n", f.Prio, f.Fn)
	}
	return
}
