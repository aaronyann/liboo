package oo

import (
	// "fmt"
	"sync"
	"sync/atomic"
)

type JobError struct {
	Para interface{}
	Err  error
}
type WaitJob struct {
	wg           sync.WaitGroup
	left_ntask   int64
	ch           chan interface{}
	quick_cancel bool
	cancel_ch    int32
	fn           func(para interface{}) error
	errch        chan JobError
	epara        []JobError
	errwg        sync.WaitGroup
}

func (wj *WaitJob) Push(para interface{}) {
	if wj.quick_cancel && atomic.LoadInt32(&wj.cancel_ch) != 0 {
		return
	}

	if wj.left_ntask > 0 && len(wj.ch) > 0 {
		wj.left_ntask--
		wj.wg.Add(1)
		go wj.routine()
	}
	wj.ch <- para
}

func (wj *WaitJob) Wait() (jearr []JobError) {
	close(wj.ch)
	wj.wg.Wait()

	close(wj.errch)
	wj.errwg.Wait()

	return wj.epara
}

func (wj *WaitJob) routine() {
	defer wj.wg.Add(-1)

	for {
		para, ok := <-wj.ch
		if !ok || (wj.quick_cancel && atomic.LoadInt32(&wj.cancel_ch) != 0) {
			break
		}

		if err := wj.fn(para); err != nil {
			// wj.epara = append(wj.epara, JobError{para, err})
			wj.errch <- JobError{para, err}
			if wj.quick_cancel {
				atomic.AddInt32(&wj.cancel_ch, 1)
				break
			}
		}
	}
}

func (wj *WaitJob) err_routine() {
	defer wj.errwg.Add(-1)

	for {
		e, ok := <-wj.errch
		if !ok {
			break
		}
		wj.epara = append(wj.epara, e)
	}
}

func WaitJobCreate(ntask int64, quick_cancel bool, fn func(para interface{}) error) *WaitJob {
	wj := &WaitJob{}

	wj.fn = fn
	wj.ch = make(chan interface{}, ntask)
	wj.errch = make(chan JobError, ntask)
	wj.left_ntask = ntask - 1
	wj.quick_cancel = quick_cancel

	wj.wg.Add(1)
	go wj.routine()

	wj.errwg.Add(1)
	go wj.err_routine()

	return wj
}
