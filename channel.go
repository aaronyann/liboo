package oo

import (
	"errors"
	"sync"
	"sync/atomic"
)

type ChannelManager struct {
	rpcSequence uint64
	channelMap  sync.Map
	chlen       uint64
}

// channel pair
type Channel struct {
	// uid      uint64
	Data     interface{}
	sequence uint64
	respChan chan interface{}
	closed   chan struct{}
	Chmgr    *ChannelManager
}

func (ch *Channel) GetSeq() uint64 {
	return ch.sequence
}
func (ch *Channel) GetSeqType() int {
	return int(ch.sequence >> 56)
}

func (ch *Channel) Close() {
	ch.Chmgr.DelChannel(ch)
}

func (ch *Channel) IsClosed() <-chan struct{} {
	return ch.closed
}

func (ch *Channel) PushMsg(m interface{}) (err error) {
	defer func() {
		if errs := recover(); errs != nil {
			LogW("Failed recover: %v", errs)
		}
	}()
	select {
	case ch.respChan <- m:
		return nil
	case <-ch.closed:
		return errors.New("chan closed")
	default:
		return errors.New("chan full")
	}

}

func (ch *Channel) RecvChan() <-chan interface{} {
	return ch.respChan
}

// -----
func (cm *ChannelManager) NewChannel(seq_high8 ...int) *Channel {
	ch := &Channel{respChan: make(chan interface{}, cm.chlen), closed: make(chan struct{})}
	ch.sequence = atomic.AddUint64(&cm.rpcSequence, 1)
	if len(seq_high8) != 0 {
		ch.sequence += uint64(seq_high8[0]&0xFF) << 56
	}
	ch.Chmgr = cm
	cm.channelMap.Store(ch.sequence, ch)
	return ch
}
func (cm *ChannelManager) DelChannel(ch *Channel) {
	cm.channelMap.Delete(ch.sequence)
	close(ch.closed)
	close(ch.respChan)
}
func (cm *ChannelManager) GetChannel(seq uint64) (*Channel, error) {
	if v, ok := cm.channelMap.Load(seq); ok {
		ch, _ := v.(*Channel)
		return ch, nil
	}

	return nil, errors.New("not found")
}
func (cm *ChannelManager) PushChannelMsg(seq uint64, m interface{}) error {
	if nil == cm {
		return errors.New("no manager")
	}
	if v, ok := cm.channelMap.Load(seq); ok {
		ch, _ := v.(*Channel)
		return ch.PushMsg(m)
	} else {
		return errors.New("not found")
	}
}
func (cm *ChannelManager) PushAllChannelMsg(m interface{}, fn func(ch *Channel) bool) (n int, err error) {
	if nil == cm {
		err = errors.New("no manager")
		return
	}
	cm.channelMap.Range(func(key, value interface{}) bool {
		ch, _ := value.(*Channel)
		if fn == nil || fn(ch) {
			if ch.PushMsg(m) == nil {
				n++
			}
		}

		return true
	})
	return
}
func (cm *ChannelManager) PushOneChannelMsg(m interface{}, fn func(ch *Channel) bool) (err error) {
	if nil == cm {
		return errors.New("no manager")
	}

	cm.channelMap.Range(func(key, value interface{}) bool {
		ch, _ := value.(*Channel)
		if fn(ch) {
			err = ch.PushMsg(m)
			return false
		}

		return true
	})
	return
}
func NewChannelManager(chlen uint64) *ChannelManager {
	cm := &ChannelManager{1, sync.Map{}, chlen}
	// if GChannelManager == nil {
	// 	GChannelManager = cm
	// }
	return cm
}
