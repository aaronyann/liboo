package oo

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/go-nats"
)

type NatsHandler = func(*NatsService, string, *RpcMsg) (*RpcMsg, error)

type natsMsg struct {
	subj string
	// rpcmsg *RpcMsg
	data []byte
}

type NatsService struct {
	conn     *nats.Conn
	services *map[string]bool
	ch       *Channel       //
	ch_recv  chan *nats.Msg //
	svrname  string
	svrmark  string
	sndmap   sync.Map //
	sndid    uint64   //
	co_sche  bool     //

	// msgHandlers *CtxHandlers //
	handleMap      sync.Map        //
	defHandler     NatsHandler     //
	rspChanHandler NatsHandler     //
	eventMap       sync.Map        //
	chmgr          *ChannelManager //

	ctxHandleMap  sync.Map
	ctxEventMap   sync.Map
	defCtxHandler ReqCtxHandler
}

const nats_low_watermark = uint64(1 << 62)

var GNatsService *NatsService

type NatsCfg struct {
	Servers  []string `toml:"servers,omitzero"`
	Mservers []string `toml:"mservers,omitzero"`
	User     string   `toml:"user,omitzero"`
	Pass     string   `toml:"pass,omitzero"`
	Token    string   `toml:"token,omitzero"`
}

func (s *NatsService) WaitForReady() {
	for {
		if nil != s.services {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}

	return
}

func (s *NatsService) fetchAllMonitor(mservers []string) error {
	var ss []string

	type natsConnect struct {
		SubList []string `json:"subscriptions_list,omitempty"`
	}
	type monitorInfo struct {
		Conns []*natsConnect `json:"connections,omitempty"`
	}
	type routesInfo struct {
		Routes []*natsConnect `json:"routes,omitempty"`
	}

	var minfos []*monitorInfo
	var routes []*routesInfo
	for _, url := range mservers {
		connz_url := strings.Trim(url, " ") + "/connz?subs=1"
		connz_rsp, err := http.Get(connz_url)
		if err != nil {
			continue
		}
		minfo := &monitorInfo{}
		if err = jsoniter.NewDecoder(connz_rsp.Body).Decode(minfo); err == nil {
			minfos = append(minfos, minfo)
		}
		connz_rsp.Body.Close()

		routez_url := strings.Trim(url, " ") + "/routez?subs=1"
		routez_rsp, err := http.Get(routez_url)
		if err != nil {
			continue
		}
		route := &routesInfo{}
		if err = jsoniter.NewDecoder(routez_rsp.Body).Decode(route); err == nil {
			routes = append(routes, route)
		}
		routez_rsp.Body.Close()
	}

	for _, minfo := range minfos {
		for _, conn := range minfo.Conns {
			for _, sub := range conn.SubList {
				if strings.Index(sub, "_") == 0 {
					continue
				}
				ss = append(ss, sub)
			}
		}
	}

	for _, route := range routes {
		for _, val := range route.Routes {
			for _, v := range val.SubList {
				if strings.Index(v, "_") == 0 {
					continue
				}
				ss = append(ss, v)
			}
		}
	}

	smap := MakeSubjs(ss)
	s.services = &smap

	return nil
}

// func (s *NatsService) natsSubSpec(nh func(msg *nats.Msg)) {
// 	var subj string

// 	subj = fmt.Sprintf("req.%s.*", s.svrname) //
// 	s.conn.QueueSubscribe(subj, s.svrname, nh)

// 	subj = fmt.Sprintf("req.%s.*.*.*", s.svrname) //
// 	s.conn.QueueSubscribe(subj, s.svrname, nh)

// 	subj = fmt.Sprintf("req.%s.*", s.svrmark) //
// 	s.conn.QueueSubscribe(subj, s.svrname, nh)

// 	subj = fmt.Sprintf("req.%s.*.*.*", s.svrmark) //
// 	s.conn.QueueSubscribe(subj, s.svrname, nh)

// 	//
// 	subj = fmt.Sprintf("rsp.%s.*", s.svrmark) //
// 	s.conn.QueueSubscribe(subj, s.svrname, nh)

//		s.conn.QueueSubscribeSyncWithChan(subj, queue, ch)
//		s.conn.Flush()
//	}
func (s *NatsService) natsSubSpec() {
	var subj string

	subj = fmt.Sprintf("req.%s.*", s.svrname) //
	s.conn.QueueSubscribeSyncWithChan(subj, s.svrname, s.ch_recv)

	subj = fmt.Sprintf("req.%s.*.*.*", s.svrname) //
	s.conn.QueueSubscribeSyncWithChan(subj, s.svrname, s.ch_recv)

	subj = fmt.Sprintf("req.%s.*", s.svrmark) //
	s.conn.QueueSubscribeSyncWithChan(subj, s.svrname, s.ch_recv)

	subj = fmt.Sprintf("req.%s.*.*.*", s.svrmark) //
	s.conn.QueueSubscribeSyncWithChan(subj, s.svrname, s.ch_recv)

	//
	subj = fmt.Sprintf("rsp.%s.*", s.svrmark) //
	s.conn.QueueSubscribeSyncWithChan(subj, s.svrname, s.ch_recv)

	s.conn.Flush()
}

func (s *NatsService) natsHandler(msg *nats.Msg) {
	defer func() {
		if errs := recover(); errs != nil {
			LogW("recover natsHandler.err=%v", errs)
		}
	}()
	// LogD("Get one nats :%s", msg.Subject)
	subjs := strings.Split(msg.Subject, ".") //
	if len(subjs) != 3 && len(subjs) != 5 {
		LogD("error nats cmd format %s, %d", subjs, len(subjs))
		return
	}

	rpcmsg := &RpcMsg{}
	if err := jsoniter.Unmarshal(msg.Data, rpcmsg); err != nil { //
		LogD("Failed to Unmarshal %s, err=%v", string(msg.Data), err)
		return
	}

	switch subjs[0] {
	//
	case "rsp":
		cid, err := strconv.ParseUint(subjs[2], 10, 64)
		if err != nil {
			return
		}
		if cid > nats_low_watermark { //
			if ch, ok := s.sndmap.Load(cid); ok {
				ch.(chan *RpcMsg) <- rpcmsg //
			}
			return
		}
		//
		if s.rspChanHandler != nil {
			if s.co_sche {
				go s.rspChanHandler(s, subjs[2], rpcmsg)
			} else {
				s.rspChanHandler(s, subjs[2], rpcmsg)
			}
		}

		//
	case "req":

		fn := s.defCtxHandler
		if v, ok := s.ctxHandleMap.Load(rpcmsg.Cmd); ok {
			// LogD("Get push hand fn, %s", rpcmsg.Cmd)
			fn, _ = v.(ReqCtxHandler)
		}

		modcmd := fmt.Sprintf("%s.%s", subjs[1], subjs[2])
		if fn == nil {
			LogD("skip %s: %s", modcmd, rpcmsg.Cmd)
			return
		}

		//
		call_fn := func() {
			defer func() {
				if errs := recover(); errs != nil {
					LogW("cmd:%s.err=%v", modcmd, errs)
				}
			}()
			ctx := MakeReqCtx(s, modcmd, rpcmsg.Sess) //
			if len(subjs) == 5 {
				ctx.ReplySubject = fmt.Sprintf("rsp.%s.%s", subjs[3], subjs[4])
			}
			retmsg, err := fn(ctx, rpcmsg)
			if err != nil {
				LogD("Failed to process cmd %s: %s, err: %v", modcmd, rpcmsg.Cmd, err)
				// return
			}

			if retmsg != nil && len(subjs) == 5 {
				// reply_subj := fmt.Sprintf("rsp.%s.%s", subjs[3], subjs[4])
				if err := s.PubSubjMsg(ctx.ReplySubject, retmsg); err != nil {
					LogW("Failed to pub natsmsg err:%v", err)
				}
			}
		}
		if s.co_sche {
			//
			go call_fn()
		} else {
			call_fn()
		}

	//event
	case "evt":
		modcmd := fmt.Sprintf("%s.%s", subjs[1], subjs[2])
		if v, ok := s.ctxEventMap.Load(modcmd); ok {
			fn, _ := v.(ReqCtxHandler)
			call_fn := func() {
				ctx := MakeReqCtx(s, modcmd, rpcmsg.Sess)
				fn(ctx, rpcmsg)
			}
			if s.co_sche {
				go call_fn()
			} else {
				call_fn()
			}
		}
	}
}

func (s *NatsService) checkServiceSubscriptions(subj string) bool {
	if s.services == nil {
		return false
	}
	svx := *s.services
	return CheckSubj(svx, subj)
}
func (s *NatsService) PrintServices() {
	if s.services == nil {
		LogD("no services")
	}
	svx := *s.services
	LogD("svx: %v", svx)
}

func InitNatsService(mservers []string, servers []string, svrname string) (*NatsService, error) {
	return InitNatsService2(&NatsCfg{
		Mservers: mservers,
		Servers:  servers,
	}, svrname)
}
func InitNatsService2(cfg *NatsCfg, svrname string) (*NatsService, error) {
	opts := nats.GetDefaultOptions()
	opts.Servers = cfg.Servers
	opts.Token = cfg.Token
	opts.User = cfg.User
	opts.Password = cfg.Pass
	opts.Name = fmt.Sprintf("gosvr.%s", GetSvrmark(svrname))
	opts.MaxReconnect = -1
	opts.ReconnectWait = 1 * time.Second //100 * time.Millisecond
	opts.Timeout = 1 * time.Second
	opts.ReconnectBufSize = 64 * 1024 * 1024 //8M

	conn, err := opts.Connect()
	if err != nil {
		return nil, err
	}

	service := &NatsService{conn: conn, svrname: svrname}
	service.chmgr = NewChannelManager(10240) //全
	service.ch = service.chmgr.NewChannel()
	service.sndid = nats_low_watermark //
	service.svrmark = GetSvrmark(svrname)
	service.co_sche = true                        //
	service.ch_recv = make(chan *nats.Msg, 10240) //

	if GNatsService == nil {
		GNatsService = service
	}

	//wait reply
	// service.natsSubSpec(func(msg *nats.Msg) {
	// 	//servicegc?
	// 	service.natsHandler(msg)
	// })
	service.natsSubSpec()

	//monitor
	if len(cfg.Mservers) > 0 {
		go func(service *NatsService, mservers []string) {
			for {
				service.fetchAllMonitor(mservers)

				select {
				case <-service.ch.IsClosed():
					return
				case <-time.After(1 * time.Second):
				}
			}
		}(service, cfg.Mservers)
	}

	return service, nil
}

func (s *NatsService) StartService() {
	// For:
	for {
		select {
		case m, ok := <-s.ch.RecvChan():
			if !ok {
				LogD("nats Closed.")
				return
			}
			nmsg, _ := m.(*natsMsg)
			if err := s.conn.Publish(nmsg.subj, nmsg.data); err != nil {
				LogW("write natscmd %s err: %v", nmsg.subj, err)

			}
		case nmsg, ok := <-s.ch_recv:
			if !ok {
				LogD("nats recv Closed.")
				return
			}
			s.natsHandler(nmsg)
		}

	}
}

func (s *NatsService) Close() {
	s.conn.Close()
	s.ch.Close()
	close(s.ch_recv)
}

func (s *NatsService) CloseCoroutineFlag() {
	s.co_sche = false
}

func (s *NatsService) SubRspChanHandle(nh NatsHandler) {
	s.rspChanHandler = nh
}

func compatibleNatsCtxHandler(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
	c, _ := ctx.Ctx.(*NatsService)
	natscmd := ctx.Cmd
	if n := strings.LastIndex(natscmd, "."); n > 0 {
		natscmd = string([]byte(natscmd)[n+1:])
	}
	fn := c.defHandler
	if v, ok := c.handleMap.Load(reqmsg.Cmd); ok {
		fn, _ = v.(NatsHandler)
	}

	if fn != nil {
		rspmsg, err = fn(c, natscmd, reqmsg)
	}
	return
}

func (s *NatsService) SubMsgHandle(natscmd string, nh interface{}) {
	if n := strings.LastIndex(natscmd, "."); n < 0 {
		// natscmd = string([]byte(natscmd)[n+1:])
		// bank
		natscmd = fmt.Sprintf("%s.%s", s.svrname, natscmd)
	}
	if nh == nil {
		s.handleMap.Delete(natscmd)
		s.ctxHandleMap.Delete(natscmd)
		return
	}
	switch nh.(type) {
	case ReqCtxHandler:
		s.ctxHandleMap.Store(natscmd, nh)
	case NatsHandler:
		s.handleMap.Store(natscmd, nh)
		s.ctxHandleMap.Store(natscmd, compatibleNatsCtxHandler)
	default:
		panic("handle func type error")
	}
}
func (s *NatsService) SubHandleMap(msgMap map[string]ReqCtxHandler, evtMap map[string]ReqCtxHandler) {
	for cmd, nh := range msgMap {
		s.SubMsgHandle(cmd, nh)
	}
	for cmd, nh := range evtMap {
		s.SubEventHandle(cmd, nh)
	}
}

func (s *NatsService) SubDefaultHandle(nh interface{}) {
	switch nh.(type) {
	case ReqCtxHandler:
		s.defCtxHandler = nh.(ReqCtxHandler)
	case NatsHandler:
		s.defHandler = nh.(NatsHandler)
		s.defCtxHandler = compatibleNatsCtxHandler
	default:
		panic("handle func type error")
	}
}

func compatibleNatsCtxEventHandler(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
	c, _ := ctx.Ctx.(*NatsService)

	if v, ok := c.eventMap.Load(ctx.Cmd); ok {
		if fn, _ := v.(NatsHandler); fn != nil {
			fn(c, ctx.Cmd, reqmsg)
		}
	}

	return
}

func (s *NatsService) SubEventHandle(modcmd string, nh interface{}) {
	if nh == nil {
		s.eventMap.Delete(modcmd)
		s.ctxEventMap.Delete(modcmd)
		return
	}

	switch nh.(type) {
	case ReqCtxHandler:
		s.ctxEventMap.Store(modcmd, nh)
	case NatsHandler:
		s.eventMap.Store(modcmd, nh)
		s.ctxEventMap.Store(modcmd, compatibleNatsCtxEventHandler)
	default:
		panic("handle func type error")
	}

	subj := fmt.Sprintf("evt.%s", modcmd)
	// s.conn.QueueSubscribe(subj, s.svrmark, func(msg *nats.Msg) {
	// 	s.natsHandler(msg)
	// })
	s.conn.QueueSubscribeSyncWithChan(subj, s.svrmark, s.ch_recv)
	s.conn.Flush()
}

func (s *NatsService) PubSubjData(subj string, data []byte) error {
	if !s.checkServiceSubscriptions(subj) {
		// LogD("no service, %s, now svrs:%v", subj, s.services)
		return errors.New("no service")
	}
	nmsg := &natsMsg{subj: subj, data: data}
	return s.ch.PushMsg(nmsg)
}
func (s *NatsService) PubSubjMsg(subj string, msg *RpcMsg) error {
	if !s.checkServiceSubscriptions(subj) {
		// LogD("no service, %s, now svrs:%v", subj, s.services)
		return errors.New("no service")
	}
	//JsonEncode
	// nmsg.rpcmsg.Para = []byte(JsonEncode(string(nmsg.rpcmsg.Para)))
	data, err := jsoniter.Marshal(msg)
	if err != nil || len(data) == 0 {
		return NewError("Marshal err:%v, datalen=%d", err, len(data))
	}

	nmsg := &natsMsg{subj: subj, data: data}
	return s.ch.PushMsg(nmsg)
}

func (s *NatsService) PubMsg(modcmd string, msg *RpcMsg) error {
	subj := fmt.Sprintf("req.%s", modcmd)

	return s.PubSubjMsg(subj, msg)
}

func (s *NatsService) PubByChannel(modcmd string, chseq uint64, msg *RpcMsg) error {
	if chseq >= nats_low_watermark {
		return errors.New("chseq more then 1<<62")
	}
	subj := fmt.Sprintf("req.%s.%s.%d", modcmd, s.svrmark, chseq)

	// if !s.checkServiceSubscriptions(subj) {
	// 	LogD("no service, %s, now svrs:%v", subj, s.services)
	// 	return errors.New("no service")
	// }

	return s.PubSubjMsg(subj, msg)
}

func (s *NatsService) PubWithResponse(modcmd string, msg *RpcMsg, sec int64) (rsp *RpcMsg, err error) {
	sndid := atomic.AddUint64(&s.sndid, 1)
	subj := fmt.Sprintf("req.%s.%s.%d", modcmd, s.svrmark, sndid)

	// if !s.checkServiceSubscriptions(subj) {
	// 	LogD("no service, %s, now svrs:%v", subj, s.services)
	// 	return nil, errors.New("no service")
	// }

	ch := make(chan *RpcMsg)
	defer close(ch)

	s.sndmap.Store(sndid, ch)
	defer s.sndmap.Delete(sndid)

	if err = s.PubSubjMsg(subj, msg); err != nil {
		return
	}

	select {
	case rsp = <-ch:
		return rsp, nil
	case <-time.After(time.Second * time.Duration(sec)):
		return nil, errors.New("timeout")
	}
}
func (s *NatsService) PubParseResponse(modcmd string, msg *RpcMsg, sec int64, pret interface{}) (err error) {
	rsp, err := s.PubWithResponse(modcmd, msg, sec)
	if err != nil {
		return err
	}
	if rsp.Err != nil {
		return errors.New(fmt.Sprintf("%s-%s", rsp.Err.Ret, rsp.Err.Msg))
	}
	if pret != nil {
		err = jsoniter.Unmarshal(rsp.Para, pret)
	}
	return
}

func (s *NatsService) RpcRequest(msg *RpcMsg, pret interface{}) (err error) {
	if msg.Sess == nil {
		return errors.New("rpc need sess")
	}
	//rpc 3
	return s.PubParseResponse(msg.Cmd, msg, 3, pret)
}

func (s *NatsService) PubEvent(natscmd string, msg *RpcMsg) error {
	subj := fmt.Sprintf("evt.%s", natscmd)
	if n := strings.LastIndex(natscmd, "."); n == -1 {
		subj = fmt.Sprintf("evt.%s.%s", s.svrname, natscmd)
		// natscmd = string([]byte(natscmd)[n+1:])
	}
	// LogD("%s, want PUB %s", s.svrname, subj)

	return s.PubSubjMsg(subj, msg)
}
