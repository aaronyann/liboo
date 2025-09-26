//go test -test.run
package oo

import (
	. "github.com/aaronyann/liboo"
	"net/http"
	"os"
	"testing"
	"time"
)

type Testpara = struct {
	Haa string
	Hbb int64
}

var gbit Bit64

//主模块根据Mark或第三方分发，N个子模块根据cmd分发
func TestMulti(t *testing.T) {
	h1 := CreateReqDispatcher("h1")
	h1.AddHandler("mod.cmd", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(0)
		return RpcReturn(reqmsg, err, &Testpara{Haa: "h1msg", Hbb: 111})
	})
	h1.AddHandler("mod.cmd1", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(1)
		return RpcReturn(reqmsg, err, &Testpara{Haa: "h1cmd1", Hbb: 112})
	})
	h1.SetDefHandler(func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(61)
		return
	})

	//----------------
	h2 := CreateReqDispatcher("h2")
	h2.AddHandler("mod.cmd", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(10)
		return RpcReturn(reqmsg, err, &Testpara{Haa: "h2msg", Hbb: 222})
	})
	h2.AddHandler("mod.cmd2", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(12)
		return
	})
	h2.SetDefHandler(func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(62)
		return
	})

	msg := &RpcMsg{Cmd: "mod.cmd", Para: JsonData(""), Mark: "mark"}
	msg1 := &RpcMsg{Cmd: "mod.cmd1", Para: JsonData(""), Mark: "mark1"}
	msg2 := &RpcMsg{Cmd: "mod.cmd2", Para: JsonData(""), Mark: "mark2"}
	msg3 := &RpcMsg{Cmd: "mod.cmd3", Para: JsonData(""), Mark: "mark3"}
	msg4 := &RpcMsg{Cmd: "mod.cmd4", Para: JsonData(""), Mark: "mark4"}

	gbit.Reset()

	//1. 集中分发单个: 若冲突，按顺序优先
	{
		h3 := MergeReqDispatcher(true, h1, h2)
		LogD("h3 %s", h3.Print())

		h3.RunHandler(nil, msg) //call h1
		LogBool(gbit.EquReset(0), "1.msg")

		h3.RunHandler(nil, msg1) //call h1 cmd1
		LogBool(gbit.EquReset(1), "1.msg1")

		h3.RunHandler(nil, msg2) //call h2 cmd2
		LogBool(gbit.EquReset(12), "1.msg2")

		h3.RunHandler(nil, msg3) //call h1 default
		LogBool(gbit.EquReset(61), "1.msg3")
	}

	//2. 集中分发组合: 若冲突，会合并共存
	{
		h3 := MergeReqDispatcher(false, h1, h2)
		h3.AddHandler("mod.cmd3", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
			gbit.SetBit(33)
			return
		})
		h3.SetDefHandler(func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
			gbit.SetBit(63)
			return
		})

		h3.RunHandler(nil, msg) //call h1; call h2
		LogBool(gbit.EquReset(0, 10), "2.msg")

		h3.RunHandler(nil, msg1) //call h1 cmd1
		LogBool(gbit.EquReset(1), "2.msg1")

		h3.RunHandler(nil, msg2) //call h2 cmd2
		LogBool(gbit.EquReset(12), "2.msg2")

		h3.RunHandler(nil, msg3) //call h3 cmd3
		LogBool(gbit.EquReset(33), "2.msg3")

		h3.RunHandler(nil, msg4) //call h3 default
		LogBool(gbit.EquReset(63), "2.msg4")
	}

	//3. 集中分发组合回复
	{
		h3 := MergeReqDispatcher(false, h1, h2)

		rspmsg, err := h3.RunHandler(nil, msg) //call h1; call h2
		LogBool(gbit.EquReset(0, 10), "3.msg")
		var xx []interface{}
		LogBool(err == nil && rspmsg != nil && JsonUnmarshal(rspmsg.Para, &xx) == nil && len(xx) == 2, "3.msg.ret")

		// LogD("msg ret: %v, err: %v", Bytes2Str(JsonData(rspmsg)), err)
		//输出 {"cmd":"mod.cmd","mark":"mark","para":[{"Haa":"h1msg","Hbb":111},{"Haa":"h2msg","Hbb":222}]}

		rspmsg2, err := h3.RunHandler(nil, msg1) //call h1 cmd1
		LogBool(gbit.EquReset(1), "3.msg1")
		var yy Testpara
		LogBool(err == nil && rspmsg2 != nil && JsonUnmarshal(rspmsg2.Para, &yy) == nil && yy.Hbb == 112, "3.msg1.ret")

		// LogD("msg1 ret: %v, err: %v", Bytes2Str(JsonData(rspmsg2)), err)
		//输出 {"cmd":"mod.cmd1","mark":"mark1","para":{"Haa":"h1cmd1","Hbb":111}}
	}

	//4. 根据Mark分发
	{
		h3 := CreateReqDispatcher("h3")
		h3.AddHandler("mark1", h1.RunHandler)
		h3.AddHandler("mark2", h2.RunHandler)

		h3.RunHandlerMark(nil, msg) //skip cmd mark
		LogBool(gbit.EquReset(), "4.msg")

		h3.RunHandlerMark(nil, msg1) //call h1 cmd1
		LogBool(gbit.EquReset(1), "4.msg1")

		//5.根据自己选定字段分发
		h3.RunHandlerEx("mark2", nil, msg2) //call h2 cmd2
		LogBool(gbit.EquReset(12), "5.msg2")
	}
}

func TestFilter(t *testing.T) {
	// LogD("filter")

	h := CreateReqDispatcher()
	h.AddInFilter(HOOK_PRIO_NORMAL, func(ctx *ReqCtx, reqmsg *RpcMsg, hFlag int64) error {
		gbit.ClearBit(1, 3)
		gbit.SetBit(0, 2, 4)
		return nil
	})
	h.AddInFilter(HOOK_PRIO_LAST, func(ctx *ReqCtx, reqmsg *RpcMsg, hFlag int64) error {
		gbit.ClearBit(0, 2)
		gbit.SetBit(1, 3, 5)
		return nil
	})
	h.AddInFilter(HOOK_PRIO_FIRST, func(ctx *ReqCtx, reqmsg *RpcMsg, hFlag int64) error {
		gbit.SetBit(0, 1, 2, 3)
		return nil
	})

	h.AddOutFilter(HOOK_PRIO_NORMAL, func(ctx *ReqCtx, reqmsg *RpcMsg, hFlag int64) error {
		gbit.ClearBit(11, 33)
		gbit.SetBit(10, 12, 14)
		return nil
	})
	h.AddOutFilter(HOOK_PRIO_LAST, func(ctx *ReqCtx, reqmsg *RpcMsg, hFlag int64) error {
		gbit.ClearBit(10, 12)
		gbit.SetBit(11, 13, 15)
		return nil
	})
	h.AddOutFilter(HOOK_PRIO_FIRST, func(ctx *ReqCtx, reqmsg *RpcMsg, hFlag int64) error {
		gbit.SetBit(10, 11, 12, 13)
		return nil
	})

	h.AddHandler("test", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(62)
		return
	})
	h.AddHandler("test2", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(63)
		return RpcReturn(reqmsg, err, &Testpara{Haa: "h1cmd1", Hbb: 112})
	})
	msg := &RpcMsg{Cmd: "mod.cmd", Para: JsonData(""), Mark: "mark"}

	gbit.Reset()
	h.RunHandler(nil, msg)
	LogBool(gbit.EquReset(), "1.msg") //no call filter or handler

	msg.Cmd = "test"
	h.RunHandler(nil, msg)
	LogBool(gbit.EquReset(1, 3, 4, 5, 62), "2.msg") //call infilter and handler

	msg.Cmd = "test2"
	h.RunHandler(nil, msg)
	LogBool(gbit.EquReset(1, 3, 4, 5, 11, 13, 14, 15, 63), "3.msg") //call in/han/out

}

func TestNats(t *testing.T) {
	var err error
	h := CreateReqDispatcher()
	h.AddHandler("mod.cmd", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(0)
		return RpcReturn(reqmsg, err, &Testpara{Haa: "h1msg", Hbb: 111})
	})
	h.AddHandler("mod.mark", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(1)
		return RpcReturn(reqmsg, err, &Testpara{Haa: "h1msg", Hbb: 111})
	})
	h.AddHandler("mod.other", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(2)
		return RpcReturn(reqmsg, err, &Testpara{Haa: "h1msg", Hbb: 111})
	})

	GNatsService, err = InitNatsService([]string{"http://127.0.0.1:60333"}, []string{"nats://127.0.0.1:4222"}, "mod")
	if err != nil {
		LogD("Failed nats, err:%v", err)
		return
	}
	go GNatsService.StartService()
	time.Sleep(2 * time.Second)

	msg := &RpcMsg{Cmd: "mod.cmd", Para: JsonData(""), Mark: "mod.mark"}

	gbit.Reset()

	//1. by cmd
	GNatsService.SubDefaultHandle(func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		return h.RunHandler(ctx, reqmsg)
	})
	GNatsService.PubMsg("mod.cmd888", msg)
	time.Sleep(1 * time.Second)
	LogBool(gbit.EquReset(0), "1.cmd")

	//2. by mark
	GNatsService.SubDefaultHandle(h.RunHandlerMark)
	GNatsService.PubMsg("mod.sdfsdf", msg) //有在听的
	time.Sleep(1 * time.Second)
	LogBool(gbit.EquReset(1), "2.mark")

	//3. by other
	GNatsService.SubDefaultHandle(func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		newcmd := "mod.other" //or other
		return h.RunHandlerEx(newcmd, ctx, reqmsg)
	})
	GNatsService.PubMsg("mod.testcmd", msg)
	time.Sleep(1 * time.Second)
	LogBool(gbit.EquReset(2), "3.other")

	time.Sleep(1 * time.Second)

	GNatsService.Close()
}

func TestWs(t *testing.T) {
	h := CreateReqDispatcher()
	h.AddHandler("mod.cmd", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(0)
		return RpcReturn(reqmsg, err, &Testpara{Haa: "h1msg", Hbb: 111})
	})
	h.AddHandler("mod.mark", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(1)
		//不能返回mod.cmd，否则就是个外部死循环
		return nil, nil //RpcReturn(reqmsg, err, &Testpara{Haa: "h1msg", Hbb: 111})
	})
	h.AddHandler("mod.other", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
		gbit.SetBit(2)
		return RpcReturn(reqmsg, err, &Testpara{Haa: "h1msg", Hbb: 111})
	})

	//server
	go ListenWsServer("127.0.0.1:63333", "", nil,
		func(w http.ResponseWriter, r *http.Request, client *WebSock) error {
			client.Sess = &Session{}
			client.DefHandleFunc(h.RunHandler)
			return nil
		})
	time.Sleep(1 * time.Second)

	//client
	ws, err := InitWsClient("ws", "127.0.0.1:63333", "/", nil)
	if err != nil {
		LogD("Failed to connect ws.")
		return
	}
	ws.DefHandleFunc(h.RunHandlerMark)
	go ws.StartDial(0, "")

	time.Sleep(1 * time.Second)

	msg := &RpcMsg{Cmd: "mod.cmd", Para: JsonData(""), Mark: "mod.mark"}
	msg1 := &RpcMsg{Cmd: "mod.cmd1", Para: JsonData(""), Mark: "mark1"}

	gbit.Reset()

	//1. 先按cmd，回复按mark
	ws.SendRpc(msg)
	time.Sleep(1 * time.Second)
	LogBool(gbit.EquReset(0, 1), "1.cmd")

	//2. 无注册的msg
	ws.SendRpc(msg1)
	time.Sleep(1 * time.Second)
	LogBool(gbit.EquReset(), "1.cmd1")

	ws.Close()
	//force close websocket server???
}
func TestMain(m *testing.M) {

	code := m.Run()

	os.Exit(code)
}
