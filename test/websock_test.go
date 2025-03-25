package oo

import (
	. "github.com/donnie24680/liboo"
	"math/rand"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestUrl(t *testing.T) {

	var u url.URL
	u.Scheme = "wss"
	u.Host = "22.33:9988"
	u.Path = "/?appid=57864c685bd54b95a1360bea0d60c4c0&randstr=323434&sign=9a7f938a0948ecf087710b9512a4dc53"
	// query := ""
	if pp := strings.IndexByte(u.Path, '?'); pp >= 0 {
		u.RawQuery = string(u.Path[pp+1:])
		u.Path = string(u.Path[:pp])
	}

	t.Logf("url: %#v, %s", u, u.String())
}
func Test_PerfClient(t *testing.T) {
	chmgr := NewChannelManager(1024) //再高就得设置nfile
	var err error
	var wss [1]*WebSock
	for i := 0; i < len(wss); i++ {
		wss[i], err = InitWsClient("wss", "testminer.hdpool.com", "/", chmgr)
		if err != nil {
			LogD("i=%d, err=%s", i, err.Error())
			continue
		}

		wss[i].SetIntervalHandler(func(it *WebSock, ms int64) {
			last_snt_ms, ok := it.Data.(int64)
			if !ok || last_snt_ms+5000 < ms {
				it.Data = ms + rand.Int63n(13000)

				it.SendRpc(&RpcMsg{Cmd: "poolmgr.heartbeat"})
				LogD("Sent heartbeat")
			}
		})
		//wss[i].SkipHandleFunc("poolmgr.heartbeat")
		//wss[i].SkipHandleFunc("poolmgr.mining_info")
		wss[i].HandleFunc("poolmgr.heartbeat", func(ctx *ReqCtx, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
			LogD("heartbeat")
			return
		})
		wss[i].HandleFunc("poolmgr.mining_info", func(c *WebSock, reqmsg *RpcMsg) (rspmsg *RpcMsg, err error) {
			LogD("mining_info")
			return
		})
		wss[i].SetReadTimeout(12) //15秒超时
		go wss[i].StartDial(1, "")
		LogD("Start == %d", i)
	}

	<-time.After(3600 * time.Second)

	for i := 0; i < len(wss); i++ {
		if wss[i] != nil {
			wss[i].Close()
		}
	}
	t.Log("end.")
}

func WSServer(t *testing.T) {

}

func WSClient(t *testing.T) {

}
