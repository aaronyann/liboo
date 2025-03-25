package oo

import (
	// "encoding/base64"
	// "errors"
	"encoding/json"
	"fmt"
	"regexp"

	// "sort"
	"strconv"
	"strings"

	// "sync"
	"time"
)

type ReqCtx = struct {
	Ctx          interface{} //
	Cmd          string      //
	Sess         *Session    //
	CoSche       bool        //
	ReplySubject string      //
}

// Session WebSocket
type Session = struct {
	Uid    uint64 `json:"uid,omitempty"`    //
	Key    string `json:"key,omitempty"`    //
	Gwid   string `json:"gwid,omitempty"`   //
	Connid uint64 `json:"connid,omitempty"` //
	Ipv4   uint   `json:"ipv4,omitempty"`   //
}
type Error = struct {
	Ret string `json:"ret,omitempty"` //
	Msg string `json:"msg,omitempty"` //
	Eid string `json:"eid,omitempty"` //
}
type RpcMsg = struct {
	Cmd  string          `json:"cmd,omitempty" example:"fund.get_wallet,console.get_miner_list"` // 两段式指令，格式为"名称空间.方法名"，如: "fund.get_wallet"
	Mark string          `json:"mark,omitempty"`                                                 // 矿池类型：chia_co。部分接口不应该填，具体信息查看其描述。
	Chk  string          `json:"chk,omitempty"`                                                  // 前端提供自验证的校验码
	Sess *Session        `json:"sess,omitempty"`                                                 // WebSocket会话
	Err  *Error          `json:"err,omitempty"`                                                  // 错误信息
	Para json.RawMessage `json:"para,omitempty"`                                                 // 数据。JSON string
}

type ParaNull = struct {
}

func MakeReqCtx(c interface{}, cmd string, sess *Session) (ctx *ReqCtx) {
	ctx = &ReqCtx{
		Ctx:  c,
		Cmd:  cmd,
		Sess: sess,
	}
	return
}

func GenSessid(uid uint64) string {
	return fmt.Sprintf("%d-%d", uid, time.Now().UnixNano())
}

func GenKey(sessid, passwd string, uid, expire uint64) string {
	s := fmt.Sprintf("%s|%d|%d", sessid, uid, expire)
	//Todo: aes encrypt
	en := AesEncrypt([]byte(passwd), []byte(s))
	return string(Base58Encode([]byte(en)))
}

func GenImgCode() string {
	return string(randStr(6, 0))
}
func GenEmailCode() string {
	return string(randStr(6, 0))
}

func AesDecryptKey(key, passwd string) string {
	s := Base58Decode([]byte(key))
	plainText := AesDecrypt([]byte(passwd), s)
	return string(plainText)
}

func CheckPlainKey(key string) (sessid string, uid uint64, e error) {
	var expire uint64
	sss := strings.Split(string(key), "|")
	if len(sss) < 3 {
		return "", 0, NewError("key count error, key=%s", key)
	}
	sessid = sss[0]
	uid, _ = strconv.ParseUint(sss[1], 10, 64)
	expire, _ = strconv.ParseUint(sss[2], 10, 64)

	if expire < (uint64)(time.Now().Unix()) {
		return "", 0, NewError("key timeout, key=%s", key)
	}
	return sessid, uid, nil
}

func CheckKey(key string) (sessid string, uid uint64, e error) {
	s := Base58Decode([]byte(key))
	//Todo: aes decrypt
	var expire uint64
	sss := strings.Split(string(s), "|")
	if len(sss) < 3 {
		return "", 0, NewError("key count error, s=%s", s)
	}
	sessid = sss[0]
	uid, _ = strconv.ParseUint(sss[1], 10, 64)
	expire, _ = strconv.ParseUint(sss[2], 10, 64)
	// ret, err := fmt.Sscanf(string(s), "%[^|]|%d|%d", &sessid, &uid, &expire)
	// if err != nil || ret != 3 {
	// 	return "", 0, NewError("key format failed, s=%s, err=%v", s, err)
	// }
	if expire < (uint64)(time.Now().Unix()) {
		return "", 0, NewError("key timeout, s=%s", s)
	}
	return sessid, uid, nil
}

func CheckSession(sess *Session) (sessid string, uid uint64, e error) {
	if sess == nil {
		return "", 0, NewError("no sess")
	}
	return CheckPlainKey(sess.Key)
}

func PackErr(err string) *Error {
	return &Error{Ret: err, Msg: ErrStr(err)}
}
func PackError(cmd string, err string, msg string) *RpcMsg {
	if msg == "" {
		msg = ErrStr(err)
	}
	return &RpcMsg{Cmd: cmd, Err: &Error{Ret: err, Msg: msg}}
}
func PackSess(uid uint64, key string, gwid string, connid uint64) *Session {
	return &Session{Uid: uid, Key: key, Gwid: gwid, Connid: connid}
}
func PackFatal(msg string) *RpcMsg {
	return &RpcMsg{Cmd: "fatal.error", Err: &Error{Ret: EFATAL, Msg: msg}}
}
func PackRpcMsg(cmd string, para interface{}, sess *Session) *RpcMsg {
	return &RpcMsg{Cmd: cmd, Para: JsonData(para), Sess: sess}
}
func PackPara(cmd string, para interface{}) *RpcMsg {
	return &RpcMsg{Cmd: cmd, Para: JsonData(para)}
}
func PackNull(cmd string) *RpcMsg {
	return &RpcMsg{Cmd: cmd}
}
func PackRspMsg(reqmsg *RpcMsg, eret string, rsp interface{}, eid ...string) (rspmsg *RpcMsg) {
	rspmsg = reqmsg
	if eret == ESUCC {
		rspmsg.Para = JsonData(rsp)
	} else {
		rspmsg.Para = nil
		rspmsg.Err = PackErr(eret)
		if len(eid) > 0 {
			rspmsg.Err.Eid = eid[0]
		}
	}
	return
}

// var GAllowErrors = make(map[string][]string)

// func RpcAllocErrors(cmd string, errs []string) {
// 	GAllowErrors[cmd] = errs
// }

func RpcReturn(reqmsg *RpcMsg, eret_rpcerr interface{}, rsp ...interface{}) (rspmsg *RpcMsg, err error) {
	rspmsg = reqmsg
	rspmsg.Para = nil

	var is_succ bool
	if eret, ok := eret_rpcerr.(string); ok && eret == ESUCC {
		is_succ = true
	}
	if eret_rpcerr == nil || is_succ {
		if len(rsp) > 0 {
			rspmsg.Para = JsonData(rsp[0])
		}
		return
	}
	switch eret_rpcerr.(type) {
	case string:
		eret, _ := eret_rpcerr.(string)
		rspmsg.Err = PackErr(eret)
	case *RpcError:
		rpcerr, _ := eret_rpcerr.(*RpcError)
		// rspmsg.Err = PackErr(rpcerr.Errno())
		rspmsg.Err = &Error{Ret: rpcerr.Eno, Msg: rpcerr.Err.Error()}
	case error:
		rspmsg.Err = PackErr(ESERVER)
		err, _ = eret_rpcerr.(error)
	default:
		rspmsg.Err = PackErr(ESERVER)
	}

	// if errs, ok := RpcAllocErrors[rspmsg.Cmd]; ok && !InArray(rspmsg.Err.Ret, errs) {
	//
	// 	panic("")
	// }
	return
}

func MakeSubjs(subjs []string) (m map[string]bool) {
	m = make(map[string]bool)

	for _, cmd := range subjs {
		cmd = strings.Split(cmd, ":")[0]
		if Str2Bytes(cmd)[0] == '-' {
			m[cmd[1:]] = false
		} else {
			m[cmd] = true
		}
	}
	return
}
func CheckSubj(m map[string]bool, subj string) bool {
	if len(m) == 0 {
		return false
	}
	if c, ok := m[subj]; ok {
		return c
	}
	subjs := strings.Split(subj, ".")
	for i := len(subjs) - 1; i > 0; i-- {
		subjs[i] = "*"
		subj = strings.Join(subjs, ".")
		if c, ok := m[subj]; ok {
			return c
		}
	}
	return false
}

func CheckEmail(email string) bool {
	b, _ := regexp.MatchString("^([A-Za-z0-9_\\.-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})$", email)
	return b
}

// CheckPhoneCanSendSms
func CheckPhoneCanSendSms(phone string) bool {
	pattern := `^(?:\+?86)?1(?:3\d{3}|5[^4\D]\d{2}|8\d{3}|7(?:[01356789]\d{2}|4(?:0\d|1[0-2]|9\d))|9[189]\d{2}|6[567]\d{2}|4[579]\d{2})\d{6}$`
	if match, _ := regexp.MatchString(pattern, phone); match {
		return true
	}
	return false
}
func CheckPasswd(passwd string) bool {
	//md5，
	b, _ := regexp.MatchString("^[0-9a-zA-Z]+$", passwd)
	return b
}

func CheckEthAddr(addr string) bool {
	b, _ := regexp.MatchString("^0x[0-9a-zA-Z]{40}$", addr)
	return b
}

func CheckBhdAddr(addr string) bool {
	b, _ := regexp.MatchString("^[0-9a-zA-Z]{34}$", addr)
	return b
}

// BURST-N7YQ-5MW5-NCHD-DXKUA
func CheckBrsAddr(addr string) bool {
	b, _ := regexp.MatchString("^BURST-[0-9A-Z]{4}-[0-9A-Z]{4}-[0-9A-Z]{4}-[0-9A-Z]{5}$", addr)
	return b
}
