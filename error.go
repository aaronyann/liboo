package oo

import (
	"errors"
	"fmt"
)

const ESUCC = "ESUCC"
const EBADGATEWAY = "EBADGATEWAY"
const ENOTAUTH = "ENOTAUTH"
const ENOTPERM = "ENOTPERM"
const EPARAM = "EPARAM"
const ESERVER = "ESERVER"
const EFATAL = "EFATAL"
const EEXISTS = "EEXISTS"
const ENEXISTS = "ENEXISTS"
const ETIMEOUT = "ETIMEOUT"
const ENEEDCODE = "ENEEDCODE"
const EPASSWD = "EPASSWD"
const ETIMENOTALLOW = "ETIMENOTALLOW"
const EBALANCE = "EBALANCE"
const ELIMITED = "ELIMITED"
const ENOTALLOW = "ENOTALLOW"
const ENODATA = "ENODATA"
const UNSUPPORTED = "UNSUPPORTED"

func ErrStr(eno string) string {
	switch eno {
	case ESUCC:
		return "Operation is successful"
	case EBADGATEWAY:
		return "Backen server has down"
	case ENOTAUTH:
		return "User not logged in or login has expired"
	case ENOTPERM:
		return "Permission denied"
	case EPARAM:
		return "Wrong parameter"
	case ESERVER:
		return "Internal server error"
	case EFATAL:
		return "Server fatal error"
	case EEXISTS:
		return "Unexpected existence"
	case ENEXISTS:
		return "Unexpected not existence"
	case ETIMEOUT:
		return "Access timeout"
	case ENEEDCODE:
		return "Need to provide a graphic verification code"
	case EPASSWD:
		return "Wrong password"
	case ETIMENOTALLOW:
		return "Operate during periods of time that are not allowed"
	case EBALANCE:
		return "Insufficient balance"
	case ELIMITED:
		return "The upper limit has been reached"
	case ENOTALLOW:
		return "Not allow to do that now"
	case ENODATA:
		return "No data"
	case UNSUPPORTED:
		return "Unsupported invocation"
	default:
		return "Unknown error"
	}
}

func NewError(format string, v ...interface{}) error {
	msg := fmt.Sprintf(format, v...)
	return errors.New(msg)
}

type RpcError struct {
	Eno string
	Err error
}

func (e *RpcError) Error() string {
	return e.Eno + ":" + ErrStr(e.Eno) + "; " + e.Err.Error()
}
func (e *RpcError) Errno() string {
	return e.Eno
}

func NewRpcError(eno string, format ...interface{}) (e *RpcError) {
	var Err error
	if len(format) > 0 {
		ff, _ := format[0].(string)
		Err = NewError(ff, format[1:]...)
	} else {
		Err = errors.New(eno)
	}
	e = &RpcError{Eno: eno, Err: Err}
	return
}
