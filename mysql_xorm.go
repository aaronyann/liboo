package oo

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"xorm.io/core"
	"xorm.io/xorm"
)

var (
	engine *xorm.Engine
)

func InitXorm(host string, port int64, user, pass, db_name string) (err error) {
	var (
		data_source = fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
			user, pass, host, port, db_name)
	)
	engine, err = xorm.NewEngine("mysql", data_source)
	if err != nil {
		err = fmt.Errorf("xorm.NewEngine %v", err)
		return
	}
	engine.SetMapper(core.SnakeMapper{})

	// engine.Exec("USE db_btc_co;")

	// err = engine.Sync2(init_tables...)
	// if nil != err {
	// 	err = fmt.Errorf("engine.Sync2 %v", err)
	// 	return
	// }

	return
}

func XormEngine() *xorm.Engine {
	return engine
}

func NewXormSession() *xorm.Session {
	return engine.NewSession()
}

func NewXormTx() (session *xorm.Session, err error) {
	session = NewXormSession()

	err = session.Begin()

	return
}

func EndXormTx(session *xorm.Session, perr *error) {
	var err error
	if nil != *perr {
		err = session.Rollback()
		if nil != err {
			*perr = fmt.Errorf("Rollback err %v *perr %v", err, *perr)
		}
	} else {
		err = session.Commit()
		if nil != err {
			*perr = fmt.Errorf("sqltx.Commit err %v", err)
		}
	}

	return
}
