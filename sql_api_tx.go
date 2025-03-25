package oo

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

func NewSqlTxConn() (conn *sqlx.DB, tx *sql.Tx, err error) {
	conn = GMysqlPool.GetConn()

	tx, err = conn.Begin()

	return
}

func CloseSqlTxConn(conn *sqlx.DB, tx *sql.Tx, err *error) {
	GMysqlPool.UnGetConn(conn)

	SqlTxProc(tx, err)

	return
}

func SqlTxProc(sqltx *sql.Tx, perr *error) {
	var err error
	if nil != *perr {
		err = sqltx.Rollback()
		if nil != err {
			*perr = NewError("sqltx.Rollback err %v *perr %v", err, *perr)
		}
	} else {
		err = sqltx.Commit()
		if nil != err {
			*perr = NewError("sqltx.Commit err %v", err)
		}
	}

	return
}

func SqlTxExec(sqltx *sql.Tx, sqlstr string) (err error) {
	_, err = sqltx.Exec(sqlstr)
	if nil != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}
	return
}

func SqlTxExecf(sqltx *sql.Tx, format string, a ...interface{}) (err error) {
	sqlstr := fmt.Sprintf(format, a...)
	_, err = sqltx.Exec(sqlstr)
	if nil != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}
	return
}
