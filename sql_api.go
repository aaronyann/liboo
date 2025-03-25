package oo

import (
	"fmt"
	"strings"
)

func SqlSelectCount(sqler *Sqler, offset, count int64, dest interface{}, total *int64) (err error) {
	conn := GMysqlPool.GetConn()
	defer GMysqlPool.UnGetConn(conn)

	sqler2 := *sqler

	sqlstr := sqler.Offset(int(offset)).Limit(int(count)).Select("*")
	err = conn.Select(dest, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}
	sqlstr = sqler2.Select("COUNT(*) AS total")
	err = conn.Get(total, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}

	return
}

func SqlSelectCount2(sqler *Sqler, sel string, offset, count int64, dest interface{}, total *int64) (err error) {
	conn := GMysqlPool.GetConn()
	defer GMysqlPool.UnGetConn(conn)

	sqler2 := *sqler

	sqlstr := sqler.Offset(int(offset)).Limit(int(count)).Select(sel)
	err = conn.Select(dest, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}
	sqlstr = sqler2.Select("COUNT(*) AS total")
	err = conn.Get(total, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}

	return
}

func SqlSelectCount20(sqler *Sqler, sel, sel_cnt string, offset, count int64, dest interface{}, total *int64) (err error) {
	conn := GMysqlPool.GetConn()
	defer GMysqlPool.UnGetConn(conn)

	sqler2 := *sqler

	sqlstr := sqler.Offset(int(offset)).Limit(int(count)).Select(sel)
	err = conn.Select(dest, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}
	sqlstr = sqler2.Select(sel_cnt)
	err = conn.Get(total, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}

	return
}

func SqlSelectCount3(sqlfmt, sel string, offset, count int64, dest interface{}, total *int64) (err error) {
	conn := GMysqlPool.GetConn()
	defer GMysqlPool.UnGetConn(conn)

	sqlstr := fmt.Sprintf(sqlfmt+" LIMIT %d OFFSET %d", sel, count, offset)
	err = conn.Select(dest, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}

	sqlstr = fmt.Sprintf(sqlfmt, "COUNT(*) AS total")
	err = conn.Get(total, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
		return
	}

	return
}

func SqlSelect(sqlstr string, dest interface{}) (err error) {
	conn := GMysqlPool.GetConn()
	defer GMysqlPool.UnGetConn(conn)

	err = conn.Select(dest, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
	}

	return
}

func SqlGet(sqlstr string, dest interface{}) (err error) {
	conn := GMysqlPool.GetConn()
	defer GMysqlPool.UnGetConn(conn)

	err = conn.Get(dest, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
	}

	return
}

func SqlGetInt64(sqlstr string) (ret int64, err error) {
	err = SqlGet(sqlstr, &ret)
	return
}

func SqlExec(sqlstr string) (err error) {
	conn := GMysqlPool.GetConn()
	defer GMysqlPool.UnGetConn(conn)

	_, err = conn.Exec(sqlstr)
	if nil != err {
		err = NewError("%v sql[%s]", err, sqlstr)
	}

	return
}

func SqlValuesInsertBatch(format string, sqlarr []string, size int) (err error) {
	if size < 100 {
		size = 100
	}
	pos, cnt := 0, len(sqlarr)

	for {
		var arr []string
		if pos+size >= cnt {
			arr = sqlarr[pos:cnt]
		} else {
			arr = sqlarr[pos : pos+size]
		}
		pos = pos + size

		sqlstr := fmt.Sprintf(format, strings.Join(arr, ","))

		err = SqlExec(sqlstr)
		if nil != err {
			err = NewError("SqlExec %v", err)
			return
		}

		if pos >= cnt {
			break
		}
	}

	return
}
