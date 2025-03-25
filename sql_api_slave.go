package oo

var SlaveMysql *MysqlPool

func SlaveSqlSelect(sqlstr string, dest interface{}) (err error) {
	conn := SlaveMysql.GetConn()
	defer SlaveMysql.UnGetConn(conn)

	err = conn.Select(dest, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
	}

	return
}

func SlaveSqlGet(sqlstr string, dest interface{}) (err error) {
	conn := SlaveMysql.GetConn()
	defer SlaveMysql.UnGetConn(conn)

	err = conn.Get(dest, sqlstr)
	if nil != err && ErrNoRows != err {
		err = NewError("%v sql[%s]", err, sqlstr)
	}

	return
}

func SlaveSqlSelectCount(sqler *Sqler, offset, count int64, dest interface{}, total *int64) (err error) {
	conn := SlaveMysql.GetConn()
	defer SlaveMysql.UnGetConn(conn)

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
