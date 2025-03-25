package oo

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

//Go

type MysqlPool struct {
	db  *sql.DB
	dbx *sqlx.DB
}

var GMysqlPool *MysqlPool
var ErrNoRows error = sql.ErrNoRows

type MysqlCfg struct {
	Host string `toml:"host,omitzero"`
	Port int64  `toml:"port,omitzero"`
	User string `toml:"user,omitzero"`
	Pass string `toml:"pass,omitzero"`
}

func IsSqlErr(err error) bool {
	return err != nil && err != sql.ErrNoRows
}

func InitMysqlPool(Host string, Port int32, UserName string, Password string, Database string) (p *MysqlPool, e error) {
	cfg := MysqlCfg{
		Host: Host,
		Port: int64(Port),
		User: UserName,
		Pass: Password,
	}
	return InitMysqlPool2(&cfg, Database)
}
func InitMysqlPool2(cfg *MysqlCfg, Database string) (p *MysqlPool, e error) {
	mysqlCfg := mysql.Config{Net: "tcp", Addr: "127.0.0.1:3306", DBName: "dbname", Collation: "utf8mb4_general_ci", Loc: time.UTC}
	Port := cfg.Port
	if Port == 0 {
		Port = 3306
	}
	mysqlCfg.Addr = fmt.Sprintf("%s:%d", cfg.Host, Port)
	mysqlCfg.User = cfg.User
	mysqlCfg.Passwd = cfg.Pass
	mysqlCfg.DBName = Database
	mysqlCfg.Timeout = 8 * time.Second
	mysqlCfg.AllowNativePasswords = true

	db, err := sql.Open("mysql", mysqlCfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	//G_MysqlDB.SetMaxOpenConns(2000)
	//G_MysqlDB.SetMaxIdleConns(1000)
	db.Ping()
	// GMysqlDB = db
	// GMysqlDB.Ping()

	dbx := sqlx.NewDb(db, "mysql")
	dbx.MapperFunc(LowerCaseWithUnderscores)

	pool := &MysqlPool{db, dbx}
	if GMysqlPool == nil {
		GMysqlPool = pool
	}
	return pool, nil
}

// func (p *MysqlPool) GetConn() *sql.DB {
// 	return p.db
// }

func (p *MysqlPool) GetConn() *sqlx.DB {
	return p.dbx
}

func (p *MysqlPool) UnGetConn(db interface{}) {

}

func (p *MysqlPool) BeginTx() (tx *sqlx.Tx, err error) {
	tx, err = p.dbx.Beginx()
	return
}
func (p *MysqlPool) EndTx(tx *sqlx.Tx, perr *error) error {
	if perr == nil || *perr == nil {
		return tx.Commit()
	} else {
		return tx.Rollback()
	}
}
func (p *MysqlPool) TxExec(tx *sqlx.Tx, sqlstr string, perr *error) (affected uint64, lastid uint64, err error) {
	if perr == nil || *perr == nil {
		res, err := tx.Exec(sqlstr)
		if err != nil {
			return 0, 0, err
		}
		return ExpandSqlResult(res)
	}
	return 0, 0, *perr
}
func (p *MysqlPool) BatchExec(sqls []string) (err error) {
	tx, err := p.dbx.Beginx()
	if err != nil {
		return
	}

	for _, s := range sqls {
		if _, err = tx.Exec(s); err != nil {
			tx.Rollback()
			return
		}
	}
	tx.Commit()
	return
}

func (p *MysqlPool) Query(sqlstr string) ([]map[string]sql.RawBytes, error) {
	db := p.GetConn()
	//defer db.Close()

	//1、query
	rows, err := db.Query(sqlstr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	//2、prepare data space
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	//3、processing data
	totalRecord := make([]map[string]sql.RawBytes, 0)
	for rows.Next() {
		values := make([]sql.RawBytes, len(columns))
		scanArgus := make([]interface{}, len(values))
		for i := range values {
			scanArgus[i] = &values[i]
		}
		err = rows.Scan(scanArgus...)
		if err != nil {
			break
		}
		rowRecord := make(map[string]sql.RawBytes)
		for i, col := range values {
			newcol := sql.RawBytes(make([]byte, len([]byte(col))))
			copy([]byte(newcol), []byte(col))
			rowRecord[columns[i]] = newcol
		}
		totalRecord = append(totalRecord, rowRecord)
	}
	return totalRecord, nil
}

func ExpandSqlResult(res sql.Result) (affected uint64, lastid uint64, err error) {
	var af, lid int64
	if af, err = res.RowsAffected(); err != nil || af == 0 {
		return
	}
	if lid, err = res.LastInsertId(); err != nil {
		return
	}
	affected = uint64(af)
	lastid = uint64(lid)

	return
}
func (p *MysqlPool) SingleExec(sqlstr string) (affected uint64, lastid uint64, err error) {
	res, err := p.dbx.Exec(sqlstr)
	if err != nil {
		return
	}
	return ExpandSqlResult(res)
}

func (p *MysqlPool) SingleSelect(p_struct_rows interface{}, sqlstr string) error {
	return p.dbx.Select(p_struct_rows, sqlstr)
}

func (p *MysqlPool) SingleGetRow(p_struct_row interface{}, sqlstr string) error {
	return p.dbx.Get(p_struct_row, sqlstr)
}

func (p *MysqlPool) SingleGetVal(p_val interface{}, sqlstr string) error {
	return p.dbx.Get(p_val, sqlstr)
}

// for get sum/count/id etc.
func (p *MysqlPool) SingleGetInt64(sqlstr string) (v int64, err error) {
	err = p.dbx.Get(&v, sqlstr)
	return
}
