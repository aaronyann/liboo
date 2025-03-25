package oo

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

func NewSqlxTx() (tx *sqlx.Tx, err error) {
	return GMysqlPool.dbx.Beginx()
}

// CloseSqlxTx closes transaction
//
// Safe to close it multiple times.
func CloseSqlxTx(tx *sqlx.Tx, inoutErr *error) {
	if *inoutErr != nil {
		err := tx.Rollback()
		if err != nil {
			*inoutErr = NewError("sqltx.Rollback err %v *perr %v", err, *inoutErr)
		}
		return
	}

	if err := tx.Commit(); err != nil {
		*inoutErr = NewError("sqltx.Commit err %v", err)
	}
}

func SqlxTxGet(tx *sqlx.Tx, dest interface{}, query string, args ...interface{}) (exists bool, err error) {
	err = tx.Get(dest, query, args...)
	switch err {
	case nil:
		return true, nil
	case sql.ErrNoRows:
		return false, nil
	default:
		return
	}
}

func SqlxTxQuery(tx *sqlx.Tx, query string, args ...interface{}) (*sqlx.Rows, error) {
	return tx.Queryx(query, args...)
}

func SqlxTxQueryContext(ctx context.Context, tx *sqlx.Tx, query string, args ...interface{}) (*sqlx.Rows, error) {
	return tx.QueryxContext(ctx, query, args...)
}

func SqlxTxPreparex(tx *sqlx.Tx, query string) (*sqlx.Stmt, error) {
	return tx.Preparex(query)
}

func SqlxTxPreparexContext(ctx context.Context, tx *sqlx.Tx, query string) (*sqlx.Stmt, error) {
	return tx.PreparexContext(ctx, query)
}

func SqlxTxExec(tx *sqlx.Tx, query string, args ...interface{}) (sql.Result, error) {
	return tx.Exec(query, args...)
}

func SqlxTxExecContext(ctx context.Context, tx *sqlx.Tx, query string, args ...interface{}) (sql.Result, error) {
	return tx.ExecContext(ctx, query, args...)
}
