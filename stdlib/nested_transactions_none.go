package stdlib

import (
	"context"
	"database/sql"
	"errors"
)

// NestedTransactionsNone is an implementation that prevents using nested transactions.
func NestedTransactionsNone(db sqlDB, tx *sql.Tx) (sqlDB, sqlTx) {
	switch typedDB := db.(type) {
	case *sql.DB:
		return &nestedTransactionNone{tx}, tx

	case *nestedTransactionNone:
		return typedDB, typedDB

	default:
		panic("unsupported type")
	}
}

type nestedTransactionNone struct {
	*sql.Tx
}

func (t *nestedTransactionNone) BeginTx(_ context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	return nil, errors.New("nested transactions are not supported")
}

func (t *nestedTransactionNone) Commit() error {
	return errors.New("nested transactions are not supported")
}

func (t *nestedTransactionNone) Rollback() error {
	return errors.New("nested transactions are not supported")
}
