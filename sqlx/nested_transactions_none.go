package sqlx

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
)

// NestedTransactionsNone is an implementation that prevents using nested transactions.
func NestedTransactionsNone(db sqlxDB, tx *sqlx.Tx) (sqlxDB, sqlxTx) {
	switch typedDB := db.(type) {
	case *sqlx.DB:
		// wrap the current transaction so callers get a DB-like object that
		// forwards methods to the active *sqlx.Tx, but rejects BeginTxx/Commit/Rollback
		// for nested transaction operations.
		return &nestedTransactionNone{Tx: tx}, tx

	case *nestedTransactionNone:
		return typedDB, typedDB

	default:
		panic("unsupported type")
	}
}

type nestedTransactionNone struct {
	*sqlx.Tx
}

func (t *nestedTransactionNone) BeginTxx(_ context.Context, _ *sql.TxOptions) (*sqlx.Tx, error) {
	return nil, errors.New("nested transactions are not supported")
}

func (t *nestedTransactionNone) Commit() error {
	return errors.New("nested transactions are not supported")
}

func (t *nestedTransactionNone) Rollback() error {
	return errors.New("nested transactions are not supported")
}
