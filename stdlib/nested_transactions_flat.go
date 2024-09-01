package stdlib

import (
	"context"
	"database/sql"
)

// NestedTransactionsFlattened is a nested transactions implementation without savepoint.
// This implementation keeps the original transaction in order to simplify the commit system,
// this type must not be used when you want to keep intermediate sql changes.
// It's compatible with PostgreSQL, MySQL, MariaDB, and SQLite.
func NestedTransactionsFlattened(db sqlDB, tx *sql.Tx) (sqlDB, sqlTx) {
	switch typedDB := db.(type) {
	case *sql.DB:
		return &nestedTransactionflattened{Tx: tx}, tx

	case *nestedTransactionflattened:
		return typedDB, typedDB

	default:
		panic("unsupported type")
	}
}

type nestedTransactionflattened struct {
	*sql.Tx
}

func (t *nestedTransactionflattened) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	return t.Tx, nil
}

func (t *nestedTransactionflattened) Commit() error {
	return nil
}

func (t *nestedTransactionflattened) Rollback() error {
	return nil
}
