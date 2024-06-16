package stdlib

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"
)

// NestedTransactionsSavepoints is a nested transactions implementation using savepoints.
// It's compatible with PostgreSQL, MySQL, MariaDB, and SQLite.
func NestedTransactionsSavepoints(db sqlDB, tx *sql.Tx) (sqlDB, sqlTx) {
	switch typedDB := db.(type) {
	case *sql.DB:
		return &nestedTransactionSavepoints{Tx: tx}, tx

	case *nestedTransactionSavepoints:
		typedDB.Tx = tx
		return typedDB, typedDB

	default:
		panic("unsupported type")
	}
}

type nestedTransactionSavepoints struct {
	*sql.Tx
	atomic.Int64
}

func (t *nestedTransactionSavepoints) Begin() (*sql.Tx, error) {
	return t.BeginTx(context.Background(), nil)
}

func (t *nestedTransactionSavepoints) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	depth := t.Int64.Add(1)

	if _, err := t.ExecContext(ctx, "SAVEPOINT sp_"+strconv.FormatInt(depth, 10)); err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	return t.Tx, nil
}

func (t *nestedTransactionSavepoints) Commit() error {
	defer t.Int64.Add(-1)

	if _, err := t.Exec("RELEASE SAVEPOINT sp_" + strconv.FormatInt(t.Int64.Load(), 10)); err != nil {
		return fmt.Errorf("failed to release savepoint: %w", err)
	}

	return nil
}

func (t *nestedTransactionSavepoints) Rollback() error {
	defer t.Int64.Add(-1)

	if _, err := t.Exec("ROLLBACK TO SAVEPOINT sp_" + strconv.FormatInt(t.Int64.Load(), 10)); err != nil {
		return fmt.Errorf("failed to rollback to savepoint: %w", err)
	}

	return nil
}
