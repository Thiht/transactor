package stdlib

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"
)

// NestedTransactionsOracle is a nested transactions implementation using Oracle savepoints.
func NestedTransactionsOracle(db sqlDB, tx *sql.Tx) (sqlDB, sqlTx) {
	switch typedDB := db.(type) {
	case *sql.DB:
		return &nestedTransactionOracle{Tx: tx}, tx

	case *nestedTransactionOracle:
		typedDB.Tx = tx
		return typedDB, typedDB

	default:
		panic("unsupported type")
	}
}

type nestedTransactionOracle struct {
	*sql.Tx
	atomic.Int64
}

func (t *nestedTransactionOracle) Begin() (*sql.Tx, error) {
	return t.BeginTx(context.Background(), nil)
}

func (t *nestedTransactionOracle) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	depth := t.Int64.Add(1)

	if _, err := t.ExecContext(ctx, "SAVEPOINT sp_"+strconv.FormatInt(depth, 10)); err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	return t.Tx, nil
}

func (t *nestedTransactionOracle) Commit() error {
	t.Int64.Add(-1)
	return nil
}

func (t *nestedTransactionOracle) Rollback() error {
	defer t.Int64.Add(-1)

	if _, err := t.Exec("ROLLBACK TO SAVEPOINT sp_" + strconv.FormatInt(t.Int64.Load(), 10)); err != nil {
		return fmt.Errorf("failed to rollback to savepoint: %w", err)
	}

	return nil
}
