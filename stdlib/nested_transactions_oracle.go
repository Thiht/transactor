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
		nestedTransaction := &nestedTransactionOracle{
			Tx:    tx,
			depth: typedDB.depth + 1,
		}
		return nestedTransaction, nestedTransaction

	default:
		panic("unsupported type")
	}
}

type nestedTransactionOracle struct {
	*sql.Tx
	depth int64
	done  atomic.Bool
}

func (t *nestedTransactionOracle) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	if _, err := t.ExecContext(ctx, "SAVEPOINT sp_"+strconv.FormatInt(t.depth+1, 10)); err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	return t.Tx, nil
}

func (t *nestedTransactionOracle) Commit() error {
	if !t.done.CompareAndSwap(false, true) {
		return sql.ErrTxDone
	}

	return nil
}

func (t *nestedTransactionOracle) Rollback() error {
	if !t.done.CompareAndSwap(false, true) {
		return sql.ErrTxDone
	}

	if _, err := t.Exec("ROLLBACK TO SAVEPOINT sp_" + strconv.FormatInt(t.depth, 10)); err != nil {
		return fmt.Errorf("failed to rollback to savepoint: %w", err)
	}

	return nil
}
