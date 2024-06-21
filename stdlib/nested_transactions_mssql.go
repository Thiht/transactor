package stdlib

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"
)

// NestedTransactionsMSSQL is a nested transactions implementation using Microsoft SQL Server savepoints.
func NestedTransactionsMSSQL(db sqlDB, tx *sql.Tx) (sqlDB, sqlTx) {
	switch typedDB := db.(type) {
	case *sql.DB:
		return &nestedTransactionMSSQL{Tx: tx}, tx

	case *nestedTransactionMSSQL:
		typedDB.Tx = tx
		return typedDB, typedDB

	default:
		panic("unsupported type")
	}
}

type nestedTransactionMSSQL struct {
	*sql.Tx
	atomic.Int64
}

func (t *nestedTransactionMSSQL) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	depth := t.Int64.Add(1)

	if _, err := t.ExecContext(ctx, "SAVE TRANSACTION sp_"+strconv.FormatInt(depth, 10)); err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	return t.Tx, nil
}

func (t *nestedTransactionMSSQL) Commit() error {
	t.Int64.Add(-1)
	return nil
}

func (t *nestedTransactionMSSQL) Rollback() error {
	defer t.Int64.Add(-1)

	if _, err := t.Exec("ROLLBACK TRANSACTION sp_" + strconv.FormatInt(t.Int64.Load(), 10)); err != nil {
		return fmt.Errorf("failed to rollback to savepoint: %w", err)
	}

	return nil
}
