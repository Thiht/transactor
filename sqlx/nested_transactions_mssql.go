package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/jmoiron/sqlx"
)

// NestedTransactionsMSSQL is a nested transactions implementation using Microsoft SQL Server savepoints.
func NestedTransactionsMSSQL(db sqlxDB, tx *sqlx.Tx) (sqlxDB, sqlxTx) {
	switch typedDB := db.(type) {
	case *sqlx.DB:
		return &nestedTransactionMSSQL{Tx: tx}, tx

	case *nestedTransactionMSSQL:
		nestedTransaction := &nestedTransactionMSSQL{
			Tx:    tx,
			depth: typedDB.depth + 1,
		}
		return nestedTransaction, nestedTransaction

	default:
		panic("unsupported type")
	}
}

type nestedTransactionMSSQL struct {
	*sqlx.Tx
	depth int64
	done  atomic.Bool
}

func (t *nestedTransactionMSSQL) BeginTxx(ctx context.Context, _ *sql.TxOptions) (*sqlx.Tx, error) {
	if _, err := t.ExecContext(ctx, "SAVE TRANSACTION sp_"+strconv.FormatInt(t.depth+1, 10)); err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	return t.Tx, nil
}

func (t *nestedTransactionMSSQL) Commit() error {
	if !t.done.CompareAndSwap(false, true) {
		return sql.ErrTxDone
	}

	return nil
}

func (t *nestedTransactionMSSQL) Rollback() error {
	if !t.done.CompareAndSwap(false, true) {
		return sql.ErrTxDone
	}

	if _, err := t.Exec("ROLLBACK TRANSACTION sp_" + strconv.FormatInt(t.depth, 10)); err != nil {
		return fmt.Errorf("failed to rollback to savepoint: %w", err)
	}

	return nil
}
