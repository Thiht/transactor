package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/jmoiron/sqlx"
)

// NestedTransactionsSavepoints is a nested transactions implementation using savepoints.
// It's compatible with PostgreSQL, MySQL, MariaDB, and SQLite.
func NestedTransactionsSavepoints(db sqlxDB, tx *sqlx.Tx) (sqlxDB, sqlxTx) {
	switch typedDB := db.(type) {
	case *sqlx.DB:
		return &nestedTransactionSavepoints{Tx: tx}, tx

	case *nestedTransactionSavepoints:
		nestedTransaction := &nestedTransactionSavepoints{
			Tx:    tx,
			depth: typedDB.depth + 1,
		}
		return nestedTransaction, nestedTransaction

	default:
		panic("unsupported type")
	}
}

type nestedTransactionSavepoints struct {
	*sqlx.Tx
	depth int64
	done  atomic.Bool
}

func (t *nestedTransactionSavepoints) BeginTxx(ctx context.Context, _ *sql.TxOptions) (*sqlx.Tx, error) {
	if _, err := t.ExecContext(ctx, "SAVEPOINT sp_"+strconv.FormatInt(t.depth+1, 10)); err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	return t.Tx, nil
}

func (t *nestedTransactionSavepoints) Commit() error {
	if !t.done.CompareAndSwap(false, true) {
		return sql.ErrTxDone
	}

	if _, err := t.Exec("RELEASE SAVEPOINT sp_" + strconv.FormatInt(t.depth, 10)); err != nil {
		return fmt.Errorf("failed to release savepoint: %w", err)
	}

	return nil
}

func (t *nestedTransactionSavepoints) Rollback() error {
	if !t.done.CompareAndSwap(false, true) {
		return sql.ErrTxDone
	}

	if _, err := t.Exec("ROLLBACK TO SAVEPOINT sp_" + strconv.FormatInt(t.depth, 10)); err != nil {
		return fmt.Errorf("failed to rollback to savepoint: %w", err)
	}

	return nil
}
