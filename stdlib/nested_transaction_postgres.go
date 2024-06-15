package stdlib

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"
)

func NestedTransactionPostgresSavepoints(db sqlDB, tx *sql.Tx) (sqlDB, sqlTx) {
	switch db.(type) {
	case *sql.DB:
		return &nestedTransactionPostgres{Tx: tx}, tx

	case *nestedTransactionPostgres:
		nestedTransaction := db.(*nestedTransactionPostgres)
		nestedTransaction.Tx = tx
		return nestedTransaction, nestedTransaction

	default:
		panic("unsupported type")
	}
}

type nestedTransactionPostgres struct {
	*sql.Tx
	atomic.Int64
}

var (
	_ sqlDB = &nestedTransactionPostgres{}
	_ sqlTx = &nestedTransactionPostgres{}
)

func (t *nestedTransactionPostgres) Begin() (*sql.Tx, error) {
	return t.BeginTx(context.Background(), nil)
}

func (t *nestedTransactionPostgres) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	t.Int64.Add(1)

	if _, err := t.ExecContext(ctx, "SAVEPOINT sp_"+strconv.FormatInt(t.Int64.Load(), 10)); err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	return t.Tx, nil
}

func (t *nestedTransactionPostgres) Commit() error {
	defer t.Int64.Add(-1)

	if _, err := t.Exec("RELEASE SAVEPOINT sp_" + strconv.FormatInt(t.Int64.Load(), 10)); err != nil {
		return fmt.Errorf("failed to release savepoint: %w", err)
	}

	return nil
}

func (t *nestedTransactionPostgres) Rollback() error {
	defer t.Int64.Add(-1)

	if _, err := t.Exec("ROLLBACK TO SAVEPOINT sp_" + strconv.FormatInt(t.Int64.Load(), 10)); err != nil {
		return fmt.Errorf("failed to rollback to savepoint: %w", err)
	}

	return nil
}
