package stdlib

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Thiht/transactor"
)

type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row

	Exec(query string, args ...any) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row

	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type DBGetter func(context.Context) DB

func NewTransactor(db *sql.DB) (transactor.Transactor, DBGetter) {
	dbGetter := func(ctx context.Context) DB {
		if db, ok := ctx.Value(transactorKey{}).(DB); ok {
			return db
		}

		return db
	}

	return &stdlibTransactor{dbGetter}, dbGetter
}

type transactorKey struct{}

type txDB struct {
	*sql.Tx
}

var _ DB = &txDB{}

func (t *txDB) Begin() (*sql.Tx, error) {
	return nil, fmt.Errorf("nested transactions are not supported")
}

func (t *txDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, fmt.Errorf("nested transactions are not supported")
}

type stdlibTransactor struct {
	DBGetter
}

func (t *stdlibTransactor) WithinTransaction(ctx context.Context, txFunc func(context.Context) error) error {
	tx, err := t.DBGetter(ctx).BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	txCtx := context.WithValue(ctx, transactorKey{}, &txDB{tx})
	if err := txFunc(txCtx); err != nil {
		_ = tx.Rollback() // If rollback fails, there's nothing to do, the transaction will expire by itself
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
