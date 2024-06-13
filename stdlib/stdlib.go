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
}

type transactorDB interface {
	DB
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type transactorTX interface {
	DB
	Commit() error
	Rollback() error
}

type transactorKey struct{}
type DBGetter func(context.Context) DB
type transactorDBGetter func(context.Context) transactorDB

func NewTransactor(db *sql.DB) (transactor.Transactor, DBGetter) {
	transactorDBGetter := func(ctx context.Context) transactorDB {
		if tx, ok := ctx.Value(transactorKey{}).(transactorDB); ok {
			return tx
		}

		return db
	}

	dbGetter := func(ctx context.Context) DB {
		if tx, ok := ctx.Value(transactorKey{}).(DB); ok {
			return tx
		}

		return db
	}

	return &stdlibTransactor{transactorDBGetter}, dbGetter
}

type txDB struct {
	*sql.Tx
}

var _ transactorDB = &txDB{}
var _ transactorTX = &txDB{}

func (t *txDB) Begin() (*sql.Tx, error) {
	return t.BeginTx(context.Background(), nil)
}

func (t *txDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if _, err := t.ExecContext(ctx, "SAVEPOINT sp"); err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	return t.Tx, nil
}

func (t *txDB) Commit() error {
	if _, err := t.Exec("RELEASE SAVEPOINT sp"); err != nil {
		return fmt.Errorf("failed to release savepoint: %w", err)
	}

	return nil
}

func (t *txDB) Rollback() error {
	if _, err := t.Exec("ROLLBACK TO SAVEPOINT sp"); err != nil {
		return fmt.Errorf("failed to rollback to savepoint: %w", err)
	}

	return nil
}

type stdlibTransactor struct {
	transactorDBGetter
}

func (t *stdlibTransactor) WithinTransaction(ctx context.Context, txFunc func(context.Context) error) error {
	currentTxDB := t.transactorDBGetter(ctx)
	tx, err := currentTxDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	currentTX, ok := currentTxDB.(transactorTX)
	if !ok {
		currentTX = tx
	}

	txCtx := context.WithValue(ctx, transactorKey{}, &txDB{tx})
	if err := txFunc(txCtx); err != nil {
		_ = currentTX.Rollback() // If rollback fails, there's nothing to do, the transaction will expire by itself
		return err
	}

	if err := currentTX.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
