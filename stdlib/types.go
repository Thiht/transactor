package stdlib

import (
	"context"
	"database/sql"
)

// DB is the common interface between *sql.DB and *sql.Tx.
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

type sqlDB interface {
	DB
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type sqlTx interface {
	DB
	Commit() error
	Rollback() error
}

var (
	_ DB    = &sql.DB{}
	_ DB    = &sql.Tx{}
	_ sqlDB = &sql.DB{}
	_ sqlTx = &sql.Tx{}
)

type (
	transactorKey struct{}
	// DBGetter is used to get the current DB handler from the context.
	// It returns the current transaction if there is one, otherwise it will return the original DB.
	DBGetter func(context.Context) DB
)

func txToContext(ctx context.Context, tx sqlDB) context.Context {
	return context.WithValue(ctx, transactorKey{}, tx)
}

func txFromContext(ctx context.Context) sqlDB {
	if tx, ok := ctx.Value(transactorKey{}).(sqlDB); ok {
		return tx
	}

	return nil
}
