package sqlx

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// DB is the common interface between *[sqlx.DB] and *[sqlx.Tx].
type DB interface {
	// database/sql methods

	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row

	Exec(query string, args ...any) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row

	// sqlx methods

	GetContext(ctx context.Context, dest any, query string, args ...any) error
	MustExecContext(ctx context.Context, query string, args ...any) sql.Result
	NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error)
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	QueryRowxContext(ctx context.Context, query string, args ...any) *sqlx.Row
	QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error)
	SelectContext(ctx context.Context, dest any, query string, args ...any) error

	Get(dest any, query string, args ...any) error
	MustExec(query string, args ...any) sql.Result
	NamedExec(query string, arg any) (sql.Result, error)
	NamedQuery(query string, arg any) (*sqlx.Rows, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	Preparex(query string) (*sqlx.Stmt, error)
	QueryRowx(query string, args ...any) *sqlx.Row
	Queryx(query string, args ...any) (*sqlx.Rows, error)
	Select(dest any, query string, args ...any) error

	Rebind(query string) string
	BindNamed(query string, arg any) (string, []any, error)
	DriverName() string
}

type sqlxDB interface {
	DB
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
}

type sqlxTx interface {
	Commit() error
	Rollback() error
}

var (
	_ DB     = &sqlx.DB{}
	_ DB     = &sqlx.Tx{}
	_ sqlxDB = &sqlx.DB{}
	_ sqlxTx = &sqlx.Tx{}
)

type (
	transactorKey struct{ _ *struct{} }
	// Deprecated: transactorMarker is used in addition to transactorKey to keep the legacy IsWithinTransaction function.
	transactorMarker struct{}
	// DBGetter is used to get the current DB handler from the context.
	// It returns the current transaction if there is one, otherwise it will return the original DB.
	DBGetter func(context.Context) DB
)

func txToContext(ctx context.Context, key *transactorKey, tx sqlxDB) context.Context {
	return context.WithValue(context.WithValue(ctx, key, tx), transactorMarker{}, struct{}{})
}

func txFromContext(ctx context.Context, key *transactorKey) sqlxDB {
	if tx, ok := ctx.Value(key).(sqlxDB); ok {
		return tx
	}

	return nil
}
