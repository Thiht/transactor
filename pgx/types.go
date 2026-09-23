package pgx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is the common interface between *[pgx.Conn], *[pgx.Tx], *[pgxpool.Conn], *[pgxpool.Pool] and *[pgxpool.Tx].
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row

	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

type pgxDB interface {
	DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

var (
	_ DB    = &pgx.Conn{}
	_ DB    = pgx.Tx(nil)
	_ DB    = &pgxpool.Conn{}
	_ DB    = &pgxpool.Pool{}
	_ DB    = &pgxpool.Tx{}
	_ pgxDB = &pgx.Conn{}
	_ pgxDB = pgx.Tx(nil)
	_ pgxDB = &pgxpool.Conn{}
	_ pgxDB = &pgxpool.Pool{}
	_ pgxDB = &pgxpool.Tx{}
)

type (
	transactorKey struct{ _ *struct{} }
	// Deprecated: transactorMarker is used in addition to transactorKey to keep the legacy IsWithinTransaction function.
	transactorMarker struct{}
	// DBGetter is used to get the current DB handler from the context.
	// It returns the current transaction if there is one, otherwise it will return the original DB.
	DBGetter func(context.Context) DB
)

func txToContext(ctx context.Context, key *transactorKey, tx pgx.Tx) context.Context {
	return context.WithValue(context.WithValue(ctx, key, tx), transactorMarker{}, struct{}{})
}

func txFromContext(ctx context.Context, key *transactorKey) pgx.Tx {
	if tx, ok := ctx.Value(key).(pgx.Tx); ok {
		return tx
	}

	return nil
}
