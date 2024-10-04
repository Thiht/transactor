package pgx

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewTransactor(db *pgx.Conn) (*pgxTransactor, DBGetter) { //nolint:revive // *pgxTransactor implements Transactor, so it's ok to return a private struct.
	pgxTxGetter := func(ctx context.Context) pgxDB {
		if tx := txFromContext(ctx); tx != nil {
			return tx
		}

		return db
	}

	dbGetter := func(ctx context.Context) DB {
		if tx := txFromContext(ctx); tx != nil {
			return tx
		}

		return db
	}

	return &pgxTransactor{
		pgxTxGetter,
	}, dbGetter
}

func NewTransactorFromPool(pool *pgxpool.Pool) (*pgxTransactor, DBGetter) { //nolint:revive // *pgxTransactor implements Transactor, so it's ok to return a private struct.
	pgxTxGetter := func(ctx context.Context) pgxDB {
		if tx := txFromContext(ctx); tx != nil {
			return tx
		}

		return pool
	}

	dbGetter := func(ctx context.Context) DB {
		if tx := txFromContext(ctx); tx != nil {
			return tx
		}

		return pool
	}

	return &pgxTransactor{
		pgxTxGetter,
	}, dbGetter
}

type (
	pgxTxGetter func(context.Context) pgxDB
)

type pgxTransactor struct {
	pgxTxGetter
}

func (t *pgxTransactor) WithinTransaction(ctx context.Context, txFunc func(context.Context) error) error {
	db := t.pgxTxGetter(ctx)

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx) // If rollback fails, there's nothing to do, the transaction will expire by itself
	}()

	txCtx := txToContext(ctx, tx)

	if err := txFunc(txCtx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func IsWithinTransaction(ctx context.Context) bool {
	return ctx.Value(transactorKey{}) != nil
}
