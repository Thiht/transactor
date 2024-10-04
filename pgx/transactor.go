package pgx

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewTransactor(db *pgx.Conn) (*Transactor, DBGetter) {
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

	return &Transactor{
		pgxTxGetter,
	}, dbGetter
}

func NewTransactorFromPool(pool *pgxpool.Pool) (*Transactor, DBGetter) {
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

	return &Transactor{
		pgxTxGetter,
	}, dbGetter
}

type (
	pgxTxGetter func(context.Context) pgxDB
)

type Transactor struct {
	pgxTxGetter
}

func (t *Transactor) WithinTransaction(ctx context.Context, txFunc func(context.Context) error) error {
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
