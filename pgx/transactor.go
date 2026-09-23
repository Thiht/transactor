package pgx

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewTransactor(db pgxDB) (*Transactor, DBGetter) {
	txKey := &transactorKey{}

	pgxTxGetter := func(ctx context.Context) pgxDB {
		if tx := txFromContext(ctx, txKey); tx != nil {
			return tx
		}

		return db
	}

	dbGetter := func(ctx context.Context) DB {
		if tx := txFromContext(ctx, txKey); tx != nil {
			return tx
		}

		return db
	}

	return &Transactor{
		pgxTxGetter,
		txKey,
	}, dbGetter
}

// Deprecated: use [NewTransactor] instead.
func NewTransactorFromPool(pool *pgxpool.Pool) (*Transactor, DBGetter) {
	return NewTransactor(pool)
}

type (
	pgxTxGetter func(context.Context) pgxDB
)

type Transactor struct {
	pgxTxGetter
	txKey *transactorKey
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

	txCtx := txToContext(ctx, t.txKey, tx)

	if err := txFunc(txCtx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (t *Transactor) IsWithinTransaction(ctx context.Context) bool {
	return ctx.Value(t.txKey) != nil
}

// Deprecated: use [Transactor.IsWithinTransaction] instead.
// This function can give the wrong result if multiple transactor instances are used.
func IsWithinTransaction(ctx context.Context) bool {
	return ctx.Value(transactorMarker{}) != nil
}
