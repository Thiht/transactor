package sqlx

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

func NewTransactor(db *sqlx.DB, nestedTransactionStrategy nestedTransactionsStrategy) (*sqlxTransactor, DBGetter) { //nolint:revive // *sqlxTransactor implements Transactor, so it's ok to return a private struct.
	sqlDBGetter := func(ctx context.Context) sqlxDB {
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

	return &sqlxTransactor{
		sqlDBGetter,
		nestedTransactionStrategy,
	}, dbGetter
}

type (
	sqlxDBGetter               func(context.Context) sqlxDB
	nestedTransactionsStrategy func(sqlxDB, *sqlx.Tx) (sqlxDB, sqlxTx)
)

type sqlxTransactor struct {
	sqlxDBGetter
	nestedTransactionsStrategy
}

func (t *sqlxTransactor) WithinTransaction(ctx context.Context, txFunc func(context.Context) error) error {
	currentDB := t.sqlxDBGetter(ctx)

	tx, err := currentDB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	newDB, currentTX := t.nestedTransactionsStrategy(currentDB, tx)
	defer func() {
		_ = currentTX.Rollback() // If rollback fails, there's nothing to do, the transaction will expire by itself
	}()
	txCtx := txToContext(ctx, newDB)

	if err := txFunc(txCtx); err != nil {
		return err
	}

	if err := currentTX.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func IsWithinTransaction(ctx context.Context) bool {
	return ctx.Value(transactorKey{}) != nil
}
