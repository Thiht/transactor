package stdlib

import (
	"context"
	"database/sql"
	"fmt"
)

func NewTransactor(db *sql.DB, nestedTransactionStrategy nestedTransactionsStrategy) (*Transactor, DBGetter) {
	txKey := &transactorKey{}

	sqlDBGetter := func(ctx context.Context) sqlDB {
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
		sqlDBGetter,
		nestedTransactionStrategy,
		txKey,
	}, dbGetter
}

type (
	sqlDBGetter                func(context.Context) sqlDB
	nestedTransactionsStrategy func(sqlDB, *sql.Tx) (sqlDB, sqlTx)
)

type Transactor struct {
	sqlDBGetter
	nestedTransactionsStrategy
	txKey *transactorKey
}

func (t *Transactor) WithinTransaction(ctx context.Context, txFunc func(context.Context) error) error {
	currentDB := t.sqlDBGetter(ctx)

	tx, err := currentDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	newDB, currentTX := t.nestedTransactionsStrategy(currentDB, tx)
	defer func() {
		_ = currentTX.Rollback() // If rollback fails, there's nothing to do, the transaction will expire by itself
	}()
	txCtx := txToContext(ctx, t.txKey, newDB)

	if err := txFunc(txCtx); err != nil {
		return err
	}

	if err := currentTX.Commit(); err != nil {
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
