package stdlib

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Thiht/transactor"
)

func NewTransactor(db *sql.DB, nestedTransactionStrategy nestedTransactionStrategy) (transactor.Transactor, DBGetter) {
	sqlDBGetter := func(ctx context.Context) sqlDB {
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

	return &stdlibTransactor{
		sqlDBGetter,
		nestedTransactionStrategy,
	}, dbGetter
}

type (
	sqlDBGetter               func(context.Context) sqlDB
	nestedTransactionStrategy func(sqlDB, *sql.Tx) (sqlDB, sqlTx)
)

type stdlibTransactor struct {
	sqlDBGetter
	nestedTransactionStrategy
}

func (t *stdlibTransactor) WithinTransaction(ctx context.Context, txFunc func(context.Context) error) error {
	currentDB := t.sqlDBGetter(ctx)

	tx, err := currentDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	newDB, currentTX := t.nestedTransactionStrategy(currentDB, tx)
	txCtx := txToContext(ctx, newDB)

	if err := txFunc(txCtx); err != nil {
		_ = currentTX.Rollback() // If rollback fails, there's nothing to do, the transaction will expire by itself
		return err
	}

	if err := currentTX.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
