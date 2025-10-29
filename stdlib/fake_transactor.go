package stdlib

import (
	"context"
	"database/sql"
)

// NewFakeTransactor initializes a Transactor and DBGetter that do nothing:
// - the Transactor just executes its callback and returns the error,
// - the DBGetter just returns the DB handler.
// They can be used in tests where the transaction system itself doesn't need to be tested.
func NewFakeTransactor(db *sql.DB) (FakeTransactor, DBGetter) {
	return FakeTransactor{}, func(_ context.Context) DB {
		return db
	}
}

type FakeTransactor struct{}

func (FakeTransactor) WithinTransaction(ctx context.Context, txFunc func(context.Context) error) error {
	return txFunc(ctx)
}
