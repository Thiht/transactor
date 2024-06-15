package stdlib

import (
	"context"
	"database/sql"
	"fmt"
)

func NestedTransactionNone(db sqlDB, tx *sql.Tx) (sqlDB, sqlTx) {
	switch db.(type) {
	case *sql.DB:
		return &nestedTransactionNone{}, tx

	case *nestedTransactionNone:
		nestedTransaction := db.(*nestedTransactionNone)
		return nestedTransaction, nestedTransaction

	default:
		panic("unsupported type")
	}
}

type nestedTransactionNone struct {
	*sql.Tx
}

var (
	_ sqlDB = &nestedTransactionNone{}
	_ sqlTx = &nestedTransactionNone{}
)

func (t *nestedTransactionNone) Begin() (*sql.Tx, error) {
	return t.BeginTx(context.Background(), nil)
}

func (t *nestedTransactionNone) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	return nil, fmt.Errorf("nested transactions are not supported")
}

func (t *nestedTransactionNone) Commit() error {
	return fmt.Errorf("nested transactions are not supported")
}

func (t *nestedTransactionNone) Rollback() error {
	return fmt.Errorf("nested transactions are not supported")
}
