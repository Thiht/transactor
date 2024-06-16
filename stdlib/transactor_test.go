package stdlib_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Thiht/transactor/stdlib"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

func TestTransactor(t *testing.T) {
	t.Parallel()

	t.Run("it should rollback the transaction", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close()
		})

		transactor, _ := stdlib.NewTransactor(db, stdlib.NestedTransactionsNone)

		mock.ExpectBegin()
		mock.ExpectRollback()

		err = transactor.WithinTransaction(context.Background(), func(_ context.Context) error {
			return errors.New("an error occurred")
		})
		require.Error(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("it should commit the transaction", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close()
		})

		transactor, _ := stdlib.NewTransactor(db, stdlib.NestedTransactionsNone)

		mock.ExpectBegin()
		mock.ExpectCommit()

		err = transactor.WithinTransaction(context.Background(), func(_ context.Context) error {
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("with no nested transactions support", func(t *testing.T) {
		t.Parallel()

		t.Run("it should fail to create a nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})

			transactor, _ := stdlib.NewTransactor(db, stdlib.NestedTransactionsNone)

			mock.ExpectBegin()
			mock.ExpectRollback()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return nil
				})
				require.Error(t, err)

				return err
			})
			require.Error(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})
	})

	t.Run("with nested transactions savepoints", func(t *testing.T) {
		t.Parallel()

		t.Run("it should rollback the nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})

			transactor, _ := stdlib.NewTransactor(db, stdlib.NestedTransactionsSavepoints)

			mock.ExpectBegin()
			mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("ROLLBACK TO SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectCommit()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return errors.New("an error occurred")
				})
				require.Error(t, err)

				return nil
			})
			require.NoError(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})

		t.Run("it should commit the nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})

			transactor, _ := stdlib.NewTransactor(db, stdlib.NestedTransactionsSavepoints)

			mock.ExpectBegin()
			mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("RELEASE SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectCommit()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return nil
				})
				require.NoError(t, err)

				return nil
			})
			require.NoError(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})

		t.Run("it should rollback the nested transaction and the parent transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})

			transactor, _ := stdlib.NewTransactor(db, stdlib.NestedTransactionsSavepoints)

			mock.ExpectBegin()
			mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("ROLLBACK TO SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectRollback()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return errors.New("an error occurred")
				})
				require.Error(t, err)

				return err
			})
			require.Error(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})

		t.Run("it should commit the second nested transaction and rollback the first nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})

			transactor, _ := stdlib.NewTransactor(db, stdlib.NestedTransactionsSavepoints)

			mock.ExpectBegin()
			mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("SAVEPOINT sp_2").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("RELEASE SAVEPOINT sp_2").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("ROLLBACK TO SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectCommit()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
						return nil
					})
					require.NoError(t, err)

					return errors.New("an error occurred")
				})
				require.Error(t, err)

				return nil
			})
			require.NoError(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})
	})
}
