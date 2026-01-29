package sqlx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sqlxTransactor "github.com/Thiht/transactor/sqlx"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactor(t *testing.T) {
	t.Parallel()

	t.Run("it should rollback the transaction if the callback fails", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close()
		})
		sqlxDB := sqlx.NewDb(db, "sqlmock")

		transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsNone)

		mock.ExpectBegin()
		mock.ExpectRollback()

		err = transactor.WithinTransaction(context.Background(), func(_ context.Context) error {
			return errors.New("an error occurred")
		})
		require.Error(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("it should commit the transaction if the callback succeeds", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close()
		})
		sqlxDB := sqlx.NewDb(db, "sqlmock")

		transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsNone)

		mock.ExpectBegin()
		mock.ExpectCommit()

		err = transactor.WithinTransaction(context.Background(), func(_ context.Context) error {
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("it should store a valid transaction wrapper in the context", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = db.Close()
		})
		sqlxDB := sqlx.NewDb(db, "postgres")

		transactor, dbGetter := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsNone)
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT 1").WithArgs(nil).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(true))
		mock.ExpectCommit()
		ctx := context.Background()
		err = transactor.WithinTransaction(ctx, func(txCtx context.Context) error {
			var result bool
			// we only know the transaction wrapper is valid if we can use it to perform a query
			db := dbGetter(txCtx)
			err := db.QueryRowxContext(txCtx, "SELECT 1", nil).Scan(&result)
			require.NoError(t, err)
			require.True(t, result)
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("it should return an error if the commit fails", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close()
		})
		sqlxDB := sqlx.NewDb(db, "sqlmock")

		transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsNone)

		mock.ExpectBegin()
		mock.ExpectCommit().WillReturnError(assert.AnError)
		// Note: after a failed Commit, Rollback is called but doesn't reach the mock because
		// the transaction is already marked as done. Rollback returns early with ErrTxDone.

		err = transactor.WithinTransaction(context.Background(), func(_ context.Context) error {
			return nil
		})
		require.Error(t, err)

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
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsNone)

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

		t.Run("it should rollback the nested transaction in case of error", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsSavepoints)

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

		t.Run("it should return the original error in case of failure to rollback a nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsSavepoints)

			mock.ExpectBegin()
			mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("ROLLBACK TO SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0)).WillReturnError(assert.AnError)
			mock.ExpectCommit()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return errors.New("an error occurred")
				})
				require.Error(t, err)
				require.ErrorContains(t, err, "an error occurred")

				return nil
			})
			require.NoError(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})

		t.Run("it should return an error in case of failure to begin a nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsSavepoints)

			mock.ExpectBegin()
			mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0)).WillReturnError(assert.AnError)
			mock.ExpectCommit()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return nil
				})
				require.Error(t, err)

				return nil
			})
			require.NoError(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})

		t.Run("it should return an error in case of failure to commit a nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsSavepoints)

			mock.ExpectBegin()
			mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("RELEASE SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0)).WillReturnError(assert.AnError)
			mock.ExpectCommit()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return nil
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
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsSavepoints)

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
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsSavepoints)

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
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsSavepoints)

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

	t.Run("with nested transactions mssql", func(t *testing.T) {
		t.Parallel()

		t.Run("it should rollback the nested transaction in case of error", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsMSSQL)

			mock.ExpectBegin()
			mock.ExpectExec("SAVE TRANSACTION sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("ROLLBACK TRANSACTION sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
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

		t.Run("it should return the original error in case of failure to rollback a nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsMSSQL)

			mock.ExpectBegin()
			mock.ExpectExec("SAVE TRANSACTION sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("ROLLBACK TRANSACTION sp_1").WillReturnResult(sqlmock.NewResult(0, 0)).WillReturnError(assert.AnError)
			mock.ExpectCommit()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return errors.New("an error occurred")
				})
				require.Error(t, err)
				require.ErrorContains(t, err, "an error occurred")

				return nil
			})
			require.NoError(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})

		t.Run("it should return an error in case of failure to begin a nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsMSSQL)

			mock.ExpectBegin()
			mock.ExpectExec("SAVE TRANSACTION sp_1").WillReturnResult(sqlmock.NewResult(0, 0)).WillReturnError(assert.AnError)
			mock.ExpectCommit()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return nil
				})
				require.Error(t, err)

				return nil
			})
			require.NoError(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})
	})

	t.Run("with nested transactions oracle", func(t *testing.T) {
		t.Parallel()

		t.Run("it should rollback the nested transaction in case of error", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsOracle)

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

		t.Run("it should return the original error in case of failure to rollback a nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsOracle)

			mock.ExpectBegin()
			mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("ROLLBACK TO SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0)).WillReturnError(assert.AnError)
			mock.ExpectCommit()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return errors.New("an error occurred")
				})
				require.Error(t, err)
				require.ErrorContains(t, err, "an error occurred")

				return nil
			})
			require.NoError(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})

		t.Run("it should return an error in case of failure to begin a nested transaction", func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Close()
			})
			sqlxDB := sqlx.NewDb(db, "sqlmock")

			transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsOracle)

			mock.ExpectBegin()
			mock.ExpectExec("SAVEPOINT sp_1").WillReturnResult(sqlmock.NewResult(0, 0)).WillReturnError(assert.AnError)
			mock.ExpectCommit()

			err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
				err := transactor.WithinTransaction(ctx, func(_ context.Context) error {
					return nil
				})
				require.Error(t, err)

				return nil
			})
			require.NoError(t, err)

			require.NoError(t, mock.ExpectationsWereMet())
		})
	})
}

func TestIsWithinTransaction(t *testing.T) {
	t.Parallel()

	t.Run("it should return false if the context is not within a transaction", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		assert.False(t, sqlxTransactor.IsWithinTransaction(ctx))
	})

	t.Run("it should return true if the context is within a transaction", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close()
		})
		sqlxDB := sqlx.NewDb(db, "sqlmock")

		transactor, _ := sqlxTransactor.NewTransactor(sqlxDB, sqlxTransactor.NestedTransactionsNone)

		mock.ExpectBegin()
		mock.ExpectCommit()

		err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
			assert.True(t, sqlxTransactor.IsWithinTransaction(ctx))
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})
}
