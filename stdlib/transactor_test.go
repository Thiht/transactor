package stdlib_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Thiht/transactor/stdlib"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIntegrationTransactor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	testcontainers.DefaultLoggingHook = func(_ testcontainers.Logging) testcontainers.ContainerLifecycleHooks {
		return testcontainers.ContainerLifecycleHooks{}
	}
	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		postgres.WithInitScripts("../testdata/init.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, postgresContainer.Terminate(ctx))
	})

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	reset := func(db *sql.DB) {
		t.Helper()
		_, err := db.Exec("UPDATE balances SET amount = 100 WHERE id = 1")
		require.NoError(t, err)
	}

	t.Run("with the pgx stdlib driver", func(t *testing.T) {
		db, err := sql.Open("pgx", dsn)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})

		transactor, dbGetter := stdlib.NewTransactor(db, stdlib.NestedTransactionsSavepoints)

		t.Run("it should rollback the transaction", func(t *testing.T) {
			t.Cleanup(func() {
				reset(db)
			})

			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = 50 WHERE id = 1")
				require.NoError(t, err)

				return errors.New("an error occurred")
			})
			require.Error(t, err)

			var amount int
			err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
			require.NoError(t, err)
			require.Equal(t, 100, amount)
		})

		t.Run("it should commit the transaction", func(t *testing.T) {
			t.Cleanup(func() {
				reset(db)
			})

			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				var amount int
				err := dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1 FOR UPDATE").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 100, amount)

				_, err = dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = $1 WHERE id = 1", amount+10)
				require.NoError(t, err)

				return err
			})
			require.NoError(t, err)

			var amount int
			err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
			require.NoError(t, err)
			require.Equal(t, 110, amount)
		})

		t.Run("with nested transactions", func(t *testing.T) {
			t.Run("it should rollback the nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = 50 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = 70 WHERE id = 1")
						require.NoError(t, err)

						_, err = dbGetter(ctx).ExecContext(ctx, "SELECT 1/0")
						require.Error(t, err)

						return err
					})
					require.Error(t, err)

					var amount int
					err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
					require.NoError(t, err)
					require.Equal(t, 50, amount)

					return nil
				})
				require.NoError(t, err)
			})

			t.Run("it should commit the nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
						require.NoError(t, err)

						return nil
					})
					require.NoError(t, err)

					var amount int
					err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
					require.NoError(t, err)
					require.Equal(t, 120, amount)

					return nil
				})
				require.NoError(t, err)
			})
		})
	})
}

func TestTransactor(t *testing.T) {
	t.Parallel()

	t.Run("it should rollback the transaction", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close()
		})

		transactor, _ := stdlib.NewTransactor(db, stdlib.NestedTransactionsSavepoints)

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

		transactor, _ := stdlib.NewTransactor(db, stdlib.NestedTransactionsSavepoints)

		mock.ExpectBegin()
		mock.ExpectCommit()

		err = transactor.WithinTransaction(context.Background(), func(_ context.Context) error {
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("with nested transactions", func(t *testing.T) {
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
