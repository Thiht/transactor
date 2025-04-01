package pgx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	pgxTransactor "github.com/Thiht/transactor/pgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIntegrationTransactorPostgres(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	testcontainers.DefaultLoggingHook = func(log.Logger) testcontainers.ContainerLifecycleHooks {
		return testcontainers.ContainerLifecycleHooks{}
	}
	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithInitScripts("../testdata/init_postgres.sql"),
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

	t.Run("with a pgx conn", func(t *testing.T) {
		reset := func(ctx context.Context, db *pgx.Conn) {
			t.Helper()
			_, err := db.Exec(ctx, "UPDATE balances SET amount = 100 WHERE id = 1")
			require.NoError(t, err)
		}

		db, err := pgx.Connect(ctx, dsn)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, db.Close(ctx))
		})

		transactor, dbGetter := pgxTransactor.NewTransactor(db)

		t.Run("it should rollback the transaction", func(t *testing.T) {
			t.Cleanup(func() {
				reset(ctx, db)
			})

			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = 50 WHERE id = 1")
				require.NoError(t, err)

				return errors.New("an error occurred")
			})
			require.Error(t, err)

			var amount int
			err = dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
			require.NoError(t, err)
			require.Equal(t, 100, amount)
		})

		t.Run("it should commit the transaction", func(t *testing.T) {
			t.Cleanup(func() {
				reset(ctx, db)
			})

			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				var amount int
				err := dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1 FOR UPDATE").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 100, amount)

				_, err = dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = $1 WHERE id = 1", amount+10)
				require.NoError(t, err)

				return err
			})
			require.NoError(t, err)

			var amount int
			err = dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
			require.NoError(t, err)
			require.Equal(t, 110, amount)
		})

		t.Run("with nested transactions", func(t *testing.T) {
			t.Run("it should rollback the nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(ctx, db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = 50 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = 70 WHERE id = 1")
						require.NoError(t, err)

						return errors.New("an error occurred")
					})
					require.Error(t, err)

					var amount int
					err = dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
					require.NoError(t, err)
					require.Equal(t, 50, amount)

					return nil
				})
				require.NoError(t, err)
			})

			t.Run("it should commit the nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(ctx, db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
						require.NoError(t, err)

						return nil
					})
					require.NoError(t, err)

					return nil
				})
				require.NoError(t, err)

				var amount int
				err = dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 120, amount)
			})

			t.Run("it should commit the second nested transaction and rollback the first nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(ctx, db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
						require.NoError(t, err)

						err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
							_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
							require.NoError(t, err)

							return nil
						})
						require.NoError(t, err)

						return errors.New("an error occurred")
					})
					require.Error(t, err)

					return nil
				})
				require.NoError(t, err)

				var amount int
				err = dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 110, amount)
			})
		})
	})

	t.Run("with a pgx pool", func(t *testing.T) {
		reset := func(ctx context.Context, db *pgxpool.Pool) {
			t.Helper()
			_, err := db.Exec(ctx, "UPDATE balances SET amount = 100 WHERE id = 1")
			require.NoError(t, err)
		}

		db, err := pgxpool.New(ctx, dsn)
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close()
		})

		transactor, dbGetter := pgxTransactor.NewTransactorFromPool(db)

		t.Run("it should rollback the transaction", func(t *testing.T) {
			t.Cleanup(func() {
				reset(ctx, db)
			})

			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = 50 WHERE id = 1")
				require.NoError(t, err)

				return errors.New("an error occurred")
			})
			require.Error(t, err)

			var amount int
			err = dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
			require.NoError(t, err)
			require.Equal(t, 100, amount)
		})

		t.Run("it should commit the transaction", func(t *testing.T) {
			t.Cleanup(func() {
				reset(ctx, db)
			})

			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				var amount int
				err := dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1 FOR UPDATE").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 100, amount)

				_, err = dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = $1 WHERE id = 1", amount+10)
				require.NoError(t, err)

				return err
			})
			require.NoError(t, err)

			var amount int
			err = dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
			require.NoError(t, err)
			require.Equal(t, 110, amount)
		})

		t.Run("with nested transactions", func(t *testing.T) {
			t.Run("it should rollback the nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(ctx, db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = 50 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = 70 WHERE id = 1")
						require.NoError(t, err)

						return errors.New("an error occurred")
					})
					require.Error(t, err)

					var amount int
					err = dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
					require.NoError(t, err)
					require.Equal(t, 50, amount)

					return nil
				})
				require.NoError(t, err)
			})

			t.Run("it should commit the nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(ctx, db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
						require.NoError(t, err)

						return nil
					})
					require.NoError(t, err)

					return nil
				})
				require.NoError(t, err)

				var amount int
				err = dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 120, amount)
			})

			t.Run("it should commit the second nested transaction and rollback the first nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(ctx, db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
						require.NoError(t, err)

						err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
							_, err := dbGetter(ctx).Exec(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
							require.NoError(t, err)

							return nil
						})
						require.NoError(t, err)

						return errors.New("an error occurred")
					})
					require.Error(t, err)

					return nil
				})
				require.NoError(t, err)

				var amount int
				err = dbGetter(ctx).QueryRow(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 110, amount)
			})
		})
	})
}
