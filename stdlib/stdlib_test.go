package stdlib_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/Thiht/transactor/stdlib"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxStdlib "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func init() {
	testcontainers.DefaultLoggingHook = func(logger testcontainers.Logging) testcontainers.ContainerLifecycleHooks {
		return testcontainers.ContainerLifecycleHooks{}
	}
}

func TestTransactor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		postgres.WithInitScripts("../testdata/init.sql"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, postgresContainer.Terminate(ctx))
	})

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	t.Run("with the pgx stdlib driver", func(t *testing.T) {
		db, err := sql.Open("pgx", dsn)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})

		transactor, dbGetter := stdlib.NewTransactor(db)

		t.Run("it should rollback the transaction", func(t *testing.T) {
			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
				require.NoError(t, err)

				_, err = dbGetter(ctx).ExecContext(ctx, "SELECT 1/0")
				require.Error(t, err)

				return err
			})
			require.Error(t, err)

			var amount int
			err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
			require.NoError(t, err)
			require.Equal(t, 100, amount)
		})

		t.Run("it should commit the transaction", func(t *testing.T) {
			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				var amount int
				err := dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
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

		t.Run("it should fail to begin a nested transaction", func(t *testing.T) {
			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				return transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					return nil
				})
			})

			require.ErrorContains(t, err, "nested transactions are not supported")
		})
	})

	t.Run("with pgxpool", func(t *testing.T) {
		pool, err := pgxpool.New(context.Background(), dsn)
		require.NoError(t, err)
		t.Cleanup(func() {
			pool.Close()
		})

		db := pgxStdlib.OpenDBFromPool(pool)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})

		transactor, dbGetter := stdlib.NewTransactor(db)

		t.Run("it should rollback the transaction", func(t *testing.T) {
			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
				require.NoError(t, err)

				_, err = dbGetter(ctx).ExecContext(ctx, "SELECT 1/0")
				require.Error(t, err)

				return err
			})
			require.Error(t, err)

			var amount int
			err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
			require.NoError(t, err)
			require.Equal(t, 110, amount)
		})

		t.Run("it should commit the transaction", func(t *testing.T) {
			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				var amount int
				err := dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 110, amount)

				_, err = dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = $1 WHERE id = 1", amount+10)
				require.NoError(t, err)

				return err
			})
			require.NoError(t, err)

			var amount int
			err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
			require.NoError(t, err)
			require.Equal(t, 120, amount)
		})

		t.Run("it should fail to begin a nested transaction", func(t *testing.T) {
			err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
				return transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					return nil
				})
			})

			require.ErrorContains(t, err, "nested transactions are not supported")
		})
	})
}
