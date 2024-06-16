package stdlib_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Thiht/transactor/stdlib"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	_ "modernc.org/sqlite"
)

func TestIntegrationTransactorPostgres(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	testcontainers.DefaultLoggingHook = func(_ testcontainers.Logging) testcontainers.ContainerLifecycleHooks {
		return testcontainers.ContainerLifecycleHooks{}
	}
	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:16-alpine"),
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

						return errors.New("an error occurred")
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

					return nil
				})
				require.NoError(t, err)

				var amount int
				err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 120, amount)
			})

			t.Run("it should commit the second nested transaction and rollback the first nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
						require.NoError(t, err)

						err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
							_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
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
				err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 110, amount)
			})
		})
	})
}

func TestIntegrationTransactorMySQL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	testcontainers.DefaultLoggingHook = func(_ testcontainers.Logging) testcontainers.ContainerLifecycleHooks {
		return testcontainers.ContainerLifecycleHooks{}
	}
	mysqlContainer, err := mysql.RunContainer(ctx,
		testcontainers.WithImage("mysql:8.0"),
		mysql.WithScripts("../testdata/init_mysql.sql"),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mysqlContainer.Terminate(ctx))
	})

	dsn, err := mysqlContainer.ConnectionString(ctx)
	require.NoError(t, err)

	reset := func(db *sql.DB) {
		t.Helper()
		_, err := db.Exec("UPDATE balances SET amount = 100 WHERE id = 1")
		require.NoError(t, err)
	}

	t.Run("with the mysql driver", func(t *testing.T) {
		db, err := sql.Open("mysql", dsn)
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

				_, err = dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = ? WHERE id = 1", amount+10)
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

						return errors.New("an error occurred")
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

					return nil
				})
				require.NoError(t, err)

				var amount int
				err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 120, amount)
			})

			t.Run("it should commit the second nested transaction and rollback the first nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
						require.NoError(t, err)

						err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
							_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
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
				err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 110, amount)
			})
		})
	})
}

func TestIntegrationTransactorSQLite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	initScript, err := os.ReadFile("../testdata/init_sqlite.sql")
	require.NoError(t, err)

	reset := func(db *sql.DB) {
		t.Helper()
		_, err := db.Exec("UPDATE balances SET amount = 100 WHERE id = 1")
		require.NoError(t, err)
	}

	t.Run("with the sqlite driver", func(t *testing.T) {
		db, err := sql.Open("sqlite", ":memory:")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})

		_, err = db.Exec(string(initScript))
		require.NoError(t, err)

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
				err := dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 100, amount)

				_, err = dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = ? WHERE id = 1", amount+10)
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

						return errors.New("an error occurred")
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

					return nil
				})
				require.NoError(t, err)

				var amount int
				err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 120, amount)
			})

			t.Run("it should commit the second nested transaction and rollback the first nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
						require.NoError(t, err)

						err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
							_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
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
				err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 110, amount)
			})
		})
	})
}

func TestIntegrationTransactorOracle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	testcontainers.DefaultLoggingHook = func(_ testcontainers.Logging) testcontainers.ContainerLifecycleHooks {
		return testcontainers.ContainerLifecycleHooks{}
	}

	oracleContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "gvenzl/oracle-free:23.4-slim-faststart",
			ExposedPorts: []string{"1521/tcp"},
			Env: map[string]string{
				"ORACLE_PASSWORD": "test",
			},
			WaitingFor: wait.ForLog("DATABASE IS READY TO USE!").WithStartupTimeout(5 * time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, oracleContainer.Terminate(ctx))
	})

	containerPort, err := oracleContainer.MappedPort(ctx, "1521/tcp")
	require.NoError(t, err)

	dsn := go_ora.BuildUrl("localhost", containerPort.Int(), "freepdb1", "system", "test", nil)

	initScript, err := os.ReadFile("../testdata/init_oracle.sql")
	require.NoError(t, err)

	reset := func(db *sql.DB) {
		t.Helper()
		_, err := db.Exec("UPDATE balances SET amount = 100 WHERE id = 1")
		require.NoError(t, err)
	}

	t.Run("with the oracle driver", func(t *testing.T) {
		db, err := sql.Open("oracle", dsn)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})

		_, err = db.Exec(string(initScript))
		require.NoError(t, err)

		transactor, dbGetter := stdlib.NewTransactor(db, stdlib.NestedTransactionsOracle)

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

				_, err = dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = (:1) WHERE id = 1", amount+10)
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

						return errors.New("an error occurred")
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

					return nil
				})
				require.NoError(t, err)

				var amount int
				err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 120, amount)
			})

			t.Run("it should commit the second nested transaction and rollback the first nested transaction", func(t *testing.T) {
				t.Cleanup(func() {
					reset(db)
				})

				err := transactor.WithinTransaction(ctx, func(ctx context.Context) error {
					_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
					require.NoError(t, err)

					err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
						_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
						require.NoError(t, err)

						err = transactor.WithinTransaction(ctx, func(ctx context.Context) error {
							_, err := dbGetter(ctx).ExecContext(ctx, "UPDATE balances SET amount = amount + 10 WHERE id = 1")
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
				err = dbGetter(ctx).QueryRowContext(ctx, "SELECT amount FROM balances WHERE id = 1").Scan(&amount)
				require.NoError(t, err)
				require.Equal(t, 110, amount)
			})
		})
	})
}
