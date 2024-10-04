# transactor

[![Go Reference](https://pkg.go.dev/badge/github.com/Thiht/transactor.svg)](https://pkg.go.dev/github.com/Thiht/transactor) [![Main Pipeline](https://github.com/Thiht/transactor/actions/workflows/main.yml/badge.svg)](https://github.com/Thiht/transactor/actions/workflows/main.yml) [![Coverage](https://codecov.io/github/Thiht/transactor/graph/badge.svg?token=NK6KCBMTR6)](https://codecov.io/github/Thiht/transactor)

The **transactor** pattern is a way to manage transactions seamlessly.
You can inject your transactor in your services to make transactions completely transparently.

It relies mostly on the [`Transactor` interface](./transactor.go):

```go
type Transactor interface {
  WithinTransaction(context.Context, func(context.Context) error) error
}
```

`WithinTransaction` starts a new transaction and adds it to the context. Any repository method can then retrieve a transaction from the context or fallback to the initial DB handler. The transaction is committed if the provided function doesn't return an error. It's rollbacked otherwise.

## Usage

### Installation

```sh
go get github.com/Thiht/transactor
```

The `database/sql` default implementation (`stdlib`) is included in the `github.com/Thiht/transactor` package.
Additional implementations are available in separate modules:

- the [`pgx`](https://github.com/jackc/pgx) implementation is available in `github.com/Thiht/transactor/pgx`,
- the [`sqlx`](https://github.com/jmoiron/sqlx) implementation is available in `github.com/Thiht/transactor/sqlx`.

The following examples use the `stdlib` implementation, but the code isn't too different with the other implementations.

### Initialize a `transactor`

This example uses `database/sql` with the [`pgx`](https://github.com/jackc/pgx) driver, but any `database/sql` driver can be used.

```go
import stdlibTransactor "github.com/Thiht/transactor/stdlib"

db, _ := sql.Open("pgx", dsn)

transactor, dbGetter := stdlibTransactor.NewTransactor(
  db,
  stdlibTransactor.NestedTransactionsSavepoints,
)
```

The currently available strategies for nested transactions with the `stdlib` implementation are:

- [NestedTransactionsSavepoints](./stdlib/nested_transactions_savepoints.go), an implementation using `SAVEPOINTS` and compatible with [PostgreSQL](https://www.postgresql.org/docs/16/sql-savepoint.html), [MySQL](https://dev.mysql.com/doc/refman/8.0/en/savepoint.html), [MariaDB](https://mariadb.com/kb/en/savepoint/), and [SQLite](https://sqlite.org/lang_savepoint.html),
- [NestedTransactionsOracle](./stdlib/nested_transactions_oracle.go), an implementation using [Oracle savepoints](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/SAVEPOINT.html),
- [NestedTransactionsMSSQL](./stdlib/nested_transactions_mssql.go), an implementation using [Microsoft SQL Server savepoints](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/save-transaction-transact-sql?view=sql-server-ver16),
- [NestedTransactionsNone](./stdlib/nested_transactions_none.go), an implementation that prevents using nested transactions.

### Use the `dbGetter` in your repositories

Instead of injecting the `*sql.DB` handler directly to your repositories, you now have to inject the `dbGetter`. It will return the appropriate DB handler depending on whether the current execution is in a transaction.

```diff
type store struct {
-  db *sql.DB
+  dbGetter stdlibTransactor.DBGetter
}

func (s store) GetBalance(ctx context.Context, account string) (int, error) {
  var balance int
-  err := s.db.QueryRowContext(
+  err := s.dbGetter(ctx).QueryRowContext(
    ctx,
    `SELECT balance FROM accounts WHERE account = $1`,
    account,
  ).Scan(&balance)
  return balance, err
}
```

You can use the `IsWithinTransaction` helper if you need to implement different behaviours depending on whether a transaction is running.
For example with PostgreSQL, you could add [`FOR UPDATE`](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE) conditionally:

```go
func (s store) GetBalance(ctx context.Context, account string) (int, error) {
  query := `SELECT balance FROM accounts WHERE account = $1`
  if stdlibTransactor.IsWithinTransaction(ctx) {
    query += ` FOR UPDATE`
  }

  // ...
}
```

### Use the `transactor` in your services

```go
type service struct {
  balanceStore stores.Balance
  transactor transactor.Transactor
}

func (s service) IncreaseBalance(ctx context.Context, account string, amount int) error {
  return s.transactor.WithinTransaction(ctx, func(ctx context.Context) error {
    balance, err := s.balanceStore.GetBalance(ctx, account)
    if err != nil {
      return err
    }

    balance += amount

    err = s.balanceStore.SetBalance(ctx, account, balance)
    if err != nil {
      return err
    }

    return nil
  })
}
```

Thanks to nested transactions support, you can even call your services within a transaction:

```go
type service struct {
  balanceStore stores.Balance
  transactor transactor.Transactor
}

func (s service) TransferBalance(
  ctx context.Context,
  fromAccount, toAccount string,
  amount int,
) error {
  return s.transactor.WithinTransaction(ctx, func(ctx context.Context) error {
    err := s.DecreaseBalance(ctx, fromAccount, amount)
    if err != nil {
      return err
    }

    err = s.IncreaseBalance(ctx, toAccount, amount)
    if err != nil {
      return err
    }

    return nil
  })
}

func (s service) IncreaseBalance(ctx context.Context, account string, amount int) error {
  return s.transactor.WithinTransaction(ctx, func(ctx context.Context) error {
    // ...
  })
}

func (s service) DecreaseBalance(ctx context.Context, account string, amount int) error {
  return s.transactor.WithinTransaction(ctx, func(ctx context.Context) error {
    // ...
  })
}
```

> [!WARNING]
> Transactions are not thread safe, so make sure not to call code making concurrent database access inside `WithinTransaction`

### Testing

In your tests, you can inject a fake `transactor` and `dbGetter`, using [NewFakeTransactor](./stdlib/fake_transactor.go):

```go
transactor, dbGetter := stdlibTransactor.NewFakeTransactor(db)
```

The fake `transactor` will simply execute its callback function, and the `dbGetter` will return the provided `db` handler.

This strategy works because when using this library, you don't have to worry about how transactions are made, just about returning errors appropriately in `WithinTransaction`.
