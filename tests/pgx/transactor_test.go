package pgx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Thiht/transactor"
	pgxTransactor "github.com/Thiht/transactor/pgx"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactor(t *testing.T) {
	t.Parallel()

	t.Run("it should implement the Transactor interface", func(t *testing.T) {
		t.Parallel()
		assert.Implements(t, (*transactor.Transactor)(nil), &pgxTransactor.Transactor{})
	})

	t.Run("it should rollback the transaction if the callback fails", func(t *testing.T) {
		t.Parallel()

		db, err := pgxmock.NewConn()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close(t.Context())
		})

		transactor, _ := pgxTransactor.NewTransactor(db)

		db.ExpectBegin()
		db.ExpectRollback()

		err = transactor.WithinTransaction(context.Background(), func(_ context.Context) error {
			return errors.New("an error occurred")
		})
		require.Error(t, err)

		require.NoError(t, db.ExpectationsWereMet())
	})

	t.Run("it should commit the transaction if the callback succeeds", func(t *testing.T) {
		t.Parallel()

		db, err := pgxmock.NewConn()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close(t.Context())
		})

		transactor, _ := pgxTransactor.NewTransactor(db)

		db.ExpectBegin()
		db.ExpectCommit()

		err = transactor.WithinTransaction(context.Background(), func(_ context.Context) error {
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, db.ExpectationsWereMet())
	})

	t.Run("it should return an error if the commit fails", func(t *testing.T) {
		t.Parallel()

		db, err := pgxmock.NewConn()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close(t.Context())
		})

		transactor, _ := pgxTransactor.NewTransactor(db)

		db.ExpectBegin()
		db.ExpectCommit().WillReturnError(assert.AnError)
		// Note: after a failed Commit, Rollback is called but doesn't reach the mock because
		// the transaction is already marked as done. Rollback returns early with ErrTxDone.

		err = transactor.WithinTransaction(context.Background(), func(_ context.Context) error {
			return nil
		})
		require.Error(t, err)

		require.NoError(t, db.ExpectationsWereMet())
	})
}

func TestTransactor_IsWithinTransaction(t *testing.T) {
	t.Parallel()

	t.Run("it should return false if the context is not within a transaction", func(t *testing.T) {
		t.Parallel()

		db, err := pgxmock.NewConn()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close(t.Context())
		})

		transactor, _ := pgxTransactor.NewTransactor(db)

		ctx := context.Background()
		assert.False(t, transactor.IsWithinTransaction(ctx))
		assert.False(t, pgxTransactor.IsWithinTransaction(ctx))
	})

	t.Run("it should return true if the context is within a transaction", func(t *testing.T) {
		t.Parallel()

		db, err := pgxmock.NewConn()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close(t.Context())
		})

		transactor, _ := pgxTransactor.NewTransactor(db)

		db.ExpectBegin()
		db.ExpectCommit()

		err = transactor.WithinTransaction(context.Background(), func(ctx context.Context) error {
			assert.True(t, transactor.IsWithinTransaction(ctx))
			assert.True(t, pgxTransactor.IsWithinTransaction(ctx))
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, db.ExpectationsWereMet())
	})

	t.Run("it should return false if the context is within another transactor transaction", func(t *testing.T) {
		t.Parallel()

		db, err := pgxmock.NewConn()
		require.NoError(t, err)
		t.Cleanup(func() {
			db.Close(t.Context())
		})

		transactorA, _ := pgxTransactor.NewTransactor(db)
		transactorB, _ := pgxTransactor.NewTransactor(db)

		db.ExpectBegin()
		db.ExpectCommit()

		err = transactorA.WithinTransaction(context.Background(), func(ctx context.Context) error {
			assert.True(t, transactorA.IsWithinTransaction(ctx))
			assert.False(t, transactorB.IsWithinTransaction(ctx))
			assert.True(t, pgxTransactor.IsWithinTransaction(ctx))
			return nil
		})
		require.NoError(t, err)

		require.NoError(t, db.ExpectationsWereMet())
	})
}
