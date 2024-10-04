package pgx_test

import (
	"context"
	"testing"

	pgxTransactor "github.com/Thiht/transactor/pgx"
	"github.com/stretchr/testify/assert"
)

func TestIsWithinTransaction(t *testing.T) {
	t.Parallel()

	t.Run("it should return false if the context is not within a transaction", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		assert.False(t, pgxTransactor.IsWithinTransaction(ctx))
	})
}
