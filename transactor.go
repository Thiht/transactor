package transactor

import "context"

type Transactor interface {
	// WithinTransaction executes the given function within a transaction.
	// The transaction is added to the context, so it has to be retrieved
	// appropriately depending on the transactor implementation.
	WithinTransaction(context.Context, func(context.Context) error) error
}
