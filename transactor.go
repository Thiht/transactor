package transactor

import "context"

type Transactor interface {
	WithinTransaction(context.Context, func(ctx context.Context) error) error
}
