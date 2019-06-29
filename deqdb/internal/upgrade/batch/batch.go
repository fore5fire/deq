package batch

import (
	"context"
	"errors"
)

// ErrDone indicates that the action is complete and no more batches should be executed. Note that
// ErrDone is provided as a convinience and has no special meaning for this library.
var ErrDone = errors.New("done")

/*
Run executes actionFunc repeatedly until it returns a non-nil error or batchSize executions
are reached. If the batchSize was exceeded, runBatch returns nil. Otherwise, the error returned
by the last call to actionFunc is returned.

Eample Usage:
	ctx, batchSize := context.Background(), 100
	var batchErr error
	for batchErr != batch.ErrDone {

		// Per batch setup code goes here.
		slice := []int{1}

		batchErr = batch.Run(ctx, batchSize, func(ctx context.Context) error {

			// Single iteration code goes here.
			slice = append(slice, slice[len(slice)-1] * 10)
			return nil
		})
		if batchErr != nil && batchErr != batch.ErrDone {
			// handle error
		}

		// Per batch end code goes here
		fmt.Println(slice)
	}
*/
func Run(ctx context.Context, batchSize int, actionFunc func(context.Context) error) error {
	for i := 0; i < batchSize; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := actionFunc(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
