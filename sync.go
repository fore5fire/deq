package deq

import (
	"context"
	"sync"
)

// SyncTo copies the events in c's queue to the database that client is connected to.
func SyncTo(ctx context.Context, dst Client, src Channel) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerCount := 3

	queue := make(chan Event, 30)
	errorc := make(chan error, 1)

	wg := sync.WaitGroup{}
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			defer cancel()
			err := syncWorker(ctx, dst, queue)
			if err != nil {
				select {
				// Try to send the error back to the original goroutine
				case errorc <- err:
				// If there's no room in the buffer then another goroutine already returned an error
				default:
				}
				return
			}
		}()
	}

	err := src.Sub(ctx, func(ctx context.Context, e Event) (*Event, error) {
		queue <- e
		return nil, nil
	})
	select {
	// Try to send the error back to the original goroutine
	case errorc <- err:
	// If there's no room in the buffer then another goroutine already returned an error
	default:
	}

	close(queue)
	cancel()
	wg.Wait()

	return <-errorc
}

func syncWorker(ctx context.Context, dst Client, queue <-chan Event) error {
	for e := range queue {
		_, err := dst.Pub(ctx, Event{
			ID:         e.ID,
			CreateTime: e.CreateTime,
			Topic:      e.Topic,
			Payload:    e.Payload,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// func (s *Store) SyncFrom(ctx context.Context, client deqc.Client) error {
// 	// client.
// }

// func (s *Store) Sync(ctx context.Context, client deqc.Client) error {

// 	ctx, cancel := context.WithCancel(ctx)
// 	defer cancel()

// 	errorc := make(chan error, 2)

// 	wg := sync.WaitGroup{}
// 	wg.Add(2)

// 	go func() {
// 		defer wg.Done()
// 		err := s.SyncTo(ctx, client)
// 		if err != nil {
// 			errorc <- err
// 		}
// 	}()
// 	go func() {
// 		defer wg.Done()
// 		err := s.SyncFrom(ctx, client)
// 		if err != nil {
// 			errorc <- err
// 		}
// 	}()
// 	wg.Wait()
// 	close(errorc)
// 	return <-errorc
// }
