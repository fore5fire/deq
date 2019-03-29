package deqdb

import (
	"context"
	"sync"

	"gitlab.com/katcheCode/deq"
	api "gitlab.com/katcheCode/deq/api/v1/deq"
)

// SyncTo copies the events in c's queue to the database that client is connected to.
func (c *Channel) SyncTo(ctx context.Context, client deq.Client) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerCount := 3

	queue := make(chan deq.Event, 30)
	errorc := make(chan error, 1)

	wg := sync.WaitGroup{}
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			defer cancel()
			err := syncWorker(ctx, client, queue)
			if err != nil {
				select {
				// Try to send the error back to the original goroutine
				case errorc <- err:
				// If there's no room in the buffer then another goroutine already returned an error
				default:
				}
			}
		}()
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer cancel()
		for {
			e, err := c.Next(ctx)
			if err != nil {
				close(queue)

				select {
				case errorc <- err:
				default:
				}
				return
			}

			queue <- e
		}
	}()

	wg.Wait()

	return <-errorc
}

func syncWorker(ctx context.Context, client deq.Client, queue <-chan deq.Event) error {
	for e := range queue {
		_, err := client.Pub(ctx, deq.Event{
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

func stateToProto(e deq.EventState) api.EventState {
	switch e {
	case deq.EventStateUnspecified:
		return api.EventState_UNSPECIFIED_STATE
	case deq.EventStateQueued:
		return api.EventState_QUEUED
	case deq.EventStateDequeuedOK:
		return api.EventState_DEQUEUED_OK
	case deq.EventStateDequeuedError:
		return api.EventState_DEQUEUED_ERROR
	default:
		panic("unrecognized EventState")
	}
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
