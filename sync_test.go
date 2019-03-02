package deq

// func TestSyncTo(t *testing.T) {
// 	t.Parallel()

// 	db, discard := newTestDB()
// 	defer discard()
// 	db2, discard2 := newTestDB()
// 	defer discard2()

// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	// Round(0) gets rid of leap-second info, which will be lost in serialization
// 	createTime := time.Now().Round(0)

// 	// Publish some events
// 	events := struct {
// 		Before, After, ExpectedBefore, ExpectedAfter, ExpectedResponses []Event
// 	}{
// 		Before: []Event{
// 			{
// 				ID:         "before-event1",
// 				Topic:      "TopicA",
// 				CreateTime: createTime,
// 			},
// 			{
// 				ID:         "before-event2",
// 				Topic:      "TopicA",
// 				CreateTime: createTime,
// 			},
// 			{
// 				ID:         "before-event1",
// 				Topic:      "TopicB",
// 				CreateTime: createTime,
// 			},
// 		},
// 		ExpectedBefore: []Event{
// 			{
// 				ID:           "before-event1",
// 				Topic:        "TopicA",
// 				CreateTime:   createTime,
// 				DefaultState: EventStateQueued,
// 				State:        EventStateQueued,
// 			},
// 			{
// 				ID:           "before-event2",
// 				Topic:        "TopicA",
// 				CreateTime:   createTime,
// 				DefaultState: EventStateQueued,
// 				State:        EventStateQueued,
// 			},
// 		},
// 		After: []Event{
// 			{
// 				ID:         "after-event1",
// 				Topic:      "TopicA",
// 				CreateTime: createTime,
// 			},
// 			{
// 				ID:         "after-event2",
// 				Topic:      "TopicA",
// 				CreateTime: createTime,
// 			},
// 			{
// 				ID:         "after-event1",
// 				Topic:      "TopicB",
// 				CreateTime: createTime,
// 			},
// 		},
// 		ExpectedAfter: []Event{
// 			{
// 				ID:           "after-event1",
// 				Topic:        "TopicA",
// 				CreateTime:   createTime,
// 				DefaultState: EventStateQueued,
// 				State:        EventStateQueued,
// 			},
// 			{
// 				ID:           "after-event2",
// 				Topic:        "TopicA",
// 				CreateTime:   createTime,
// 				DefaultState: EventStateQueued,
// 				State:        EventStateQueued,
// 			},
// 		},
// 		ExpectedResponses: []Event{
// 			{
// 				ID:           "before-event1",
// 				Topic:        "Response-TopicA",
// 				CreateTime:   createTime,
// 				DefaultState: EventStateQueued,
// 				State:        EventStateQueued,
// 			},
// 			{
// 				ID:           "before-event2",
// 				Topic:        "Response-TopicA",
// 				CreateTime:   createTime,
// 				DefaultState: EventStateQueued,
// 				State:        EventStateQueued,
// 			},
// 			{
// 				ID:           "after-event1",
// 				Topic:        "Response-TopicA",
// 				CreateTime:   createTime,
// 				DefaultState: EventStateQueued,
// 				State:        EventStateQueued,
// 			},
// 			{
// 				ID:           "after-event2",
// 				Topic:        "Response-TopicA",
// 				CreateTime:   createTime,
// 				DefaultState: EventStateQueued,
// 				State:        EventStateQueued,
// 			},
// 		},
// 	}

// 	for _, e := range events.Before {
// 		_, err := db.Pub(e)
// 		if err != nil {
// 			t.Fatalf("pub: %v", err)
// 		}
// 	}

// 	errc := make(chan error)
// 	recieved := make(chan Event)
// 	responses := make(chan Event)

// 	// Subscribe to events
// 	go func() {
// 		channel := db.Channel("test-channel", "TopicA")
// 		defer channel.Close()

// 		errc <- channel.SyncTo(ctx, client)
// 	}()

// 	// Subscribe to response events
// 	go func() {
// 		channel := db.Channel("test-channel", "Response-TopicA")
// 		defer channel.Close()

// 		errc <- channel.Sub(ctx, func(e Event) (*Event, ack.Code) {

// 			responses <- e

// 			return nil, ack.DequeueOK
// 		})
// 	}()

// 	// Check Sub error just before we cancel the context
// 	defer func() {
// 		select {
// 		case err := <-errc:
// 			t.Errorf("subscribe: %v", err)
// 		default:
// 		}
// 	}()

// 	// Verify that events were recieved by handler
// 	var actual []Event
// 	for e := range recieved {
// 		actual = append(actual, e)
// 		if len(actual) >= len(events.ExpectedBefore) {
// 			break
// 		}
// 	}
// 	if !cmp.Equal(events.ExpectedBefore, actual) {
// 		t.Errorf("pre-sub recieved events:\n%s", cmp.Diff(events.ExpectedBefore, actual))
// 	}

// 	// Publish some more events now that we're already subscribed
// 	for _, e := range events.After {
// 		_, err := db.Pub(e)
// 		if err != nil {
// 			t.Fatalf("pub: %v", err)
// 		}
// 	}

// 	// Verify that events were recieved by handler
// 	actual = nil
// 	for e := range recieved {
// 		actual = append(actual, e)
// 		if len(actual) >= len(events.ExpectedAfter) {
// 			break
// 		}
// 	}
// 	if !cmp.Equal(events.ExpectedAfter, actual) {
// 		t.Errorf("post-sub recieved events:\n%s", cmp.Diff(events.ExpectedAfter, actual))
// 	}

// 	// Verify that response events were published
// 	actual = nil
// 	for e := range responses {
// 		actual = append(actual, e)
// 		if len(actual) >= len(events.ExpectedResponses) {
// 			break
// 		}
// 	}
// 	if !cmp.Equal(events.ExpectedResponses, actual) {
// 		t.Errorf("response events:\n%s", cmp.Diff(events.ExpectedResponses, actual))
// 	}
// }
