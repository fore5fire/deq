package deqdb

import (
	"gitlab.com/katcheCode/deq"
	"gitlab.com/katcheCode/deq/deqdb/internal/data"
)

type eventIter struct {
	*data.EventIter
	txn data.Txn
}

// NewEventIter creates a new EventIter that iterates events on the topic and channel of c.
//
// opts.Min and opts.Max specify the range of event IDs to read from c's topic. EventIter only has
// partial support for opts.PrefetchCount.
func (c *Channel) NewEventIter(opts *deq.IterOptions) deq.EventIter {

	txn := c.db.NewTransaction(false)
	it, err := data.NewEventIter(txn, c.topic, c.name, opts)
	if err != nil {
		panic(err.Error())
	}

	return &eventIter{
		EventIter: it,
		txn:       txn,
	}
}

func (it eventIter) Close() {
	it.EventIter.Close()
	it.txn.Discard()
}

type indexIter struct {
	*data.IndexIter
	txn data.Txn
}

// NewIndexIter creates a new IndexIter that iterates events on the topic and channel of c.
//
// opts.Min and opts.Max specify the range of event IDs to read from c's topic. EventIter only has
// partial support for opts.PrefetchCount.
func (c *Channel) NewIndexIter(opts *deq.IterOptions) deq.EventIter {

	txn := c.db.NewTransaction(false)
	it, err := data.NewIndexIter(txn, c.topic, c.name, opts)
	if err != nil {
		panic(err.Error())
	}

	return &indexIter{
		IndexIter: it,
		txn:       txn,
	}
}

func (it indexIter) Close() {
	it.IndexIter.Close()
	it.txn.Discard()
}
