package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// AggregateIterator is an iterator that aggregates the values of base iterator as it iterates without
// interfering with the base iterator.
type AggregateIterator[T any, S any] struct {
	base  badgerutils.Iterator[T]
	state *S
	f     func(*S, T, *badger.Item) *S
}

// Aggregate creates a new aggregate iterator.
func Aggregate[T any, S any](base badgerutils.Iterator[T], f func(*S, T, *badger.Item) *S) *AggregateIterator[T, S] {
	return &AggregateIterator[T, S]{base: base, f: f}
}

// Close implements the Iterator interface.
func (it *AggregateIterator[T, S]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *AggregateIterator[T, S]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *AggregateIterator[T, S]) Next() {
	it.base.Next()
	it.update()
}

func (it *AggregateIterator[T, S]) update() {
	if !it.base.Valid() {
		return
	}

	v, err := it.Value()
	if err != nil {
		return
	}
	it.state = it.f(it.state, v, it.Item())
}

// Rewind implements the Iterator interface.
func (it *AggregateIterator[T, S]) Rewind() {
	it.base.Rewind()
	it.state = nil
	it.update()
}

// Seek implements the Iterator interface.
func (it *AggregateIterator[T, S]) Seek(key []byte) {
	it.base.Seek(key)
	it.state = nil
	it.update()
}

// Valid implements the Iterator interface.
func (it *AggregateIterator[T, S]) Valid() bool {
	return it.base.Valid()
}

// Value implements the Iterator interface.
func (it *AggregateIterator[T, S]) Value() (value T, err error) {
	return it.base.Value()
}

// Result returns the aggregated result.
func (it *AggregateIterator[T, S]) Result() *S {
	return it.state
}
