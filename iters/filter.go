package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// FilterIterator is an iterator that filters the items based on a predicate.
type FilterIterator[T any] struct {
	base badgerutils.Iterator[T]
	f    func(T, *badger.Item) bool
}

// Filter creates a new filter iterator.
func Filter[T any](base badgerutils.Iterator[T], f func(T, *badger.Item) bool) *FilterIterator[T] {
	return &FilterIterator[T]{base: base, f: f}
}

// Close implements the Iterator interface.
func (it *FilterIterator[T]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *FilterIterator[T]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *FilterIterator[T]) Next() {
	it.base.Next()
	it.findNext()
}

func (it *FilterIterator[T]) findNext() {
	for it.base.Valid() {
		v, err := it.Value()
		if err != nil {
			return
		}
		if it.f(v, it.Item()) {
			return
		}
		it.base.Next()
	}
}

// Rewind implements the Iterator interface.
func (it *FilterIterator[T]) Rewind() {
	it.base.Rewind()
	it.findNext()
}

// Seek implements the Iterator interface.
func (it *FilterIterator[T]) Seek(key []byte) {
	it.base.Seek(key)
	it.findNext()
}

// Valid implements the Iterator interface.
func (it *FilterIterator[T]) Valid() bool {
	if !it.base.Valid() {
		return false
	}

	v, err := it.Value()
	if err != nil {
		return false
	}
	return it.f(v, it.Item())
}

// Key implements the Iterator interface.
func (it *FilterIterator[T]) Key() []byte {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *FilterIterator[T]) Value() (value T, err error) {
	return it.base.Value()
}
