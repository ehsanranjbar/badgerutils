package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// FlattenIterator is an iterator that flattens a set of iterators into a single iterator.
type FlattenIterator[T any] struct {
	base    badgerutils.Iterator[badgerutils.Iterator[T]]
	current badgerutils.Iterator[T]
	err     error
}

// Flatten creates a new flatten iterator.
func Flatten[T any](base badgerutils.Iterator[badgerutils.Iterator[T]]) *FlattenIterator[T] {
	return &FlattenIterator[T]{base: base}
}

// Close implements the Iterator interface.
func (it *FlattenIterator[T]) Close() {
	it.current.Close()
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *FlattenIterator[T]) Item() *badger.Item {
	if it.current == nil {
		return nil
	}

	return it.current.Item()
}

// Next implements the Iterator interface.
func (it *FlattenIterator[T]) Next() {
	if it.current != nil {
		it.current.Next()
		if it.current.Valid() {
			return
		}
		it.current.Close()
	}

	it.base.Next()
	if it.base.Valid() {
		it.current, it.err = it.base.Value()
		it.current.Rewind()
	}
}

// Rewind implements the Iterator interface.
func (it *FlattenIterator[T]) Rewind() {
	it.base.Rewind()
	if it.base.Valid() {
		if it.current != nil {
			it.current.Close()
		}

		it.current, it.err = it.base.Value()
		it.current.Rewind()
	}
}

// Seek implements the Iterator interface.
func (it *FlattenIterator[T]) Seek(key []byte) {
	it.base.Seek(key)
	if it.base.Valid() {
		if it.current != nil {
			it.current.Close()
		}

		it.current, it.err = it.base.Value()
		it.current.Rewind()
	}
}

// Valid implements the Iterator interface.
func (it *FlattenIterator[T]) Valid() bool {
	return it.base.Valid() || (it.current != nil && it.current.Valid())
}

// Value returns the current value of the iterator.
func (it *FlattenIterator[T]) Value() (value T, err error) {
	if it.err != nil {
		return value, it.err
	}

	if it.current != nil {
		return it.current.Value()
	}

	return value, nil
}
