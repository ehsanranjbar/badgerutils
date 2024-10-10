package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// FlattenIterator is an iterator that flattens a set of iterators into a single iterator.
type FlattenIterator[T any] struct {
	base    badgerutils.Iterator[badgerutils.Iterator[T]]
	current badgerutils.Iterator[T]
}

// Flatten creates a new flatten iterator.
func Flatten[T any](base badgerutils.Iterator[badgerutils.Iterator[T]]) *FlattenIterator[T] {
	return &FlattenIterator[T]{base: base}
}

// Close implements the Iterator interface.
func (it *FlattenIterator[T]) Close() {
	if it.current != nil {
		it.current.Close()
	}
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *FlattenIterator[T]) Item() *badger.Item {
	if it.current == nil || !it.current.Valid() {
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
	}

	it.base.Next()
	it.nextCurrent()
}

func (it *FlattenIterator[T]) nextCurrent() {
	for {
		if !it.base.Valid() {
			break
		}

		if it.current != nil {
			it.current.Close()
		}

		var err error
		it.current, err = it.base.Value()
		if err != nil {
			continue
		}

		it.current.Rewind()
		if it.current.Valid() {
			break
		}

		it.base.Next()
	}
}

// Rewind implements the Iterator interface.
func (it *FlattenIterator[T]) Rewind() {
	it.base.Rewind()
	it.nextCurrent()
}

// Seek implements the Iterator interface.
func (it *FlattenIterator[T]) Seek(key []byte) {
	it.base.Seek(key)
	it.nextCurrent()
}

// Valid implements the Iterator interface.
func (it *FlattenIterator[T]) Valid() bool {
	return it.base.Valid() || (it.current != nil && it.current.Valid())
}

// Key implements the Iterator interface.
func (it *FlattenIterator[T]) Key() []byte {
	if it.current != nil {
		return it.current.Key()
	}

	return nil
}

// Value returns the current value of the iterator.
func (it *FlattenIterator[T]) Value() (value T, err error) {
	if it.current != nil {
		return it.current.Value()
	}

	return value, nil
}
