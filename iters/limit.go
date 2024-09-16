package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// LimitIterator is an iterator that limits the number of items when calling Next.
type LimitIterator[T any] struct {
	base badgerutils.Iterator[T]
	n    int
	i    int
}

// Limit creates a new limit iterator.
func Limit[T any](base badgerutils.Iterator[T], n int) *LimitIterator[T] {
	return &LimitIterator[T]{base: base, n: n}
}

// Close implements the Iterator interface.
func (it *LimitIterator[T]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *LimitIterator[T]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *LimitIterator[T]) Next() {
	if it.i < it.n {
		it.base.Next()
		it.i++
	}
}

// Rewind implements the Iterator interface.
func (it *LimitIterator[T]) Rewind() {
	it.base.Rewind()
	it.i = 0
}

// Seek implements the Iterator interface.
func (it *LimitIterator[T]) Seek(key []byte) {
	it.base.Seek(key)
	it.i = 0
}

// Valid implements the Iterator interface.
func (it *LimitIterator[T]) Valid() bool {
	return it.i < it.n && it.base.Valid()
}

// Key implements the Iterator interface.
func (it *LimitIterator[T]) Key() []byte {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *LimitIterator[T]) Value() (value T, err error) {
	return it.base.Value()
}
