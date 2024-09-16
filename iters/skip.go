package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// SkipIterator is an iterator that skips the first n items on Rewind and Seek.
type SkipIterator[T any] struct {
	base badgerutils.Iterator[T]
	n    int
}

// Skip creates a new skip iterator.
func Skip[T any](base badgerutils.Iterator[T], n int) *SkipIterator[T] {
	return &SkipIterator[T]{base: base, n: n}
}

// Close implements the Iterator interface.
func (it *SkipIterator[T]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *SkipIterator[T]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *SkipIterator[T]) Next() {
	it.base.Next()
}

// Rewind implements the Iterator interface.
func (it *SkipIterator[T]) Rewind() {
	it.base.Rewind()
	it.skip()
}

func (it *SkipIterator[T]) skip() {
	for i := 0; i < it.n && it.Valid(); i++ {
		it.base.Next()
	}
}

// Seek implements the Iterator interface.
func (it *SkipIterator[T]) Seek(key []byte) {
	it.base.Seek(key)
	it.skip()
}

// Valid implements the Iterator interface.
func (it *SkipIterator[T]) Valid() bool {
	return it.base.Valid()
}

// Key implements the Iterator interface.
func (it *SkipIterator[T]) Key() []byte {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *SkipIterator[T]) Value() (value T, err error) {
	return it.base.Value()
}
