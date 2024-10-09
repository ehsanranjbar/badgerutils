package iters

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// SeverIterator is an iterator that severs (stops) the iteration when the predicate is true.
type SeverIterator[T any] struct {
	base    badgerutils.Iterator[T]
	f       func(T, *badger.Item) bool
	severed bool
}

// Sever creates a new sever iterator.
func Sever[T any](base badgerutils.Iterator[T], f func(T, *badger.Item) bool) *SeverIterator[T] {
	return &SeverIterator[T]{base: base, f: f}
}

// Close implements the Iterator interface.
func (it *SeverIterator[T]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *SeverIterator[T]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *SeverIterator[T]) Next() {
	if it.severed {
		return
	}
	it.base.Next()
	if it.base.Valid() && !it.severed {
		it.checkSevered()
	}
}

func (it *SeverIterator[T]) checkSevered() {
	value, err := it.base.Value()
	if err != nil || it.f(value, it.base.Item()) {
		it.severed = true
		return
	}

	it.severed = false
}

// Rewind implements the Iterator interface.
func (it *SeverIterator[T]) Rewind() {
	it.base.Rewind()
	it.checkSevered()
}

// Seek implements the Iterator interface.
func (it *SeverIterator[T]) Seek(key []byte) {
	it.base.Seek(key)
	it.checkSevered()
}

// Valid implements the Iterator interface.
func (it *SeverIterator[T]) Valid() bool {
	return it.base.Valid() && !it.severed
}

// Key implements the Iterator interface.
func (it *SeverIterator[T]) Key() []byte {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *SeverIterator[T]) Value() (value T, err error) {
	if it.severed {
		return value, fmt.Errorf("unable to return value in a severed iterator")
	}

	return it.base.Value()
}
