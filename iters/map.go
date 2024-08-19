package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// MapIterator is an iterator that maps the value from T to U.
type MapIterator[T, U any] struct {
	base badgerutils.Iterator[T]
	f    func(T, *badger.Item) (U, error)
}

// Map creates a new map iterator.
func Map[T, U any](base badgerutils.Iterator[T], f func(T, *badger.Item) (U, error)) *MapIterator[T, U] {
	return &MapIterator[T, U]{base: base, f: f}
}

// Close implements the Iterator interface.
func (it *MapIterator[T, U]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *MapIterator[T, U]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *MapIterator[T, U]) Next() {
	it.base.Next()
}

// Rewind implements the Iterator interface.
func (it *MapIterator[T, U]) Rewind() {
	it.base.Rewind()
}

// Seek implements the Iterator interface.
func (it *MapIterator[T, U]) Seek(key []byte) {
	it.base.Seek(key)
}

// Valid implements the Iterator interface.
func (it *MapIterator[T, U]) Valid() bool {
	return it.base.Valid()
}

// Value implements the Iterator interface.
func (it *MapIterator[T, U]) Value() (value U, err error) {
	v, err := it.base.Value()
	if err != nil {
		return value, err
	}
	return it.f(v, it.base.Item())
}
