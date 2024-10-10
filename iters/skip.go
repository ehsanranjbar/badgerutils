package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// SkipIterator is an iterator that skips first items that doesn't satisfy the condition.
type SkipIterator[T, S any] struct {
	base  badgerutils.Iterator[T]
	state S
	f     func(S, []byte, T, *badger.Item) (S, bool)
}

// SkipN creates a new skip iterator that skips the first n items.
func SkipN[T any](base badgerutils.Iterator[T], n int) *SkipIterator[T, int] {
	return Skip[T, int](base, func(s int, k []byte, v T, item *badger.Item) (int, bool) {
		return s + 1, s < n
	})
}

// Skip creates a new skip iterator.
func Skip[T any, S any](base badgerutils.Iterator[T], f func(S, []byte, T, *badger.Item) (S, bool)) *SkipIterator[T, S] {
	return &SkipIterator[T, S]{base: base, f: f}
}

// Close implements the Iterator interface.
func (it *SkipIterator[T, S]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *SkipIterator[T, S]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *SkipIterator[T, S]) Next() {
	it.base.Next()
}

// Rewind implements the Iterator interface.
func (it *SkipIterator[T, S]) Rewind() {
	it.base.Rewind()
	it.skip()
}

func (it *SkipIterator[T, S]) skip() {
	for it.base.Valid() {
		k := it.base.Key()
		v, _ := it.base.Value()
		s, ok := it.f(it.state, k, v, it.base.Item())
		if !ok {
			break
		}

		it.state = s
		it.base.Next()
	}
}

// Seek implements the Iterator interface.
func (it *SkipIterator[T, S]) Seek(key []byte) {
	it.base.Seek(key)
	it.skip()
}

// Valid implements the Iterator interface.
func (it *SkipIterator[T, S]) Valid() bool {
	return it.base.Valid()
}

// Key implements the Iterator interface.
func (it *SkipIterator[T, S]) Key() []byte {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *SkipIterator[T, S]) Value() (value T, err error) {
	return it.base.Value()
}
