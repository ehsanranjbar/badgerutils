package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// JoinIterator is an iterator that joins two iterators by
// pairing the respective items and values and mapping them to a new type using the given custom function.
// Note that Item() will return nil and Seek() is not implemented for this iterator because of nature of the join operation.
type JoinIterator[A, B, C any] struct {
	iterA badgerutils.Iterator[A]
	iterB badgerutils.Iterator[B]
	f     func(A, *badger.Item, B, *badger.Item) (C, bool)
	value C
	err   error
}

// Join creates a new JoinIterator.
func Join[A, B, C any](iterA badgerutils.Iterator[A], iterB badgerutils.Iterator[B], f func(A, *badger.Item, B, *badger.Item) (C, bool)) *JoinIterator[A, B, C] {
	return &JoinIterator[A, B, C]{iterA: iterA, iterB: iterB, f: f}
}

// Close implements the Iterator interface.
func (it *JoinIterator[A, B, C]) Close() {
	it.iterA.Close()
	it.iterB.Close()
}

// Item implements the Iterator interface.
func (it *JoinIterator[A, B, C]) Item() *badger.Item {
	return nil
}

// Next implements the Iterator interface.
func (it *JoinIterator[A, B, C]) Next() {
	it.iterA.Next()
	it.iterB.Next()
}

// Rewind implements the Iterator interface.
func (it *JoinIterator[A, B, C]) Rewind() {
	it.iterA.Rewind()
	it.iterB.Rewind()
}

// Seek implements the Iterator interface.
func (it *JoinIterator[A, B, C]) Seek(key []byte) {
	panic("not implemented")
}

// Valid implements the Iterator interface.
func (it *JoinIterator[A, B, C]) Valid() bool {
	aValid := it.iterA.Valid()
	bValid := it.iterB.Valid()
	if !aValid && !bValid {
		return false
	}

	var (
		aVal         A
		bVal         B
		aItem, bItem *badger.Item
	)
	if aValid {
		aVal, it.err = it.iterA.Value()
		if it.err != nil {
			return false
		}
		aItem = it.iterA.Item()
	}
	if bValid {
		bVal, it.err = it.iterB.Value()
		if it.err != nil {
			return false
		}
		bItem = it.iterB.Item()
	}

	c, ok := it.f(aVal, aItem, bVal, bItem)
	it.value = c
	return ok
}

// Value implements the Iterator interface.
func (it *JoinIterator[A, B, C]) Value() (value C, err error) {
	return it.value, it.err
}

// Union is a type that holds two values of different types.
type Union[T, U any] struct {
	T T
	U U
}

func UnionJoinFunc[T, U any](a T, aItem *badger.Item, b U, bItem *badger.Item) (Union[T, U], bool) {
	return Union[T, U]{T: a, U: b}, true
}
