package iters

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// LookupIterator is an iterator that will retrieve references from an Iterator[*Ref]
// and return the actual values given a Store[*T].
type LookupIterator[T any] struct {
	store badgerutils.Store[T]
	iter  badgerutils.Iterator[[]byte]
}

// Lookup creates a new lookup iterator.
func Lookup[T any](
	store badgerutils.Store[T],
	iter badgerutils.Iterator[[]byte],
) *LookupIterator[T] {
	return &LookupIterator[T]{store: store, iter: iter}
}

// Close implements the Iterator interface.
func (it *LookupIterator[T]) Close() {
	it.iter.Close()
}

// Item implements the Iterator interface.
func (it *LookupIterator[T]) Item() *badger.Item {
	return it.iter.Item()
}

// Next implements the Iterator interface.
func (it *LookupIterator[T]) Next() {
	it.iter.Next()
}

// Rewind implements the Iterator interface.
func (it *LookupIterator[T]) Rewind() {
	it.iter.Rewind()
}

// Seek implements the Iterator interface.
func (it *LookupIterator[T]) Seek(key []byte) {
	it.iter.Seek(key)
}

// Valid implements the Iterator interface.
func (it *LookupIterator[T]) Valid() bool {
	return it.iter.Valid()
}

// Value returns the actual value of the reference.
func (it *LookupIterator[T]) Value() (*T, error) {
	ref, err := it.iter.Value()
	if err != nil {
		return nil, err
	}
	return it.store.Get(ref)
}
