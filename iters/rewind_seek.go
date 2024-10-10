package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// RewindSeekIterator is an iterator that can seeks to the specified key on rewind.
type RewindSeekIterator[T any] struct {
	base badgerutils.Iterator[T]
	key  []byte
}

// RewindSeek creates a new rewind seek iterator.
func RewindSeek[T any](base badgerutils.Iterator[T], key []byte) *RewindSeekIterator[T] {
	return &RewindSeekIterator[T]{base: base, key: key}
}

// Close implements the Iterator interface.
func (it *RewindSeekIterator[T]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *RewindSeekIterator[T]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *RewindSeekIterator[T]) Next() {
	it.base.Next()
}

// Rewind implements the Iterator interface.
func (it *RewindSeekIterator[T]) Rewind() {
	it.base.Seek(it.key)
}

// Seek implements the Iterator interface.
func (it *RewindSeekIterator[T]) Seek(key []byte) {
	it.base.Seek(key)
}

// Valid implements the Iterator interface.
func (it *RewindSeekIterator[T]) Valid() bool {
	return it.base.Valid()
}

// Key implements the Iterator interface.
func (it *RewindSeekIterator[T]) Key() []byte {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *RewindSeekIterator[T]) Value() (T, error) {
	return it.base.Value()
}
