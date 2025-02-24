package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// MapIterator is an iterator that maps the value from T to U.
type MapIterator[KA, VA, KB, VB any] struct {
	base     badgerutils.Iterator[KA, VA]
	f        func(KA, VA, *badger.Item) (KB, VB, error)
	cacheKey *KB
	cacheVal *VB
	err      error
}

// Map creates a new map iterator.
func Map[KA, VA, KB, VB any](base badgerutils.Iterator[KA, VA], f func(KA, VA, *badger.Item) (KB, VB, error)) *MapIterator[KA, VA, KB, VB] {
	return &MapIterator[KA, VA, KB, VB]{base: base, f: f}
}

// Close implements the Iterator interface.
func (it *MapIterator[KA, VA, KB, VB]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *MapIterator[KA, VA, KB, VB]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *MapIterator[KA, VA, KB, VB]) Next() {
	it.cacheKey = nil
	it.cacheVal = nil
	it.base.Next()
}

// Rewind implements the Iterator interface.
func (it *MapIterator[KA, VA, KB, VB]) Rewind() {
	it.base.Rewind()
}

// Seek implements the Iterator interface.
func (it *MapIterator[KA, VA, KB, VB]) Seek(key []byte) {
	it.base.Seek(key)
}

// Valid implements the Iterator interface.
func (it *MapIterator[KA, VA, KB, VB]) Valid() bool {
	return it.base.Valid()
}

// Key implements the Iterator interface.
func (it *MapIterator[KA, VA, KB, VB]) Key() KB {
	if it.cacheKey != nil {
		return *it.cacheKey
	}

	it.process()
	return *it.cacheKey
}

func (it *MapIterator[KA, VA, KB, VB]) process() {
	va, err := it.base.Value()
	if err != nil {
		it.err = err
		return
	}

	kb, vb, err := it.f(it.base.Key(), va, it.base.Item())
	if err != nil {
		it.err = err
		return
	}

	it.cacheKey = &kb
	it.cacheVal = &vb
}

// Value implements the Iterator interface.
func (it *MapIterator[KA, VA, KB, VB]) Value() (value VB, err error) {
	if it.cacheVal != nil {
		return *it.cacheVal, nil
	}

	it.process()
	return *it.cacheVal, it.err
}
