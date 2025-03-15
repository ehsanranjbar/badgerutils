package iters

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// LookupIterator is an iterator that looks up the value from a store.
type LookupIterator[IK, K, V any] struct {
	base   badgerutils.Iterator[IK, K]
	getter Getter[K, V]
	// TODO: Checkout if caching the key and value is a good idea.
	cacheKey   *K
	cacheValue *V
}

// Getter is a constraint for gives a value V given a key K.
type Getter[K, V any] interface {
	Get(key K) (*V, error)
}

// Lookup creates a new lookup iterator.
func Lookup[IK, K, V any](
	store Getter[K, V],
	iter badgerutils.Iterator[IK, K],
) *LookupIterator[IK, K, V] {
	return &LookupIterator[IK, K, V]{
		base:       iter,
		getter:     store,
		cacheKey:   nil,
		cacheValue: nil,
	}
}

// Close implements the Iterator interface.
func (it *LookupIterator[IK, K, V]) Close() {
	it.base.Close()
}

// Item implements the Iterator interface.
func (it *LookupIterator[IK, K, V]) Item() *badger.Item {
	return it.base.Item()
}

// Next implements the Iterator interface.
func (it *LookupIterator[IK, K, V]) Next() {
	it.cacheKey = nil
	it.cacheValue = nil
	it.base.Next()
}

// Rewind implements the Iterator interface.
func (it *LookupIterator[IK, K, V]) Rewind() {
	it.base.Rewind()
}

// Seek implements the Iterator interface.
func (it *LookupIterator[IK, K, V]) Seek(key []byte) {
	it.base.Seek(key)
}

// Valid implements the Iterator interface.
func (it *LookupIterator[IK, K, V]) Valid() bool {
	return it.base.Valid()
}

// Key implements the Iterator interface.
func (it *LookupIterator[IK, K, V]) Key() K {
	if it.cacheKey != nil {
		return *it.cacheKey
	}

	k, err := it.base.Value()
	if err != nil {
		panic(err)
	}
	it.cacheKey = &k

	return k
}

// Value implements the Iterator interface.
func (it *LookupIterator[IK, K, V]) Value() (value *V, err error) {
	if it.cacheValue != nil {
		return it.cacheValue, nil
	}

	v, err := it.getter.Get(it.Key())
	if err != nil {
		return value, err
	}
	it.cacheValue = v

	return v, nil
}
