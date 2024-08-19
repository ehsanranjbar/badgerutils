package iters

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// Lookup creates a new lookup iterator that will retrieve references from an Iterator[*Ref]
// and return the actual values given a Store[*T].
func Lookup[K, V any](
	store Getter[K, V],
	iter badgerutils.Iterator[K],
) *MapIterator[K, *V] {
	return Map(iter, func(k K, _ *badger.Item) (*V, error) {
		return store.Get(k)
	})
}

// Getter is a constraint for gives a value V given a key K.
type Getter[K, V any] interface {
	Get(key K) (*V, error)
}
