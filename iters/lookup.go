package iters

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// Lookup creates a new lookup iterator that will retrieve references from an Iterator[*Ref]
// and return the actual values given a Store[*T].
func Lookup[IK, K, V any](
	store Getter[K, V],
	iter badgerutils.Iterator[IK, K],
) *MapIterator[IK, K, K, *V] {
	return Map(iter, func(ik IK, k K, _ *badger.Item) (K, *V, error) {
		v, err := store.Get(k)
		if err != nil {
			return k, nil, err
		}

		return k, v, nil
	})
}

// Getter is a constraint for gives a value V given a key K.
type Getter[K, V any] interface {
	Get(key K) (*V, error)
}
