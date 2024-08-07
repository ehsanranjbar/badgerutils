package badgerutils

import (
	badger "github.com/dgraph-io/badger/v4"
)

// Store is the generalized interface that represents a key-value store with get, set, delete and iterate operations.
type Store interface {
	Delete(key []byte) error
	Get(key []byte) (item *badger.Item, err error)
	NewIterator(opts badger.IteratorOptions) *badger.Iterator
	Set(key, value []byte) error
	SetEntry(e *badger.Entry) error
}
