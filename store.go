package badgerutils

import (
	badger "github.com/dgraph-io/badger/v4"
)

// Store is a store that works with serialized values.
type Store[T any] interface {
	Delete(key []byte) error
	Get(key []byte) (v *T, err error)
	NewIterator(opts badger.IteratorOptions) Iterator[*T]
	Set(key []byte, value *T) error
}

// BadgerStore is a store that *badger.Txn is compatible with it
// and has the SetEntry operation which is mandatory for some low-level stores like PrefixStore and SerializedStore.
type BadgerStore interface {
	Delete(key []byte) error
	Get(key []byte) (v *badger.Item, err error)
	NewIterator(opts badger.IteratorOptions) *badger.Iterator
	Set(key, value []byte) error
	SetEntry(e *badger.Entry) error
}
