package badgerutils

import (
	badger "github.com/dgraph-io/badger/v4"
)

type Instantiator[I any] interface {
	Instantiate(txn *badger.Txn) I
}

// StoreInstance is a store that works with serialized values.
type StoreInstance[K, G, S any, I BadgerIterator] interface {
	Delete(key K) error
	Get(key K) (v G, err error)
	NewIterator(opts badger.IteratorOptions) I
	Set(key K, value S) error
}

// BadgerStore is a store that *badger.Txn is compatible with it
// and has the SetEntry operation which is mandatory for some low-level stores like PrefixStore and SerializedStore.
type BadgerStore interface {
	StoreInstance[[]byte, *badger.Item, []byte, *badger.Iterator]
	SetEntry(e *badger.Entry) error
}
