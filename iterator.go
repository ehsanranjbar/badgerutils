package badgerutils

import (
	badger "github.com/dgraph-io/badger/v4"
)

// Iterator is an interface that extends ValueIterator with a Key method.
type Iterator[K, V any] interface {
	ValueIterator[V]
	Key() K
}

// ValueIterator is an interface that extends BadgerIterator with a Value method.
type ValueIterator[V any] interface {
	BadgerIterator
	Value() (value V, err error)
}

// BadgerIterator is the interface that represents a badger iterator.
type BadgerIterator interface {
	Close()
	Item() *badger.Item
	Next()
	Rewind()
	Seek(key []byte)
	Valid() bool
}
