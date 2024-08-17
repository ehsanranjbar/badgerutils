package badgerutils

import (
	badger "github.com/dgraph-io/badger/v4"
)

// Iterator has the most common methods for *badger.Iterator along with a Value method
// which is used in pretty much all high-level stuff of this package because we mostly work with serialized values.
type Iterator[T any] interface {
	BadgerIterator
	Value() (value T, err error)
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
