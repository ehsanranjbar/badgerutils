package prefix

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// Iterator is an iterator trims the prefix from the key.
type Iterator struct {
	base   badgerutils.BadgerIterator
	prefix []byte
}

// NewIterator creates a new iterator.
func NewIterator(base badgerutils.BadgerIterator, prefix []byte) *Iterator {
	return &Iterator{
		base:   base,
		prefix: prefix,
	}
}

// NewIteratorFromStore creates a new iterator from a prefix store.
func NewIteratorFromStore(store *Store) *Iterator {
	return NewIterator(store.NewIterator(badger.DefaultIteratorOptions), store.Prefix())
}

// Close closes the iterator.
func (it *Iterator) Close() {
	it.base.Close()
}

// Item returns the current item.
func (it *Iterator) Item() *badger.Item {
	return it.base.Item()
}

// Next moves to the next item.
func (it *Iterator) Next() {
	it.base.Next()
}

// Rewind rewinds the iterator.
func (it *Iterator) Rewind() {
	it.base.Rewind()
}

// Seek seeks the key.
func (it *Iterator) Seek(key []byte) {
	it.base.Seek(append(it.prefix, key...))
}

// Valid returns if the iterator is valid.
func (it *Iterator) Valid() bool {
	return it.base.Valid()
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	return it.base.Item().Key()[len(it.prefix):]
}
