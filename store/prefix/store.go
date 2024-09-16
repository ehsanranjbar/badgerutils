package prefix

import (
	badger "github.com/dgraph-io/badger/v4"
	badgerutils "github.com/ehsanranjbar/badgerutils"
)

// Store is a store that prefixes all keys with a given prefix.
type Store struct {
	base   badgerutils.BadgerStore
	prefix []byte
}

// New creates a new PrefixStore.
func New(store badgerutils.BadgerStore, prefix []byte) *Store {
	return &Store{
		base:   store,
		prefix: prefix,
	}
}

// Prefix returns the prefix of the store.
func (s *Store) Prefix() []byte {
	if pfx, ok := s.base.(prefixed); ok {
		return append(pfx.Prefix(), s.prefix...)
	}
	return s.prefix
}

type prefixed interface {
	Prefix() []byte
}

// Delete deletes the key from the store.
func (s *Store) Delete(key []byte) error {
	return s.base.Delete(append(s.prefix, key...))
}

// Get gets the key from the store.
func (s *Store) Get(key []byte) (*badger.Item, error) {
	return s.base.Get(append(s.prefix, key...))
}

// Iterate iterates over the store.
func (s *Store) NewIterator(opts badger.IteratorOptions) *badger.Iterator {
	return s.base.NewIterator(badger.IteratorOptions{
		Prefix: append(s.prefix, opts.Prefix...),
	})
}

// Set sets the key in the store.
func (s *Store) Set(key, value []byte) error {
	return s.base.Set(append(s.prefix, key...), value)
}

// SetEntry sets the entry in the store.
func (s *Store) SetEntry(e *badger.Entry) error {
	e.Key = append(s.prefix, e.Key...)
	return s.base.SetEntry(e)
}
