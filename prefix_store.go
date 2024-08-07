package badgerutils

import (
	badger "github.com/dgraph-io/badger/v4"
)

// PrefixStore is a store that prefixes all keys with a given prefix.
type PrefixStore struct {
	base   Store
	prefix []byte
}

// NewPrefixStore creates a new PrefixStore.
func NewPrefixStore(store Store, prefix string) *PrefixStore {
	return &PrefixStore{
		base:   store,
		prefix: []byte(prefix),
	}
}

// Delete deletes the key from the store.
func (s *PrefixStore) Delete(key []byte) error {
	return s.base.Delete(append(s.prefix, key...))
}

// Get gets the key from the store.
func (s *PrefixStore) Get(key []byte) (*badger.Item, error) {
	return s.base.Get(append(s.prefix, key...))
}

// Iterate iterates over the store.
func (s *PrefixStore) NewIterator(opts badger.IteratorOptions) *badger.Iterator {
	return s.base.NewIterator(badger.IteratorOptions{
		Prefix: append(s.prefix, opts.Prefix...),
	})
}

// Set sets the key in the store.
func (s *PrefixStore) Set(key, value []byte) error {
	return s.base.Set(append(s.prefix, key...), value)
}

// SetEntry sets the entry in the store.
func (s *PrefixStore) SetEntry(e *badger.Entry) error {
	e.Key = append(s.prefix, e.Key...)
	return s.base.SetEntry(e)
}
