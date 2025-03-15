package serialized

import (
	"encoding"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
)

// BSP is an interface for pointer of T that is binary serializable/deserializable.
type BSP[T any] interface {
	*T
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type Store[T any, PT BSP[T]] struct {
	base   badgerutils.Instantiator[badgerutils.BadgerStore]
	prefix []byte
}

// New creates a new Store.
func New[
	T any,
	PT BSP[T],
](base badgerutils.Instantiator[badgerutils.BadgerStore]) *Store[T, PT] {
	var prefix []byte
	if pfx, ok := base.(prefixed); ok {
		prefix = pfx.Prefix()
	}

	return &Store[T, PT]{
		base:   base,
		prefix: prefix,
	}
}

type prefixed interface {
	Prefix() []byte
}

// Prefix returns the prefix of the store.
func (s *Store[T, PT]) Prefix() []byte {
	return s.prefix
}

// Instantiate creates a new Instance.
func (s *Store[T, PT]) Instantiate(txn *badger.Txn) badgerutils.StoreInstance[[]byte, *T, *T, badgerutils.Iterator[[]byte, *T]] {
	var base badgerutils.BadgerStore = txn
	if s.base != nil {
		base = s.base.Instantiate(txn)
	}

	return &Instance[T, PT]{
		base:   base,
		prefix: s.prefix,
	}
}

// Instance is a store that serializes all keys and values.
type Instance[T any, PT BSP[T]] struct {
	base   badgerutils.BadgerStore
	prefix []byte
}

// Prefix returns the prefix of the store.
func (s *Instance[T, PT]) Prefix() []byte {
	return s.prefix
}

// Delete deletes the key from the store.
func (s *Instance[T, PT]) Delete(key []byte) error {
	return s.base.Delete(key)
}

// Get gets the value of the key from the store and unmarshal it.
func (s *Instance[T, PT]) Get(key []byte) (value *T, err error) {
	_, value, err = s.GetWithItem(key)
	return value, err
}

// NewIterator creates a new iterator.
func (s *Instance[T, PT]) NewIterator(opts badger.IteratorOptions) badgerutils.Iterator[[]byte, *T] {
	var iter badgerutils.BadgerIterator = s.base.NewIterator(opts)
	if pfx := s.Prefix(); pfx != nil {
		iter = pstore.NewIterator(iter, pfx)
	}

	return NewIterator[T, PT](iter)
}

// GetWithItem is similar to Get, but it also returns the badger.Item as well.
func (s *Instance[T, PT]) GetWithItem(key []byte) (item *badger.Item, value *T, err error) {
	item, err = s.base.Get(key)
	if err != nil {
		return nil, nil, err
	}
	v := PT(new(T))
	err = item.Value(func(val []byte) error {
		return v.UnmarshalBinary(val)
	})
	return item, (*T)(v), err
}

// Set marshals the value as binary and sets it to the key.
func (s *Instance[T, PT]) Set(key []byte, value *T) error {
	var (
		data []byte
		err  error
	)
	if value != nil {
		data, err = PT(value).MarshalBinary()
		if err != nil {
			return err
		}
	}
	return s.base.Set(key, data)
}
