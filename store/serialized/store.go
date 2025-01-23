package serialized

import (
	"encoding"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
)

// PointerBinaryUnmarshaler is an interface that unmarshals a binary data.
type PointerBinaryUnmarshaler[T any] interface {
	encoding.BinaryUnmarshaler
	*T
}

// Store is a store that serializes all keys and values.
type Store[T encoding.BinaryMarshaler, PT PointerBinaryUnmarshaler[T]] struct {
	base   badgerutils.BadgerStore
	prefix []byte
}

// New creates a new serialized store.
func New[T encoding.BinaryMarshaler, PT PointerBinaryUnmarshaler[T]](base badgerutils.BadgerStore) *Store[T, PT] {
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

// Delete deletes the key from the store.
func (s *Store[T, PT]) Delete(key []byte) error {
	return s.base.Delete(key)
}

// Get gets the value of the key from the store and unmarshal it.
func (s *Store[T, PT]) Get(key []byte) (value *T, err error) {
	_, value, err = s.GetWithItem(key)
	return value, err
}

// NewIterator creates a new iterator.
func (s *Store[T, PT]) NewIterator(opts badger.IteratorOptions) badgerutils.Iterator[*T] {
	var iter badgerutils.BadgerIterator = s.base.NewIterator(opts)
	if pfx := s.Prefix(); pfx != nil {
		iter = pstore.NewIterator(iter, pfx)
	}

	return NewIterator[T, PT](iter)
}

// GetWithItem is similar to Get, but it also returns the badger.Item as well.
func (s *Store[T, PT]) GetWithItem(key []byte) (item *badger.Item, value *T, err error) {
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

// TemporaryItem is an item that has a TTL.
type TemporaryItem interface {
	TTL() time.Duration
}

// MetaBearer is an item that has meta byte.
type MetaBearer interface {
	MetaByte() byte
}

// Set marshals the value as binary and sets it to the key.
// If the value implements TemporaryItem, it will set the TTL.
// If the value implements MetaBearer, it will set the meta byte.
func (s *Store[T, PT]) Set(key []byte, value *T) error {
	var (
		data []byte
		err  error
	)
	if value != nil {
		data, err = (*value).MarshalBinary()
		if err != nil {
			return err
		}
	}

	entry := badger.NewEntry(key, data)
	anyValue := any(value)
	if ti, ok := anyValue.(TemporaryItem); ok {
		entry = entry.WithTTL(ti.TTL())
	}
	if md, ok := anyValue.(MetaBearer); ok {
		entry = entry.WithMeta(md.MetaByte())
	}

	return s.base.SetEntry(entry)
}
