package serialized

import (
	"encoding"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// BinarySerializable is a serialized item.
type BinarySerializable interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// Store is a store that serializes all keys and values.
type Store[T any, PT interface {
	BinarySerializable
	*T
}] struct {
	base badgerutils.BadgerStore
}

// New creates a new serialized store.
func New[T any, PT interface {
	BinarySerializable
	*T
}](base badgerutils.BadgerStore) *Store[T, PT] {
	return &Store[T, PT]{base: base}
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
	return NewSerializedIterator[T, PT](s.base.NewIterator(opts))
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
		data, err = (PT)(value).MarshalBinary()
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

// SerializedIterator is an iterator that unmarshal the value.
type SerializedIterator[T any, PT interface {
	BinarySerializable
	*T
}] struct {
	base        *badger.Iterator
	cachedValue *T
}

// NewSerializedIterator creates a new serialized iterator.
func NewSerializedIterator[T any, PT interface {
	BinarySerializable
	*T
}](base *badger.Iterator) *SerializedIterator[T, PT] {
	return &SerializedIterator[T, PT]{base: base}
}

// Close closes the iterator.
func (it *SerializedIterator[T, PT]) Close() {
	it.base.Close()
}

// Item returns the current item.
func (it *SerializedIterator[T, PT]) Item() *badger.Item {
	return it.base.Item()
}

// Key returns the current key.
func (it *SerializedIterator[T, PT]) Next() {
	it.base.Next()
	it.cachedValue = nil
}

// Rewind rewinds the iterator.
func (it *SerializedIterator[T, PT]) Rewind() {
	it.base.Rewind()
	it.cachedValue = nil
}

// Seek seeks the key.
func (it *SerializedIterator[T, PT]) Seek(key []byte) {
	it.base.Seek(key)
	it.cachedValue = nil
}

// Valid returns if the iterator is valid.
func (it *SerializedIterator[T, PT]) Valid() bool {
	return it.base.Valid()
}

// Value returns the current value unmarshaled as T
func (it *SerializedIterator[T, PT]) Value() (value *T, err error) {
	if it.cachedValue != nil {
		return it.cachedValue, nil
	}

	item := it.base.Item()
	if item == nil {
		return nil, nil
	}
	v := PT(new(T))
	err = item.Value(func(val []byte) error {
		return v.UnmarshalBinary(val)
	})
	it.cachedValue = (*T)(v)
	return it.cachedValue, err
}
