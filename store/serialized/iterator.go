package serialized

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

// Iterator is an iterator that unmarshal the value.
type Iterator[T any, PT BSP[T]] struct {
	base        badgerutils.BadgerIterator
	keyProvider keyProvider
	cachedValue *T
}

type keyProvider interface {
	Key() []byte
}

// NewIterator creates a new serialized iterator.
func NewIterator[T any, PT BSP[T]](base badgerutils.BadgerIterator) *Iterator[T, PT] {
	kp, _ := base.(keyProvider)

	return &Iterator[T, PT]{
		base:        base,
		keyProvider: kp,
	}
}

// Close closes the iterator.
func (it *Iterator[T, PT]) Close() {
	it.base.Close()
}

// Item returns the current item.
func (it *Iterator[T, PT]) Item() *badger.Item {
	return it.base.Item()
}

// Key returns the current key.
func (it *Iterator[T, PT]) Next() {
	it.base.Next()
	it.cachedValue = nil
}

// Rewind rewinds the iterator.
func (it *Iterator[T, PT]) Rewind() {
	it.base.Rewind()
	it.cachedValue = nil
}

// Seek seeks the key.
func (it *Iterator[T, PT]) Seek(key []byte) {
	it.base.Seek(key)
	it.cachedValue = nil
}

// Valid returns if the iterator is valid.
func (it *Iterator[T, PT]) Valid() bool {
	return it.base.Valid()
}

// Key returns the current key.
func (it *Iterator[T, PT]) Key() []byte {
	if it.keyProvider == nil {
		return it.base.Item().Key()
	}

	return it.keyProvider.Key()
}

// Value returns the current value unmarshaled as T
func (it *Iterator[T, PT]) Value() (value *T, err error) {
	if it.cachedValue != nil {
		return it.cachedValue, nil
	}

	item := it.base.Item()
	if item == nil {
		return nil, nil
	}
	v := PT(new(T))
	err = item.Value(func(val []byte) error {
		if len(val) == 0 {
			return nil
		}
		return v.UnmarshalBinary(val)
	})
	it.cachedValue = (*T)(v)
	return it.cachedValue, err
}
