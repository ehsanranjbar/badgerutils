package ref

import (
	"bytes"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
)

type iterator struct {
	base        badgerutils.BadgerIterator
	keyProvider keyProvider
}

type keyProvider interface {
	Key() []byte
}

func newIterator(base badgerutils.BadgerIterator) *iterator {
	kp, _ := base.(keyProvider)

	return &iterator{
		base:        base,
		keyProvider: kp,
	}
}

// Close implements the Iterator interface
func (i *iterator) Close() {
	i.base.Close()
}

// Item implements the Iterator interface
func (i *iterator) Item() *badger.Item {
	return i.base.Item()
}

// Next implements the Iterator interface
func (i *iterator) Next() {
	i.base.Next()
}

// Rewind implements the Iterator interface
func (i *iterator) Rewind() {
	i.base.Rewind()
}

// Seek implements the Iterator interface
func (i *iterator) Seek(key []byte) {
	i.base.Seek(key)
}

// Valid implements the Iterator interface
func (i *iterator) Valid() bool {
	return i.base.Valid()
}

// Key returns the current key.
func (it *iterator) Key() []byte {
	if it.keyProvider == nil {
		return extractPrefix(it.base.Item().Key(), it.base.Item().UserMeta())
	}

	return extractPrefix(it.keyProvider.Key(), it.base.Item().UserMeta())
}

func extractPrefix(key []byte, keyLen uint8) []byte {
	return key[:len(key)-int(keyLen)]
}

// Value implements the Iterator interface
func (i *iterator) Value() ([]byte, error) {
	item := i.base.Item()
	if item == nil {
		return nil, nil
	}

	key := extractKey(item.Key(), item.UserMeta())
	return bytes.Clone(key), nil
}
