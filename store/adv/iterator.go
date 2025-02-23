package adv

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec"
)

var _ badgerutils.Iterator[int64, *struct{}] = (*Iterator[int64, struct{}])(nil)

// Iterator is an iterator that unmarshal id, data and optionally fetch metadata.
type Iterator[I comparable, T any] struct {
	base    badgerutils.Iterator[[]byte, *T]
	idCodec codec.Codec[I]
}

func newIterator[I comparable, T any](
	base badgerutils.Iterator[[]byte, *T],
	idCodec codec.Codec[I],
) *Iterator[I, T] {
	return &Iterator[I, T]{
		base:    base,
		idCodec: idCodec,
	}
}

// Close closes the iterator.
func (it *Iterator[I, T]) Close() {
	it.base.Close()
}

// Item returns the current item.
func (it *Iterator[I, T]) Item() *badger.Item {
	return it.base.Item()
}

// Next moves to the next item.
func (it *Iterator[I, T]) Next() {
	it.base.Next()
}

// Rewind rewinds the iterator.
func (it *Iterator[I, T]) Rewind() {
	it.base.Rewind()
}

// SeekT seeks the key.
func (it *Iterator[I, T]) SeekT(key I) {
	keyBytes, err := it.idCodec.Encode(key)
	if err != nil {
		panic(err)
	}

	it.base.Seek(keyBytes)
}

// Seek seeks the key.
func (it *Iterator[I, T]) Seek(key []byte) {
	it.base.Seek(key)
}

// Valid returns if the iterator is valid.
func (it *Iterator[I, T]) Valid() bool {
	return it.base.Valid()
}

// Key returns the current key.
func (it *Iterator[I, T]) Key() I {
	keyBytes := it.base.Key()
	key, err := it.idCodec.Decode(keyBytes)
	if err != nil {
		panic(err)
	}

	return key
}

// Value returns the current value.
func (it *Iterator[I, T]) Value() (*T, error) {
	v, err := it.base.Value()
	if err != nil {
		return nil, err
	}

	return v, nil
}
