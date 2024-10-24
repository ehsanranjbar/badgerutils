package object

import (
	"encoding"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec"
	"github.com/ehsanranjbar/badgerutils/extensions"
)

// Iterator is an iterator that unmarshal id, data and optionally fetch metadata.
type Iterator[I any, D encoding.BinaryMarshaler] struct {
	base      badgerutils.Iterator[*D]
	idCodec   codec.Codec[I]
	metaStore *extensions.AssociateStore[D, extensions.Metadata, *extensions.Metadata]
	fetchMeta bool
}

func newIterator[I any, D encoding.BinaryMarshaler](
	base badgerutils.Iterator[*D],
	idCodec codec.Codec[I],
	metaStore *extensions.AssociateStore[D, extensions.Metadata, *extensions.Metadata],
	fetchMeta bool,
) *Iterator[I, D] {
	return &Iterator[I, D]{
		base:      base,
		idCodec:   idCodec,
		metaStore: metaStore,
		fetchMeta: fetchMeta,
	}
}

// FetchMeta sets if the iterator should fetch metadata.
func (it *Iterator[I, D]) FetchMeta(b bool) {
	it.fetchMeta = b
}

// Close closes the iterator.
func (it *Iterator[I, D]) Close() {
	it.base.Close()
}

// Item returns the current item.
func (it *Iterator[I, D]) Item() *badger.Item {
	return it.base.Item()
}

// Next moves to the next item.
func (it *Iterator[I, D]) Next() {
	it.base.Next()
}

// Rewind rewinds the iterator.
func (it *Iterator[I, D]) Rewind() {
	it.base.Rewind()
}

// Seek seeks the key.
func (it *Iterator[I, D]) Seek(key I) {
	keyBytes, err := it.idCodec.Encode(key)
	if err != nil {
		panic(err)
	}

	it.base.Seek(keyBytes)
}

// Valid returns if the iterator is valid.
func (it *Iterator[I, D]) Valid() bool {
	return it.base.Valid()
}

// Key returns the current key.
func (it *Iterator[I, D]) Key() I {
	keyBytes := it.base.Key()
	key, err := it.idCodec.Decode(keyBytes)
	if err != nil {
		panic(err)
	}

	return key
}

// Value returns the current value.
func (it *Iterator[I, D]) Value() (*Object[I, D], error) {
	data, err := it.base.Value()
	if err != nil {
		return nil, err
	}
	id := it.Key()
	obj := &Object[I, D]{
		ID:   &id,
		Data: *data,
	}

	if it.fetchMeta {
		meta, err := it.metaStore.Get(it.base.Key())
		if err != nil {
			return nil, err
		}

		if meta != nil {
			obj.Metadata = *meta
		}
	}

	return obj, nil
}
