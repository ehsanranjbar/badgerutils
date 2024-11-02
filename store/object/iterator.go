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
func (it *Iterator[I, D]) FetchMeta(b bool) *Iterator[I, D] {
	it.fetchMeta = b
	return it
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

// SeekT seeks the key.
func (it *Iterator[I, D]) SeekT(key I) {
	keyBytes, err := it.idCodec.Encode(key)
	if err != nil {
		panic(err)
	}

	it.base.Seek(keyBytes)
}

// Seek seeks the key.
func (it *Iterator[I, D]) Seek(key []byte) {
	it.base.Seek(key)
}

// Valid returns if the iterator is valid.
func (it *Iterator[I, D]) Valid() bool {
	return it.base.Valid()
}

// KeyT returns the current key.
func (it *Iterator[I, D]) KeyT() I {
	keyBytes := it.base.Key()
	key, err := it.idCodec.Decode(keyBytes)
	if err != nil {
		panic(err)
	}

	return key
}

// Key returns the current key.
func (it *Iterator[I, D]) Key() []byte {
	return it.base.Key()
}

// Value returns the current value.
func (it *Iterator[I, D]) Value() (*Object[I, D], error) {
	data, err := it.base.Value()
	if err != nil {
		return nil, err
	}
	k := it.Key()
	id, err := it.idCodec.Decode(k)
	if err != nil {
		return nil, err
	}
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
