package extutil

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	extstore "github.com/ehsanranjbar/badgerutils/store/ext"
)

// MapWrapper is an extension that wraps an extension of type U and maps the value to type T.
type MapWrapper[T, U any] struct {
	base extstore.Extension[U]
	m    func(*T) *U
}

// NewMapWrapper creates a new MapWrapper.
func NewMapWrapper[T, U any](base extstore.Extension[U], m func(*T) *U) *MapWrapper[T, U] {
	return &MapWrapper[T, U]{
		base: base,
		m:    m,
	}
}

// Instantiate implements the Extension interface.
func (mw *MapWrapper[T, U]) Instantiate(txn *badger.Txn) extstore.ExtensionInstance[T] {
	return &MapWrapperInstance[T, U]{
		base: mw.base.Instantiate(txn),
		m:    mw.m,
	}
}

// RegisterStore implements the StoreRegistry interface.
func (mw *MapWrapper[T, U]) RegisterStore(store badgerutils.Instantiator[badgerutils.BadgerStore]) {
	if sr, ok := mw.base.(extstore.StoreRegistry); ok {
		sr.RegisterStore(store)
	}
}

// MapWrapperInstance is an instance of MapWrapper.
type MapWrapperInstance[T, U any] struct {
	base extstore.ExtensionInstance[U]
	m    func(*T) *U
}

// OnDelete implements the ExtensionInstance interface.
func (mwi *MapWrapperInstance[T, U]) OnDelete(key []byte, value *T) error {
	return mwi.base.OnDelete(key, mwi.m(value))
}

// OnSet implements the ExtensionInstance interface.
func (mwi *MapWrapperInstance[T, U]) OnSet(key []byte, old, new *T, opts ...any) error {
	return mwi.base.OnSet(key, mwi.m(old), mwi.m(new), opts...)
}
