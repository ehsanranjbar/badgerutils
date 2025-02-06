package indexing

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/store/ext"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
)

// Extension is an extension for extensible stores that indexes the data with a given indexer.
type Extension[T any] struct {
	indexer    Indexer[T]
	descriptor IndexDescriptor
	store      *refstore.Store
}

// NewExtension creates a new Extension.
func NewExtension[T any](indexer Indexer[T]) ext.Extension[T] {
	descriptor, _ := indexer.(IndexDescriptor)
	return &Extension[T]{
		indexer:    indexer,
		descriptor: descriptor,
	}
}

// Init implements the extensible.Extension interface.
func (e *Extension[T]) Init(store badgerutils.Instantiator[badgerutils.BadgerStore]) {
	e.store = refstore.New(store)
}

// Instantiate implements the extensible.Extension interface.
func (e *Extension[T]) Instantiate(txn *badger.Txn) ext.ExtensionInstance[T] {
	return &ExtensionInstance[T]{
		ext:   e,
		store: e.store.Instantiate(txn).(*refstore.Instance),
	}
}

type ExtensionInstance[T any] struct {
	ext   *Extension[T]
	store *refstore.Instance
}

// OnDelete implements the extensible.Extension interface.
func (e *ExtensionInstance[T]) OnDelete(key []byte, value *T) error {
	kvs, err := e.ext.indexer.Index(value, false)
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		err := e.store.Delete(kv.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

// OnSet implements the extensible.Extension interface.
func (e *ExtensionInstance[T]) OnSet(key []byte, old, new *T, opts ...any) error {
	if old != nil {
		kvs, err := e.ext.indexer.Index(old, false)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			err := e.store.Delete(kv.Key)
			if err != nil {
				return err
			}
		}
	}

	kvs, err := e.ext.indexer.Index(new, true)
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		err := e.store.Set(key, refstore.NewRefEntry(kv.Key).WithValue(kv.Value))
		if err != nil {
			return err
		}
	}

	return nil
}

// Lookup queries the index with the given arguments and returns an iterator of keys.
func (e *ExtensionInstance[T]) Lookup(opts badger.IteratorOptions, args ...any) (badgerutils.Iterator[[]byte, []byte], error) {
	iter, err := e.ext.indexer.Lookup(args...)
	if err != nil {
		return nil, err
	}

	return LookupPartitions(e.store, iter, opts), nil
}

// SupportedQueries returns the supported queries of the index.
func (e *ExtensionInstance[T]) SupportedQueries() []string {
	return e.ext.descriptor.SupportedQueries()
}

// SupportedValues returns the supported values of the index.
func (e *ExtensionInstance[T]) SupportedValues() []string {
	return e.ext.descriptor.SupportedValues()
}
