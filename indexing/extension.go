package indexing

import (
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
)

// Extension is an extension for extensible stores that indexes the data with a given indexer.
type Extension[T any] struct {
	store   badgerutils.BadgerStore
	indexer Indexer[T]
}

// NewExtension creates a new Extension.
func NewExtension[T any](indexer Indexer[T]) *Extension[T] {
	return &Extension[T]{
		indexer: indexer,
	}
}

// Init implements the extensible.Extension interface.
func (e *Extension[T]) Init(
	store badgerutils.BadgerStore,
	iter badgerutils.Iterator[*T],
) error {
	e.store = store

	initialized, err := e.isInitialized()
	if err != nil {
		return err
	}
	if !initialized {
		err := e.indexIter(iter)
		if err != nil {
			return err
		}

		err = e.setInitialized()
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Extension[T]) isInitialized() (bool, error) {
	_, err := e.store.Get(nil)
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func (e *Extension[T]) indexIter(iter badgerutils.Iterator[*T]) error {
	store := refstore.New(e.store)

	for iter.Rewind(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v, err := iter.Value()
		if err != nil {
			return err
		}
		var ttl time.Duration
		if iter.Item().ExpiresAt() != 0 {
			ttl = time.Duration(iter.Item().ExpiresAt()-uint64(time.Now().Unix())) * time.Second
		}

		kvs := e.indexer.Index(v, true)
		for _, kv := range kvs {
			err = store.Set(k, refstore.NewRefEntry(kv.Key).WithValue(kv.Value).WithTTL(ttl))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *Extension[T]) setInitialized() error {
	return e.store.Set(nil, nil)
}

// OnDelete implements the extensible.Extension interface.
func (e *Extension[T]) OnDelete(key []byte, value *T) error {
	store := refstore.New(e.store)

	kvs := e.indexer.Index(value, false)
	for _, kv := range kvs {
		err := store.Delete(kv.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

// OnSet implements the extensible.Extension interface.
func (e *Extension[T]) OnSet(key []byte, old, new *T) error {
	store := refstore.New(e.store)

	if old != nil {
		kvs := e.indexer.Index(old, false)
		for _, kv := range kvs {
			err := store.Delete(kv.Key)
			if err != nil {
				return err
			}
		}
	}

	kvs := e.indexer.Index(new, true)
	for _, kv := range kvs {
		var ttl time.Duration
		if ti, ok := any(new).(sstore.TemporaryItem); ok {
			ttl = ti.TTL()
		}
		err := store.Set(key, refstore.NewRefEntry(kv.Key).WithValue(kv.Value).WithTTL(ttl))
		if err != nil {
			return err
		}
	}

	return nil
}

// Drop implements the extensible.Extension interface.
func (e *Extension[T]) Drop() error {
	return nil
}

// GetByRef returns the value by the reference.
func (e *Extension[T]) GetByRef(ref []byte) ([]byte, error) {
	return refstore.New(e.store).Get(ref)
}

// GetRefIterator returns a new ref iterator.
func (e *Extension[T]) GetRefIterator(opts badger.IteratorOptions) badgerutils.Iterator[[]byte] {
	return refstore.New(e.store).NewIterator(opts)
}