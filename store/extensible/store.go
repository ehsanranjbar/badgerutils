package extensible

import (
	"encoding"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
)

var (
	dataStorePrefix = []byte("data")
	extStorePrefix  = []byte("ext")
)

// Store is a store that stores objects.
type Store[T encoding.BinaryMarshaler,
	PT sstore.PointerBinaryUnmarshaler[T],
] struct {
	base badgerutils.BadgerStore
	exts map[string]Extension[T]
}

// Extension is an extension to the object store.
type Extension[T any] interface {
	Init(store badgerutils.BadgerStore, iter badgerutils.Iterator[*T]) error
	OnDelete(key []byte, value *T) error
	OnSet(key []byte, old, new *T, opts ...any) error
	Drop() error
}

// New creates a new ObjectStore.
func New[T encoding.BinaryMarshaler,
	PT sstore.PointerBinaryUnmarshaler[T],
](
	base badgerutils.BadgerStore,
) *Store[T, PT] {
	return &Store[T, PT]{
		base: base,
		exts: make(map[string]Extension[T]),
	}
}

// Prefix returns the prefix of the store.
func (s *Store[T, PT]) Prefix() []byte {
	if pfx, ok := s.base.(prefixed); ok {
		return pfx.Prefix()
	}

	return nil
}

type prefixed interface {
	Prefix() []byte
}

// Delete deletes an object along with all it's auxiliary references (i.e. secondary indexes).
func (s *Store[T, PT]) Delete(key []byte) error {
	err := s.onDelete(key)
	if err != nil {
		return err
	}

	err = s.getDataStore().Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete object's data: %w", err)
	}

	return nil
}

func (s *Store[T, PT]) onDelete(key []byte) error {
	if len(s.exts) == 0 {
		return nil
	}

	data, err := s.getDataStore().Get(key)
	if err != nil {
		return err
	}

	for name, ext := range s.exts {
		err := ext.OnDelete(key, data)
		if err != nil {
			return fmt.Errorf("failure in running extension %s OnDelete: %w", name, err)
		}
	}

	return nil
}

func (s *Store[T, PT]) getDataStore() *sstore.Store[T, PT] {
	return sstore.New[T, PT](pstore.New(s.base, dataStorePrefix))
}

func (s *Store[T, PT]) getExtensionStore(name string) *pstore.Store {
	return pstore.New(s.base, append(extStorePrefix, []byte(name)...))
}

// Get gets an object given it's key.
func (s *Store[T, PT]) Get(key []byte) (*T, error) {
	return s.getDataStore().Get(key)
}

// NewIterator creates a new iterator over the objects.
func (s *Store[T, PT]) NewIterator(opts badger.IteratorOptions) badgerutils.Iterator[*T] {
	return s.getDataStore().NewIterator(opts)
}

// Set inserts the object into the store as a new object or updates an existing object
func (s *Store[T, PT]) Set(key []byte, obj *T) error {
	dstore := s.getDataStore()
	old, err := dstore.Get(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get object's data: %w", err)
	}

	err = s.getDataStore().Set(key, obj)
	if err != nil {
		return fmt.Errorf("failed to set object's data: %w", err)
	}

	err = s.onSet(key, old, obj)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store[T, PT]) SetWithOptions(key []byte, obj *T, opts ...any) error {
	dstore := s.getDataStore()
	old, err := dstore.Get(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get object's data: %w", err)
	}

	err = s.getDataStore().Set(key, obj)
	if err != nil {
		return fmt.Errorf("failed to set object's data: %w", err)
	}

	err = s.onSet(key, old, obj, opts...)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store[T, PT]) onSet(key []byte, old, new *T, opts ...any) error {
	if len(s.exts) == 0 {
		return nil
	}

	for name, ext := range s.exts {
		err := ext.OnSet(key, old, new, opts...)
		if err != nil {
			return fmt.Errorf("failure in running extension %s OnSet: %w", name, err)
		}
	}

	return nil
}

// AddExtension adds an extension and feed it all the existing objects.
func (s *Store[T, PT]) AddExtension(name string, ext Extension[T]) error {
	if name == "" {
		return errors.New("extension name cannot be empty")
	}
	if s.exts == nil {
		s.exts = make(map[string]Extension[T])
	}
	if _, ok := s.exts[name]; ok {
		return fmt.Errorf("an extension already registered with name %s", name)
	}

	store := s.getExtensionStore(name)
	iter := s.getDataStore().NewIterator(badger.IteratorOptions{})
	defer iter.Close()
	err := ext.Init(store, iter)
	if err != nil {
		return fmt.Errorf("failed to initialize extension %s: %w", name, err)
	}

	s.exts[name] = ext
	return nil
}

// DropExtension drops an extension.
func (s *Store[T, PT]) DropExtension(name string) error {
	ext, ok := s.exts[name]
	if !ok {
		return fmt.Errorf("extension %s not found", name)
	}

	err := ext.Drop()
	if err != nil {
		return fmt.Errorf("failed to drop extension %s: %w", name, err)
	}

	store := s.getExtensionStore(name)
	err = dropStore(store)
	if err != nil {
		return fmt.Errorf("failed to purge extension %s sub store: %w", name, err)
	}

	delete(s.exts, name)
	return nil
}

func dropStore(store *pstore.Store) error {
	iter := pstore.NewIteratorFromStore(store)
	defer iter.Close()

	for iter.Rewind(); iter.Valid(); iter.Next() {
		err := store.Delete(iter.Key())
		if err != nil {
			return err
		}
	}

	return nil
}

// DropAllExtensions drops all extensions.
func (s *Store[T, PT]) DropAllExtensions() error {
	for name := range s.exts {
		err := s.DropExtension(name)
		if err != nil {
			return err
		}
	}

	return nil
}
