package ext

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/internal/ordmap"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
)

var (
	dataStorePrefix = []byte{'d'}
	extStorePrefix  = []byte{'x'}
)

// Store is a wrapper around a serialized store with an ordered list of extensions
// that can modify data before it is stored or do arbitrary operations on set and delete.
type Store[
	T any,
	PT sstore.BSP[T],
] struct {
	dataStore   badgerutils.Instantiator[badgerutils.StoreInstance[[]byte, *T, *T, badgerutils.Iterator[[]byte, *T]]]
	extStore    *pstore.Store
	exts        *ordmap.Map[string, Extension[T]]
	prefix      []byte
	initialized bool
	init        sync.Once
}

// New creates a new Store.
func New[
	T any,
	PT sstore.BSP[T],
](base badgerutils.Instantiator[badgerutils.BadgerStore]) *Store[T, PT] {
	var prefix []byte
	if pfx, ok := base.(prefixed); ok {
		prefix = pfx.Prefix()
	}

	store := &Store[T, PT]{
		dataStore: sstore.New[T, PT](pstore.New(base, dataStorePrefix)),
		extStore:  pstore.New(base, extStorePrefix),
		exts:      ordmap.New[string, Extension[T]](),
		prefix:    prefix,
	}

	return store
}

type prefixed interface {
	Prefix() []byte
}

// WithExtension adds an extension to the store.
func (s *Store[T, PT]) WithExtension(name string, ext Extension[T]) *Store[T, PT] {
	if s.initialized {
		panic("store is already initialized")
	}

	if sr, ok := ext.(StoreRegistry); ok {
		sr.RegisterStore(pstore.New(s.extStore, []byte(name)))
	}

	err := s.exts.Add(name, ext)
	if err != nil {
		panic("extension with the same name already exists")
	}

	return s
}

// Instantiate creates a new Instance.
func (s *Store[T, PT]) Instantiate(txn *badger.Txn) *Instance[T, PT] {
	// Locking any changes to the store's configuration on first instantiation.
	s.init.Do(func() {
		s.initialized = true
	})

	return &Instance[T, PT]{
		dataStore: s.dataStore.Instantiate(txn),
		exts:      s.instantiateExts(txn),
		prefix:    s.prefix,
	}
}

func (s *Store[T, PT]) instantiateExts(txn *badger.Txn) *ordmap.Map[string, ExtensionInstance[T]] {
	exts := ordmap.New[string, ExtensionInstance[T]]()
	for name, ext := range s.exts.Iter() {
		exts.Add(name, ext.Instantiate(txn))
	}

	return exts
}

// GetExtension returns an extension by name.
func (s *Store[T, PT]) GetExtension(name string) Extension[T] {
	if ext, ok := s.exts.Get(name); ok {
		return ext
	}

	return nil
}

func (s *Store[T, PT]) Prefix() []byte {
	return s.prefix
}

// Instance is an instance of Store.
type Instance[
	T any,
	PT sstore.BSP[T],
] struct {
	dataStore badgerutils.StoreInstance[[]byte, *T, *T, badgerutils.Iterator[[]byte, *T]]
	exts      *ordmap.Map[string, ExtensionInstance[T]]
	prefix    []byte
}

// Prefix returns the prefix of the store.
func (s *Instance[T, PT]) Prefix() []byte {
	return s.prefix
}

// Delete implements the badgerutils.StoreInstance interface.
func (s *Instance[T, PT]) Delete(key []byte) error {
	err := s.onDelete(key)
	if err != nil {
		return err
	}

	err = s.dataStore.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete record: %w", err)
	}

	return nil
}

func (s *Instance[T, PT]) onDelete(key []byte) error {
	if s.exts.Len() == 0 {
		return nil
	}

	data, err := s.dataStore.Get(key)
	if err != nil {
		return err
	}

	ctx := context.Background()
	for _, name := range s.exts.Iter() {
		err := name.OnDelete(ctx, key, data)
		if err != nil {
			return fmt.Errorf("failure in running extension %s OnDelete: %w", name, err)
		}
	}

	return nil
}

// Get implements the badgerutils.StoreInstance interface.
func (s *Instance[T, PT]) Get(key []byte) (*T, error) {
	return s.dataStore.Get(key)
}

// NewIterator implements the badgerutils.StoreInstance interface.
func (s *Instance[T, PT]) NewIterator(opts badger.IteratorOptions) badgerutils.Iterator[[]byte, *T] {
	return s.dataStore.NewIterator(opts)
}

// Set implements the badgerutils.StoreInstance interface.
func (s *Instance[T, PT]) Set(key []byte, v *T) error {
	return s.SetWithOptions(key, v)
}

// SetWithOptions is a variant of Set that allows passing options to extensions.
func (s *Instance[T, PT]) SetWithOptions(key []byte, v *T, opts ...any) error {
	err := s.onSet(key, v, opts...)
	if err != nil {
		return err
	}

	err = s.dataStore.Set(key, v)
	if err != nil {
		return fmt.Errorf("failed to set record: %w", err)
	}

	return nil
}

func (s *Instance[T, PT]) onSet(key []byte, new *T, opts ...any) error {
	if s.exts.Len() == 0 {
		return nil
	}

	old, err := s.dataStore.Get(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get record: %w", err)
	}

	ctx := context.Background()
	for name, ext := range s.exts.Iter() {
		extOpts := filterOptions(name, opts)
		err := ext.OnSet(ctx, key, old, new, extOpts...)
		if err != nil {
			return fmt.Errorf("failure in running extension %s OnSet: %w", name, err)
		}
	}

	return nil
}

func filterOptions(name string, opts []any) []any {
	var extOpts []any
	for _, opt := range opts {
		if so, ok := opt.(ExtOption); ok {
			if so.extName == name {
				extOpts = append(extOpts, so.value)
			} else {
				continue
			}
		}
	}

	return extOpts
}

// GetExtension returns an extension's instance by name.
func (s *Instance[T, PT]) GetExtension(name string) ExtensionInstance[T] {
	if ext, ok := s.exts.Get(name); ok {
		return ext
	}

	return nil
}
