package ext

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/internal/ordmap"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
)

var (
	dataStorePrefix = []byte{0x00}
	extStorePrefix  = []byte{0x01}
)

// Store is a store that executes hooks on object creation and deletion.
type Store[
	T any,
	PT sstore.PBS[T],
] struct {
	dataStore badgerutils.Instantiator[badgerutils.StoreInstance[[]byte, *T, *T, badgerutils.Iterator[[]byte, *T]]]
	extStore  *pstore.Store
	exts      *ordmap.Map[string, Extension[T]]
	prefix    []byte
}

// New creates a new Store.
func New[
	T any,
	PT sstore.PBS[T],
](
	base badgerutils.Instantiator[badgerutils.BadgerStore],
	opts ...func(*Store[T, PT]) error,
) *Store[T, PT] {
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
	for _, opt := range opts {
		opt(store)
	}

	return store
}

type prefixed interface {
	Prefix() []byte
}

// WithExtension adds an extension to the store.
func WithExtension[T any, PT sstore.PBS[T]](name string, ext Extension[T]) func(*Store[T, PT]) error {
	return func(s *Store[T, PT]) error {
		if sr, ok := ext.(StoreRegistry); ok {
			sr.RegisterStore(pstore.New(s.extStore, []byte(name)))
		}

		err := s.exts.Add(name, ext)
		if err != nil {
			return fmt.Errorf("failed to add extension %s: %w", name, err)
		}

		return nil
	}
}

// Instantiate creates a new Instance.
func (s *Store[T, PT]) Instantiate(txn *badger.Txn) *Instance[T, PT] {
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

// Instance is a store that stores objects.
type Instance[
	T any,
	PT sstore.PBS[T],
] struct {
	dataStore badgerutils.StoreInstance[[]byte, *T, *T, badgerutils.Iterator[[]byte, *T]]
	exts      *ordmap.Map[string, ExtensionInstance[T]]
	prefix    []byte
}

// Prefix returns the prefix of the store.
func (s *Instance[T, PT]) Prefix() []byte {
	return s.prefix
}

// Delete deletes an object along with all its auxiliary references (i.e. secondary indexes).
func (s *Instance[T, PT]) Delete(key []byte) error {
	err := s.onDelete(key)
	if err != nil {
		return err
	}

	err = s.dataStore.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete object's data: %w", err)
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

// Get gets an object given it's key.
func (s *Instance[T, PT]) Get(key []byte) (*T, error) {
	return s.dataStore.Get(key)
}

// NewIterator creates a new iterator over the objects.
func (s *Instance[T, PT]) NewIterator(opts badger.IteratorOptions) badgerutils.Iterator[[]byte, *T] {
	return s.dataStore.NewIterator(opts)
}

// Set inserts the object into the store as a new object or updates an existing object
func (s *Instance[T, PT]) Set(key []byte, obj *T) error {
	return s.SetWithOptions(key, obj)
}

// SetWithOptions inserts the object into the store as a new object or updates an existing object
func (s *Instance[T, PT]) SetWithOptions(key []byte, obj *T, opts ...any) error {
	err := s.onSet(key, obj, opts...)
	if err != nil {
		return err
	}

	err = s.dataStore.Set(key, obj)
	if err != nil {
		return fmt.Errorf("failed to set object's data: %w", err)
	}

	return nil
}

func (s *Instance[T, PT]) onSet(key []byte, new *T, opts ...any) error {
	if s.exts.Len() == 0 {
		return nil
	}

	old, err := s.dataStore.Get(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to get object's data: %w", err)
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

		extOpts = append(extOpts, opt)
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

// ManagerInstance is an instance of the store that can manage extensions which typically is used in migrations.
type ManagerInstance[T any, PT sstore.PBS[T]] struct {
	*Instance[T, PT]
	store *Store[T, PT]
	txn   *badger.Txn
}

// AddExtension adds an extension and feed it all the existing objects.
func (s *ManagerInstance[T, PT]) AddExtension(name string, ext Extension[T]) error {
	if name == "" {
		return errors.New("extension name cannot be empty")
	}
	if _, ok := s.exts.Get(name); ok {
		return fmt.Errorf("extension with name %s already exists", name)
	}

	if sr, ok := ext.(StoreRegistry); ok {
		es := pstore.New(s.store.extStore, []byte(name))
		sr.RegisterStore(es)
	}

	iter := s.dataStore.NewIterator(badger.IteratorOptions{})
	defer iter.Close()
	extIns := ext.Instantiate(s.txn)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		v, err := iter.Value()
		if err != nil {
			return fmt.Errorf("failed to initialize extension %s: %w", name, err)
		}
		err = extIns.OnSet(context.Background(), iter.Key(), nil, v, InitializationFlag{})
		if err != nil {
			return fmt.Errorf("failed to initialize extension %s: %w", name, err)
		}
	}

	s.store.exts.Add(name, ext)
	s.exts.Add(name, extIns)
	return nil
}

// InitializationFlag is a flag that is passed to extensions to indicate that the extension is being initialized.
type InitializationFlag struct{}

// DropExtension drops an extension.
func (s *ManagerInstance[T, PT]) DropExtension(name string) error {
	_, ok := s.exts.Get(name)
	if !ok {
		return fmt.Errorf("extension %s not found", name)
	}

	es := pstore.New(s.store.extStore, []byte(name)).Instantiate(s.txn)
	err := dropStore(es)
	if err != nil {
		return fmt.Errorf("failed to purge extension %s sub store: %w", name, err)
	}

	s.store.exts.Delete(name)
	s.exts.Delete(name)
	return nil
}

func dropStore(store badgerutils.BadgerStore) error {
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
func (s *ManagerInstance[T, PT]) DropAllExtensions() error {
	for name := range s.exts.Iter() {
		err := s.DropExtension(name)
		if err != nil {
			return err
		}
	}

	return nil
}

// ListExtensions lists all extensions.
func (s *ManagerInstance[T, PT]) ListExtensions() []string {
	var names []string
	for name := range s.exts.Iter() {
		names = append(names, name)
	}

	return names
}
