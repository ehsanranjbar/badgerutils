package object

import (
	"encoding"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/indexing"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
)

var (
	vstorePrefix = []byte("v")
	istorePrefix = []byte("i")
)

// Store is a store that stores objects.
type Store[T encoding.BinaryMarshaler, PT sstore.PointerBinaryUnmarshaler[T]] struct {
	vstore  *sstore.Store[T, PT]
	istore  badgerutils.BadgerStore
	indexer indexing.Indexer[T]
}

// New creates a new ObjectStore.
func New[T encoding.BinaryMarshaler, PT sstore.PointerBinaryUnmarshaler[T]](
	base badgerutils.BadgerStore,
	indexer indexing.Indexer[T],
) *Store[T, PT] {
	return &Store[T, PT]{
		vstore:  sstore.New[T, PT](pstore.New(base, vstorePrefix)),
		istore:  pstore.New(base, istorePrefix),
		indexer: indexer,
	}
}

// Delete deletes an object along with all it's auxiliary references (i.e. secondary indexes).
func (s *Store[T, PT]) Delete(key []byte) error {
	err := s.deleteRefs(key)
	if err != nil {
		return fmt.Errorf("failed to delete refs: %w", err)
	}

	err = s.vstore.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}

func (s *Store[T, PT]) deleteRefs(key []byte) error {
	obj, err := s.vstore.Get(key)
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}

	idxs := s.indexer.Index(obj, false)
	for name, idx := range idxs {
		rs := refstore.New(pstore.New(s.istore, []byte(name)))
		err := rs.Delete(append(idx.Key, key...))
		if err != nil {
			return fmt.Errorf("failed to delete ref: %w", err)
		}
	}

	return nil
}

// Get gets an object given it's key.
func (s *Store[T, PT]) Get(key []byte) (*T, error) {
	return s.vstore.Get(key)
}

// GetByRef gets an object given it's index name and prefix.
func (s *Store[T, PT]) GetByRef(index string, prefix []byte) (*T, error) {
	rs := refstore.New(pstore.New(s.istore, []byte(index)))

	key, err := rs.Get(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	return s.vstore.Get(key)
}

// NewIterator creates a new iterator over the objects.
func (s *Store[T, PT]) NewIterator(opts badger.IteratorOptions) badgerutils.Iterator[*T] {
	return s.vstore.NewIterator(opts)
}

// NewRefIterator creates a new iterator over an index.
func (s *Store[T, PT]) NewRefIterator(index string, opts badger.IteratorOptions) badgerutils.Iterator[[]byte] {
	return refstore.New(pstore.New(s.istore, []byte(index))).NewIterator(opts)
}

// Set inserts the object into the store as a new object or updates an existing object
// along with inserting/updating all it's auxiliary references (i.e. secondary indexes).
func (s *Store[T, PT]) Set(key []byte, obj *T) error {
	err := s.deleteRefs(key)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to delete old refs: %w", err)
	}

	err = s.vstore.Set(key, obj)
	if err != nil {
		return fmt.Errorf("failed to set object: %w", err)
	}

	err = s.setRefs(key, obj)
	if err != nil {
		return fmt.Errorf("failed to set refs: %w", err)
	}

	return nil
}

func (s *Store[T, PT]) setRefs(key []byte, obj *T) error {
	var ttl time.Duration
	if ti, ok := any(obj).(sstore.TemporaryItem); ok {
		ttl = ti.TTL()
	}

	idxs := s.indexer.Index(obj, true)
	for name, idx := range idxs {
		rs := refstore.New(pstore.New(s.istore, []byte(name)))
		err := rs.Set(key, refstore.NewRefEntry(idx.Key).WithTTL(ttl))
		if err != nil {
			return fmt.Errorf("failed to set ref: %w", err)
		}
	}

	return nil
}
