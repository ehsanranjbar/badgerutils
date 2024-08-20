package object

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
)

var (
	vstorePrefix = []byte("v")
	istorePrefix = []byte("i")
)

// Indexer is an indexer.
type Indexer[T any] interface {
	Index(v *T, update bool) map[string]refstore.RefEntry
}

// Store is a store that stores objects.
type Store[V any, PV interface {
	sstore.BinarySerializable
	*V
}] struct {
	vstore  *sstore.Store[V, PV]
	istore  badgerutils.BadgerStore
	indexer Indexer[V]
}

// New creates a new ObjectStore.
func New[V any, PV interface {
	sstore.BinarySerializable
	*V
}](
	base badgerutils.BadgerStore,
	indexer Indexer[V],
) *Store[V, PV] {
	return &Store[V, PV]{
		vstore:  sstore.New[V, PV](pstore.New(base, vstorePrefix)),
		istore:  pstore.New(base, istorePrefix),
		indexer: indexer,
	}
}

// Delete deletes an object along with all it's auxiliary references (i.e. secondary indexes).
func (s *Store[V, PV]) Delete(key []byte) error {
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

func (s *Store[V, PV]) deleteRefs(key []byte) error {
	obj, err := s.vstore.Get(key)
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}

	refs := s.indexer.Index(obj, false)
	for name, ref := range refs {
		rs := refstore.New(pstore.New(s.istore, []byte(name)))
		err := rs.Delete(append(ref.Prefix, key...))
		if err != nil {
			return fmt.Errorf("failed to delete ref: %w", err)
		}
	}

	return nil
}

// Get gets an object given it's key.
func (s *Store[V, PV]) Get(key []byte) (*V, error) {
	return s.vstore.Get(key)
}

// GetByRef gets an object given it's index name and prefix.
func (s *Store[V, PV]) GetByRef(index string, prefix []byte) (*V, error) {
	rs := refstore.New(pstore.New(s.istore, []byte(index)))

	key, err := rs.Get(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	return s.vstore.Get(key)
}

// NewIterator creates a new iterator over the objects.
func (s *Store[V, PV]) NewIterator(opts badger.IteratorOptions) badgerutils.Iterator[*V] {
	return s.vstore.NewIterator(opts)
}

// NewRefIterator creates a new iterator over an index.
func (s *Store[V, PV]) NewRefIterator(index string, opts badger.IteratorOptions) badgerutils.Iterator[[]byte] {
	return refstore.New(pstore.New(s.istore, []byte(index))).NewIterator(opts)
}

// Set inserts the object into the store as a new object or updates an existing object
// along with inserting/updating all it's auxiliary references (i.e. secondary indexes).
func (s *Store[V, PV]) Set(key []byte, obj *V) error {
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

func (s *Store[V, PV]) setRefs(key []byte, obj *V) error {
	var ttl time.Duration
	if ti, ok := any(obj).(sstore.TemporaryItem); ok {
		ttl = ti.TTL()
	}

	refs := s.indexer.Index(obj, true)
	for name, ref := range refs {
		rs := refstore.New(pstore.New(s.istore, []byte(name)))
		err := rs.Set(key, ref.WithTTL(ttl))
		if err != nil {
			return fmt.Errorf("failed to set ref: %w", err)
		}
	}

	return nil
}
