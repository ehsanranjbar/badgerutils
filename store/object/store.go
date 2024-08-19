package object

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"

	"github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
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
type Store[K any, V any, PV interface {
	sstore.BinarySerializable
	*V
}] struct {
	vstore  *sstore.Store[V, PV]
	istore  badgerutils.BadgerStore
	indexer Indexer[V]
}

// New creates a new ObjectStore.
func New[K any, V any, PV interface {
	sstore.BinarySerializable
	*V
}](
	base badgerutils.BadgerStore,
	indexer Indexer[V],
) *Store[K, V, PV] {
	return &Store[K, V, PV]{
		vstore:  sstore.New[V, PV](pstore.New(base, vstorePrefix)),
		istore:  pstore.New(base, istorePrefix),
		indexer: indexer,
	}
}

// Delete deletes an object along with all it's auxiliary references (i.e. secondary indexes).
func (s *Store[K, V, PV]) Delete(key K) error {
	keyBz := anyToBytes(key)

	err := s.deleteRefs(keyBz)
	if err != nil {
		return fmt.Errorf("failed to delete refs: %w", err)
	}

	err = s.vstore.Delete(keyBz)
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}

func anyToBytes(v any) []byte {
	switch v := v.(type) {
	case int:
		return binary.LittleEndian.AppendUint64(nil, uint64(v))
	case uint:
		return binary.LittleEndian.AppendUint64(nil, uint64(v))
	case int8:
		return []byte{byte(v)}
	case uint8:
		return []byte{byte(v)}
	case int16:
		return binary.LittleEndian.AppendUint16(nil, uint16(v))
	case uint16:
		return binary.LittleEndian.AppendUint16(nil, v)
	case int32:
		return binary.LittleEndian.AppendUint32(nil, uint32(v))
	case uint32:
		return binary.LittleEndian.AppendUint32(nil, v)
	case int64:
		return binary.LittleEndian.AppendUint64(nil, uint64(v))
	case uint64:
		return binary.LittleEndian.AppendUint64(nil, v)
	case string:
		return []byte(v)
	case []byte:
		return v
	case encoding.BinaryMarshaler:
		bz, err := v.MarshalBinary()
		if err != nil {
			panic(fmt.Sprintf("failed to marshal binary: %v", err))
		}
		return bz
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}
}

func (s *Store[K, V, PV]) deleteRefs(key []byte) error {
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
func (s *Store[K, V, PV]) Get(key K) (*V, error) {
	keyBz := anyToBytes(key)

	return s.vstore.Get(keyBz)
}

// GetByRef gets an object given it's index name and prefix.
func (s *Store[K, V, PV]) GetByRef(index string, prefix []byte) (*V, error) {
	rs := refstore.New(pstore.New(s.istore, []byte(index)))

	key, err := rs.Get(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	return s.vstore.Get(key)
}

// NewIterator creates a new iterator over the objects.
func (s *Store[K, V, PV]) NewIterator(opts badger.IteratorOptions) badgerutils.Iterator[*V] {
	return s.vstore.NewIterator(opts)
}

// NewRefIterator creates a new iterator over an index.
func (s *Store[K, V, PV]) NewRefIterator(index string, opts badger.IteratorOptions) badgerutils.Iterator[K] {
	rs := refstore.New(pstore.New(s.istore, []byte(index)))

	return iters.Map(rs.NewIterator(opts), func(key []byte, _ *badger.Item) (K, error) {
		return bytesToAny[K](key), nil
	})
}

func bytesToAny[T any](bz []byte) (v T) {
	rv := reflect.ValueOf(&v).Elem()
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		rv.SetInt(int64(binary.LittleEndian.Uint64(bz)))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		rv.SetUint(binary.LittleEndian.Uint64(bz))
	case reflect.String:
		rv.SetString(string(bz))
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			rv.SetBytes(bz)
		}
	case reflect.Ptr:
		if rv.Type().Elem().Implements(reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()) {
			err := any(&v).(encoding.BinaryUnmarshaler).UnmarshalBinary(bz)
			if err != nil {
				panic(fmt.Sprintf("failed to unmarshal binary: %v", err))
			}
		}
	default:
		panic(fmt.Sprintf("unsupported type: %T", v))
	}

	return v
}

// Set inserts the object into the store as a new object or updates an existing object
// along with inserting/updating all it's auxiliary references (i.e. secondary indexes).
func (s *Store[K, V, PV]) Set(key K, obj *V) error {
	keyBz := anyToBytes(key)

	err := s.deleteRefs(keyBz)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return fmt.Errorf("failed to delete old refs: %w", err)
	}

	err = s.vstore.Set(keyBz, obj)
	if err != nil {
		return fmt.Errorf("failed to set object: %w", err)
	}

	err = s.setRefs(keyBz, obj)
	if err != nil {
		return fmt.Errorf("failed to set refs: %w", err)
	}

	return nil
}

func (s *Store[K, V, PV]) setRefs(key []byte, obj *V) error {
	refs := s.indexer.Index(obj, true)
	for name, ref := range refs {
		rs := refstore.New(pstore.New(s.istore, []byte(name)))
		err := rs.Set(key, ref)
		if err != nil {
			return fmt.Errorf("failed to set ref: %w", err)
		}
	}

	return nil
}
