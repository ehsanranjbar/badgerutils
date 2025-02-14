package extutil

import (
	"bytes"
	"encoding"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	extstore "github.com/ehsanranjbar/badgerutils/store/ext"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

var (
	associateDataPrefix = []byte("ad")
)

// AssociateStore is an extension to store additional along with the main object.
type AssociateStore[
	T, U any,
	PU sstore.PBS[U],
] struct {
	store     badgerutils.Instantiator[badgerutils.StoreInstance[[]byte, *U, *U, badgerutils.Iterator[[]byte, *U]]]
	synthFunc func(key []byte, oldV *T, newV T, oldU, newU *U) (*U, error)
}

// NewAssociateStore creates a new AssociateStore.
func NewAssociateStore[
	T, U any,
	PU sstore.PBS[U],
](opts ...func(*AssociateStore[T, U, PU])) *AssociateStore[T, U, PU] {
	as := &AssociateStore[T, U, PU]{}
	for _, opt := range opts {
		opt(as)
	}
	return as
}

// WithSynthFunc sets the default function to use for synthesizing the associated object.
func WithSynthFunc[
	T any,
	U encoding.BinaryMarshaler,
	PU sstore.PBS[U],
](f func(key []byte, oldV *T, newV T, oldU, newU *U) (*U, error)) func(*AssociateStore[T, U, PU]) {
	return func(as *AssociateStore[T, U, PU]) {
		as.synthFunc = f
	}
}

// Init implements the Extension interface.
func (as *AssociateStore[T, U, PU]) Init(store badgerutils.Instantiator[badgerutils.BadgerStore]) {
	as.store = sstore.New[U, PU](store)
}

// Instantiate implements the Extension interface.
func (as *AssociateStore[T, U, PU]) Instantiate(txn *badger.Txn) extstore.ExtensionInstance[T] {
	return &AssociateStoreInstance[T, U, PU]{
		store:     as.store.Instantiate(txn),
		synthFunc: as.synthFunc,
	}
}

// AssociateStoreInstance is an extension to store additional along with the main object.
type AssociateStoreInstance[
	T, U any,
	PU sstore.PBS[U],
] struct {
	store     badgerutils.StoreInstance[[]byte, *U, *U, badgerutils.Iterator[[]byte, *U]]
	synthFunc func(key []byte, oldV *T, newV T, oldU, newU *U) (*U, error)
}

// OnDelete implements the Extension interface.
func (as *AssociateStoreInstance[T, U, PU]) OnDelete(key []byte, value *T) error {
	key = append(associateDataPrefix, key...)
	return as.store.Delete(key)
}

// OnSet implements the Extension interface.
func (as *AssociateStoreInstance[T, U, PU]) OnSet(key []byte, old, new *T, opts ...any) error {
	opt, ok := findAs[AssociateData[U]](opts)
	if ok {
		return as.set(key, old, new, &opt.data)
	}

	return as.set(key, old, new, nil)
}

func (as *AssociateStoreInstance[T, U, PU]) set(k []byte, oldV, newV *T, u *U) error {
	if as.synthFunc != nil {
		oldU, err := as.Get(k)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		var safeNewV T
		if newV != nil {
			safeNewV = *newV
		}
		u, err = as.synthFunc(k, oldV, safeNewV, oldU, u)
		if err != nil {
			return err
		}
	}
	if u == nil {
		return nil
	}

	k = append(associateDataPrefix, k...)
	return as.store.Set(k, u)
}

// Get gets the associated data.
func (as *AssociateStoreInstance[T, U, PU]) Get(key []byte) (*U, error) {
	key = append(associateDataPrefix, key...)
	u, err := as.store.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return u, nil
}

// AssociateData is an option for extensible store to set the associated data.
type AssociateData[U any] struct {
	data U
}

// WithAssociateData sets the associated data.
func WithAssociateData[U any](data U) AssociateData[U] {
	return AssociateData[U]{data: data}
}

// Metadata is wrapper around map[string]any with encoding.BinaryMarshaler BinaryUnmarshaler implemented.
type Metadata map[string]any

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (m Metadata) MarshalBinary() ([]byte, error) {
	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)

	var buf bytes.Buffer
	enc.Reset(&buf)
	err := enc.EncodeMap(m)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler interface.
func (m *Metadata) UnmarshalBinary(data []byte) error {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	var err error
	*m, err = dec.DecodeMap()
	if err != nil {
		return err
	}

	return nil
}

// MetadataSynthFunc returns a function that can be used as a synthFunc for AssociateStore to store a
// map[string]any as metadata.
// If statistics is true, it will set "created_at" and "updated_at" fields for each value in the map.
func MetadataSynthFunc[T any, M ~map[string]any](statistics bool) func(_ []byte, _ *T, _ T, oldU, newU *M) (*M, error) {
	return func(_ []byte, oldV *T, _ T, oldU, newU *M) (*M, error) {
		if newU == nil || *newU == nil {
			newU = &M{}
		}

		if statistics {
			now := time.Now().UTC()
			if oldV == nil {
				(*newU)["created_at"] = now
			}
			(*newU)["updated_at"] = now
		}

		return mergeMaps(oldU, newU), nil
	}
}

func mergeMaps[M ~map[string]any](old, new *M) *M {
	if old == nil {
		return new
	}
	if new == nil {
		return old
	}

	for k, v := range *new {
		if v == nil {
			delete(*old, k)
			continue
		}
		(*old)[k] = v
	}
	return old
}
