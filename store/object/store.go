package object

import (
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
type Store[T any, PT interface {
	sstore.BinarySerializable
	*T
}] struct {
	vstore  *sstore.Store[T, PT]
	istore  badgerutils.BadgerStore
	indexer *Indexer[T]
}

// New creates a new ObjectStore.
func New[T any, PT interface {
	sstore.BinarySerializable
	*T
}](
	base badgerutils.BadgerStore,
	indexer *Indexer[T],
) *Store[T, PT] {
	return &Store[T, PT]{
		vstore:  sstore.New[T, PT](pstore.New(base, vstorePrefix)),
		istore:  pstore.New(base, istorePrefix),
		indexer: indexer,
	}
}
