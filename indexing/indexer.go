package indexing

import (
	"github.com/ehsanranjbar/badgerutils"
)

// Indexer is an indexer.
type Indexer[T any] interface {
	Index(v *T, set bool) []badgerutils.RawKVPair
}
