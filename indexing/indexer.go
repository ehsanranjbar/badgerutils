package indexing

import (
	"github.com/ehsanranjbar/badgerutils"
)

// Indexer is an indexer.
type Indexer[T any] interface {
	Index(v *T, set bool) ([]badgerutils.RawKVPair, error)
	Lookup(args ...any) (badgerutils.Iterator[Partition], error)
}

// IndexDescriptor is an index describer.
type IndexDescriptor interface {
	SupportedQueries() []string
	SupportedValues() []string
}
