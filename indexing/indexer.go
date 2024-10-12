package indexing

import (
	"fmt"

	"github.com/ehsanranjbar/badgerutils"
)

// ErrUndefinedLookup is an error for situations in which the indexer has no optimized lookup for the given arguments.
var ErrUndefinedLookup = fmt.Errorf("undefined lookup")

// Indexer is an indexer.
type Indexer[T any] interface {
	Index(v *T, set bool) ([]badgerutils.RawKVPair, error)
	Lookup(args ...any) (badgerutils.Iterator[Partition], error)
}
