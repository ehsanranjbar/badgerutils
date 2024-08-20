package indexing

import "github.com/ehsanranjbar/badgerutils"

// Indexer is an indexer.
type Indexer[T any] interface {
	Index(v *T, update bool) map[string]badgerutils.RawKVPair
}
