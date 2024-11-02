package ext

import (
	"github.com/ehsanranjbar/badgerutils"
)

// Extension is an extension to the object store.
type Extension[T any] interface {
	Init(store badgerutils.BadgerStore, iter badgerutils.Iterator[*T]) error
	OnDelete(key []byte, value *T) error
	OnSet(key []byte, old, new *T, opts ...any) error
	Drop() error
}

// SpecificOption is an option that is specific to an extension.
type SpecificOption struct {
	extName string
	value   any
}

// WithSpecificOption creates a specific option.
func WithSpecificOption(name string, value any) SpecificOption {
	return SpecificOption{
		extName: name,
		value:   value,
	}
}
