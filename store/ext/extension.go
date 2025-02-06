package ext

import (
	"github.com/ehsanranjbar/badgerutils"
)

// Extension is an extension to the object store.
type Extension[T any] interface {
	badgerutils.Instantiator[ExtensionInstance[T]]
	Init(store badgerutils.Instantiator[badgerutils.BadgerStore])
}

// ExtensionInstance is an extension to the object store.
type ExtensionInstance[T any] interface {
	OnDelete(key []byte, value *T) error
	OnSet(key []byte, old, new *T, opts ...any) error
}

// ExtOption is an option that is specific to an extension.
type ExtOption struct {
	extName string
	value   any
}

// WithExtOption creates a specific option.
func WithExtOption(name string, value any) ExtOption {
	return ExtOption{
		extName: name,
		value:   value,
	}
}
