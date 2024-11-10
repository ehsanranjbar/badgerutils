package qlutil

import (
	"encoding"
	"strings"
	"time"

	qlvalue "github.com/araddon/qlbridge/value"
	"github.com/ehsanranjbar/badgerutils/schema"
)

// ObjectContextWrapper is a wrapper around an object that implements the qlbridge.ContextReader interface.
type ObjectContextWrapper[I any, D encoding.BinaryMarshaler] struct {
	id        *I
	data      D
	metadata  map[string]any
	extractor schema.PathExtractor[D]
	flatter   schema.Flatter[D]
}

// NewObjectContextWrapper creates a new ObjectContextWrapper.
func NewObjectContextWrapper[I any, D encoding.BinaryMarshaler](
	id *I,
	data D,
	metadata map[string]any,
	extractor schema.PathExtractor[D],
	flatter schema.Flatter[D],
) *ObjectContextWrapper[I, D] {
	return &ObjectContextWrapper[I, D]{
		id:        id,
		data:      data,
		metadata:  metadata,
		extractor: extractor,
		flatter:   flatter,
	}
}

// Get implements the qlbridge.ContextReader interface.
func (c *ObjectContextWrapper[I, D]) Get(key string) (qlvalue.Value, bool) {
	switch {
	case key == "_id":
		return qlvalue.NewValue(c.id), true
	case strings.HasPrefix(key, "_metadata"):
		// Assume nil on error.
		v, _ := schema.ExtractPathFromAny(c.metadata, strings.TrimPrefix(key, "_metadata"))
		return qlvalue.NewValue(v), true
	default:
		v, err := c.extractor.ExtractPath(c.data, key)
		if err != nil {
			return qlvalue.NewErrorValue(err), false
		}
		return qlvalue.NewValue(v), true
	}
}

// Row implements the qlbridge.ContextReader interface.
// I don't know what this is supposed to do.
func (c *ObjectContextWrapper[I, D]) Row() map[string]qlvalue.Value {
	if c.flatter == nil {
		return nil
	}

	flat, err := c.flatter.Flatten(c.data)
	if err != nil {
		return nil
	}
	row := make(map[string]qlvalue.Value, len(flat))
	for k, v := range flat {
		row[k] = qlvalue.NewValue(v)
	}
	return row
}

// Ts implements the qlbridge.ContextReader interface.
// I don't know what this is supposed to do.
func (c *ObjectContextWrapper[I, D]) Ts() time.Time { return time.Time{} }
