package lex

import "encoding"

// Value is a lexicographically sortable value.
type Value interface {
	Invert() Value
	Size() int
	Resize(int) Value

	encoding.BinaryMarshaler
}
