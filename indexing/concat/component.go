package concat

import (
	"github.com/ehsanranjbar/badgerutils/codec/lex"
)

const (
	// DefaultMaxComponentSize is the default size that the binary representation of a component will be padded or truncated to.
	DefaultMaxComponentSize = 256
)

// Component represents a component of concatenated keys in index
type Component struct {
	path       string
	descending bool
	size       int
}

// NewComponent creates a new component with the given path.
func NewComponent(path string) Component {
	return Component{path: path, size: DefaultMaxComponentSize}
}

// Desc sets the descending flag of the component.
func (comp Component) Desc() Component {
	comp.descending = true
	return comp
}

// Size modifies the size of the component.
func (comp Component) WithSize(size int) Component {
	if size <= 0 {
		panic("size must be positive")
	}

	comp.size = size
	return comp
}

func (comp Component) postProcess(v lex.Value) []byte {
	if comp.descending {
		v = v.Invert()
	}

	v = v.Resize(comp.size)

	bz, err := v.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return bz
}
