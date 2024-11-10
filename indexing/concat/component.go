package concat

const (
	// DefaultMaxComponentSize is the default size that the binary representation of a component will be padded or truncated to.
	DefaultMaxComponentSize = 256
)

// Component represents a component of concatenated keys in index
type Component struct {
	path        string
	includeType bool
	size        int
	descending  bool
}

// NewComponent creates a new component with the given path.
func NewComponent(path string) Component {
	return Component{path: path, size: DefaultMaxComponentSize}
}

// IncludeType sets the includeType flag of the component.
func (comp Component) IncludeType() Component {
	comp.includeType = true
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

// Desc sets the descending flag of the component.
func (comp Component) Desc() Component {
	comp.descending = true
	return comp
}
