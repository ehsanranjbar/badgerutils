package exprs

// Equal represents an equality expression.
type Equal struct {
	value any
}

// NewEqual creates a new equality expression.
func NewEqual(value any) Equal {
	return Equal{value: value}
}

// Value returns the value of the equality expression.
func (e Equal) Value() any { return e.value }
