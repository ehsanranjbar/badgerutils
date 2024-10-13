package exprs

// In represents a set that the value must be in.
type In struct {
	values []any
}

// NewIn creates a new in expression.
func NewIn(values ...any) In {
	return In{values: values}
}

// Values returns the values of the in expression.
func (i In) Values() []any { return i.values }
