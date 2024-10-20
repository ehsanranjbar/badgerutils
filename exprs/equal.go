package exprs

// Equal represents an equality expression.
type Equal[T any] struct {
	value T
}

// NewEqual creates a new equality expression.
func NewEqual[T any](value T) Equal[T] {
	return Equal[T]{value: value}
}

// Value returns the value of the equality expression.
func (e Equal[T]) Value() T { return e.value }
