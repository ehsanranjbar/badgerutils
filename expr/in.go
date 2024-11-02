package expr

// In represents a set that the value must be in.
type In[T any] struct {
	values []T
}

// NewIn creates a new in expression.
func NewIn[T any](values ...T) In[T] {
	return In[T]{values: values}
}

// Values returns the values of the in expression.
func (i In[T]) Values() []T { return i.values }
