package indexing

// RangeLookupExpr is an expression for a range lookup.
type RangeLookupExpr struct {
	path string
	low  Bound[any]
	high Bound[any]
}

// NewRangeLookupExpr creates a new range lookup expression.
func NewRangeLookupExpr(path string, low, high Bound[any]) RangeLookupExpr {
	return RangeLookupExpr{path: path, low: low, high: high}
}

// NewEqualLookupExpr creates a new equal lookup expression.
func NewEqualLookupExpr(path string, value any) RangeLookupExpr {
	return RangeLookupExpr{path: path, low: NewBound(value, false), high: NewBound(value, false)}
}

// Path returns the path of the expression.
func (r RangeLookupExpr) Path() string { return r.path }

// Low returns the low bound of the expression.
func (r RangeLookupExpr) Low() Bound[any] { return r.low }

// High returns the high bound of the expression.
func (r RangeLookupExpr) High() Bound[any] { return r.high }

// IsEqual returns true if the expression is an equal lookup.
func (r RangeLookupExpr) IsEqual() bool {
	return r.low.value == r.high.value && !r.low.exclusive && !r.high.exclusive
}

// Bound represents a bound in a range.
type Bound[T any] struct {
	value     T
	exclusive bool
	empty     bool
}

// NewBound creates a new bound.
func NewBound[T any](value T, exclusive bool) Bound[T] {
	return Bound[T]{value: value, exclusive: exclusive}
}

// EmptyBound creates an empty bound.
func EmptyBound[T any]() Bound[T] {
	return Bound[T]{empty: true}
}

// Value returns the value of the bound.
func (b Bound[T]) Value() T { return b.value }

// Exclusive returns true if the bound is exclusive.
func (b Bound[T]) Exclusive() bool { return b.exclusive }

// IsEmpty returns true if the bound is empty.
func (b Bound[T]) IsEmpty() bool { return b.empty }
