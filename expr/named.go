package expr

// Named associates a name with an expression.
type Named struct {
	name       string
	expression any
}

// NewNamed creates a new named expression.
func NewNamed(name string, expr any) Named {
	return Named{name: name, expression: expr}
}

// Name returns the name of the expression.
func (n Named) Name() string { return n.name }

// Expr returns the expression.
func (n Named) Expression() any { return n.expression }
