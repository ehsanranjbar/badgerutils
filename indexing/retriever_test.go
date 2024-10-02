package indexing_test

import (
	"encoding/json"
	"testing"

	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/stretchr/testify/require"
)

func TestMapValueRetriever_RetrieveValue(t *testing.T) {
	type Bar struct {
		C int
	}

	type Foo struct {
		A string
		B *Bar
	}

	tests := []struct {
		name     string
		input    *Foo
		paths    []string
		expected map[string]any
	}{
		{
			name: "Retrieve single field",
			input: &Foo{
				A: "test",
			},
			paths: []string{"A"},
			expected: map[string]any{
				"A": "test",
			},
		},
		{
			name: "Retrieve nested field",
			input: &Foo{
				A: "test",
				B: &Bar{
					C: 123,
				},
			},
			paths: []string{"A", "B.C"},
			expected: map[string]any{
				"A":   "test",
				"B.C": 123,
			},
		},
		{
			name: "Nil pointer",
			input: &Foo{
				A: "test",
			},
			paths: []string{"B.C"},
			expected: map[string]any{
				"B.C": nil,
			},
		},
		{
			name:     "Nil",
			input:    nil,
			paths:    []string{"A"},
			expected: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := indexing.NewMapValueRetriever[Foo](
				json.Marshal,
				tt.paths...,
			)
			require.NoError(t, err)

			got := r.RetrieveValue(tt.input)
			expectedJson, err := json.Marshal(tt.expected)
			require.NoError(t, err)
			require.Equal(t, string(expectedJson), string(got))
		})
	}
}
