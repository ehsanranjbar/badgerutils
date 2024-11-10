package schema

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractPathFromAny(t *testing.T) {
	tests := []struct {
		name    string
		v       any
		path    string
		want    any
		wantErr bool
	}{
		{
			name: "Simple Map",
			v: map[string]any{
				"foo": "bar",
			},
			path:    "foo",
			want:    "bar",
			wantErr: false,
		},
		{
			name: "Nested Map",
			v: map[string]any{
				"foo": map[string]any{
					"bar": "baz",
				},
			},
			path:    "foo.bar",
			want:    "baz",
			wantErr: false,
		},
		{
			name: "Map with missing key",
			v: map[string]any{
				"foo": "bar",
			},
			path:    "baz",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Simple Slice",
			v:       []any{"foo", "bar", "baz"},
			path:    "1",
			want:    "bar",
			wantErr: false,
		},
		{
			name:    "Slice with out of range index",
			v:       []any{"foo", "bar", "baz"},
			path:    "3",
			want:    nil,
			wantErr: true,
		},
		{
			name: "Nested slice",
			v: []any{
				[]any{"foo", "bar"},
				[]any{"baz", "qux"},
			},
			path:    "1.0",
			want:    "baz",
			wantErr: false,
		},
		{
			name: "Wildcard Slice",
			v: []any{
				map[string]any{"foo": []any{"1", "2"}},
				map[string]any{"foo": []any{"3", "4"}},
			},
			path:    "*.foo.*",
			want:    []any{"1", "2", "3", "4"},
			wantErr: false,
		},
		{
			name:    "Nil value",
			v:       nil,
			path:    "foo",
			want:    nil,
			wantErr: true,
		},
		{
			name: "Empty path",
			v: map[string]any{
				"foo": "bar",
			},
			path:    "",
			want:    map[string]any{"foo": "bar"},
			wantErr: false,
		},
		{
			name:    "Invalid index",
			v:       []any{"foo", "bar"},
			path:    "a",
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractPathFromAny(tt.v, tt.path)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.True(t, reflect.DeepEqual(got, tt.want), "ExtractPathFromAny() = %v, want %v", got, tt.want)
		})
	}
}
