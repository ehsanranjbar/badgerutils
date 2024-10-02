package btree_test

import (
	"testing"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/indexing/btree"
	"github.com/ehsanranjbar/badgerutils/utils/be"
	"github.com/stretchr/testify/require"
)

type Foo struct {
	Name    string
	Gender  string
	Age     int
	Struct  Bar
	Pointer *Bar
}

type Bar struct {
	Test int
}

type MockRetriever struct{}

func (r *MockRetriever) RetrieveValue(v *Foo) []byte {
	return []byte("value")
}

func TestIndexer_Index(t *testing.T) {
	tests := []struct {
		name       string
		components []string
		retriever  indexing.ValueRetriever[Foo]
		input      *Foo
		want       []badgerutils.RawKVPair
		wantErr    bool
	}{
		{
			name:       "Single component",
			components: []string{"Name"},
			input:      &Foo{Name: "Alice"},
			want:       []badgerutils.RawKVPair{{Key: []byte("Alice")}},
		},
		{
			name:       "Multiple components",
			components: []string{"Gender", "Age"},
			input:      &Foo{Gender: "Male", Age: 30},
			want:       []badgerutils.RawKVPair{{Key: append(be.EncodeString("Male"), be.EncodeInt64(30)...), Value: nil}},
		},
		{
			name:       "Nested struct",
			components: []string{"Struct.Test"},
			input:      &Foo{Struct: Bar{Test: 42}},
			want:       []badgerutils.RawKVPair{{Key: be.EncodeInt64(42)}},
		},
		{
			name:       "Pointer to struct",
			components: []string{"Pointer.Test"},
			input:      &Foo{Pointer: &Bar{Test: 42}},
			want:       []badgerutils.RawKVPair{{Key: be.EncodeInt64(42)}},
		},
		{
			name:       "Descending order",
			components: []string{"Age desc"},
			input:      &Foo{Age: 30},
			want:       []badgerutils.RawKVPair{{Key: be.InverseBytes(be.EncodeInt64(30))}},
		},
		{
			name:       "With retriever",
			components: []string{"Name"},
			retriever:  &MockRetriever{},
			input:      &Foo{Name: "Alice"},
			want:       []badgerutils.RawKVPair{{Key: []byte("Alice"), Value: []byte("value")}},
		},
		{
			name:       "Non-existing field",
			components: []string{"NonExisting"},
			input:      &Foo{},
			wantErr:    true,
		},
		{
			name:       "Invalid component",
			components: []string{"Name desc invalid"},
			wantErr:    true,
		},
		{
			name:       "Nil input",
			components: []string{"Name"},
			input:      nil,
			want:       nil,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer, err := btree.New[Foo](tt.components...)
			if tt.wantErr {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}

			indexer.SetRetriever(tt.retriever)

			got := indexer.Index(tt.input, false)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIndexer_String(t *testing.T) {
	tests := []struct {
		name       string
		components []string
		want       string
	}{
		{
			name:       "Single component",
			components: []string{"Name"},
			want:       "Name",
		},
		{
			name:       "Multiple components",
			components: []string{"Gender", "Age"},
			want:       "Gender, Age",
		},
		{
			name:       "Nested struct",
			components: []string{"Struct.Test"},
			want:       "Struct.Test",
		},
		{
			name:       "Pointer to struct",
			components: []string{"Pointer.Test"},
			want:       "Pointer.Test",
		},
		{
			name:       "Descending order",
			components: []string{"Age desc"},
			want:       "Age DESC",
		},
		{
			name:       "Multiple components with descending",
			components: []string{"Name", "Age desc"},
			want:       "Name, Age DESC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer, err := btree.New[Foo](tt.components...)
			require.NoError(t, err)

			got := indexer.String()
			require.Equal(t, tt.want, got)
		})
	}
}
