package btree_test

import (
	"testing"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/indexing/btree"
	"github.com/ehsanranjbar/badgerutils/iters"
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

			got := indexer.Index(tt.input, true)
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
func TestIndexer_Lookup(t *testing.T) {
	tests := []struct {
		name       string
		components []string
		args       []any
		want       []indexing.Partition
		wantErr    bool
	}{
		{
			name:       "Single component equal",
			components: []string{"Name"},
			args:       []any{indexing.NewEqualLookupExpr("Name", "Alice")},
			want:       []indexing.Partition{indexing.NewPartition(indexing.NewBound([]byte("Alice"), false), indexing.NewBound([]byte("Alice"), false))},
		},
		{
			name:       "Multiple components equal",
			components: []string{"Gender", "Age"},
			args:       []any{indexing.NewEqualLookupExpr("Gender", "Male"), indexing.NewEqualLookupExpr("Age", 30)},
			want:       []indexing.Partition{indexing.NewPartition(indexing.NewBound(append(be.EncodeString("Male"), be.EncodeInt64(30)...), false), indexing.NewBound(append(be.EncodeString("Male"), be.EncodeInt64(30)...), false))},
		},
		{
			name:       "Nested struct equal",
			components: []string{"Struct.Test"},
			args:       []any{indexing.NewEqualLookupExpr("Struct.Test", 42)},
			want:       []indexing.Partition{indexing.NewPartition(indexing.NewBound(be.EncodeInt64(42), false), indexing.NewBound(be.EncodeInt64(42), false))},
		},
		{
			name:       "Pointer to struct equal",
			components: []string{"Pointer.Test"},
			args:       []any{indexing.NewEqualLookupExpr("Pointer.Test", 42)},
			want:       []indexing.Partition{indexing.NewPartition(indexing.NewBound(be.EncodeInt64(42), false), indexing.NewBound(be.EncodeInt64(42), false))},
		},
		{
			name:       "Descending order equal",
			components: []string{"Age desc"},
			args:       []any{indexing.NewEqualLookupExpr("Age", 30)},
			want:       []indexing.Partition{indexing.NewPartition(indexing.NewBound(be.InverseBytes(be.EncodeInt64(30)), false), indexing.NewBound(be.InverseBytes(be.EncodeInt64(30)), false))},
		},
		{
			name:       "Non-existing field",
			components: []string{"Name"},
			args:       []any{indexing.NewEqualLookupExpr("NonExisting", "Value")},
			wantErr:    true,
		},
		{
			name:       "Undefined lookup",
			components: []string{"Name", "Age"},
			args:       []any{indexing.NewEqualLookupExpr("Age", 30)},
			wantErr:    true,
		},
		{
			name:       "Invalid argument",
			components: []string{"Name"},
			args:       []any{"InvalidArg"},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer, err := btree.New[Foo](tt.components...)
			require.NoError(t, err)

			got, err := indexer.Lookup(tt.args...)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			gotPartitions, err := iters.Collect(got)
			require.NoError(t, err)
			require.Equal(t, tt.want, gotPartitions)
		})
	}
}
