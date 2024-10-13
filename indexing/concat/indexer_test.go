package concat_test

import (
	"bytes"
	"testing"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/exprs"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/indexing/concat"
	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/ehsanranjbar/badgerutils/utils/be"
	"github.com/stretchr/testify/require"
)

type Foo struct {
	Str1     string
	Str2     string
	Int      int
	Struct   Bar
	Pointer  *Bar
	Bytes    []byte
	Array    [3]int
	StrSlice []string
}

type Bar struct {
	Test int
}

func TestIndexer_Index(t *testing.T) {
	tests := []struct {
		name       string
		components []concat.Component
		input      *Foo
		want       []badgerutils.RawKVPair
		wantErr    bool
	}{
		{
			name:       "Single component",
			components: []concat.Component{concat.NewComponent("Str1")},
			input:      &Foo{Str1: "Alice"},
			want: []badgerutils.RawKVPair{
				{
					Key: be.PadOrTrimRight([]byte("Alice"), concat.DefaultStringSize),
				},
			},
		},
		{
			name:       "Multiple components",
			components: []concat.Component{concat.NewComponent("Str2"), concat.NewComponent("Int")},
			input:      &Foo{Str2: "Male", Int: 30},
			want: []badgerutils.RawKVPair{
				{
					Key: append(be.PadOrTrimRight(be.EncodeString("Male"), concat.DefaultStringSize), be.EncodeInt64(30)...),
				},
			},
		},
		{
			name:       "Nested struct",
			components: []concat.Component{concat.NewComponent("Struct.Test")},
			input:      &Foo{Struct: Bar{Test: 42}},
			want:       []badgerutils.RawKVPair{{Key: be.EncodeInt64(42)}},
		},
		{
			name:       "Pointer to struct",
			components: []concat.Component{concat.NewComponent("Pointer.Test")},
			input:      &Foo{Pointer: &Bar{Test: 42}},
			want:       []badgerutils.RawKVPair{{Key: be.EncodeInt64(42)}},
		},
		{
			name:       "Bytes",
			components: []concat.Component{concat.NewComponent("Bytes")},
			input:      &Foo{Bytes: []byte{1, 2, 3}},
			want: []badgerutils.RawKVPair{
				{Key: be.PadOrTrimRight([]byte{1, 2, 3}, concat.DefaultBytesSize)},
			},
		},
		{
			name:       "Array",
			components: []concat.Component{concat.NewComponent("Array")},
			input:      &Foo{Array: [3]int{1, 2, 3}},
			want: []badgerutils.RawKVPair{
				{Key: be.EncodeInt64(1)},
				{Key: be.EncodeInt64(2)},
				{Key: be.EncodeInt64(3)},
			},
		},
		{
			name:       "Slice",
			components: []concat.Component{concat.NewComponent("StrSlice")},
			input:      &Foo{StrSlice: []string{"Alice", "Bob"}},
			want: []badgerutils.RawKVPair{
				{Key: be.PadOrTrimRight([]byte("Alice"), concat.DefaultStringSize)},
				{Key: be.PadOrTrimRight([]byte("Bob"), concat.DefaultStringSize)},
			},
		},
		{
			name:       "Array x Slice",
			components: []concat.Component{concat.NewComponent("Array"), concat.NewComponent("StrSlice")},
			input:      &Foo{Array: [3]int{1, 2, 3}, StrSlice: []string{"Alice", "Bob"}},
			want: []badgerutils.RawKVPair{
				{Key: append(be.EncodeInt64(1), be.PadOrTrimRight([]byte("Alice"), concat.DefaultStringSize)...)},
				{Key: append(be.EncodeInt64(1), be.PadOrTrimRight([]byte("Bob"), concat.DefaultStringSize)...)},
				{Key: append(be.EncodeInt64(2), be.PadOrTrimRight([]byte("Alice"), concat.DefaultStringSize)...)},
				{Key: append(be.EncodeInt64(2), be.PadOrTrimRight([]byte("Bob"), concat.DefaultStringSize)...)},
				{Key: append(be.EncodeInt64(3), be.PadOrTrimRight([]byte("Alice"), concat.DefaultStringSize)...)},
				{Key: append(be.EncodeInt64(3), be.PadOrTrimRight([]byte("Bob"), concat.DefaultStringSize)...)},
			},
		},
		{
			name:       "Descending order",
			components: []concat.Component{concat.NewComponent("Int").Desc()},
			input:      &Foo{Int: 30},
			want:       []badgerutils.RawKVPair{{Key: be.Inverse(be.EncodeInt64(30))}},
		},
		{
			name:       "Custom size",
			components: []concat.Component{concat.NewComponent("Str1").Size(10)},
			input:      &Foo{Str1: "Alice"},
			want: []badgerutils.RawKVPair{
				{Key: be.PadOrTrimRight([]byte("Alice"), 10)},
			},
		},
		{
			name:       "Non-existing field",
			components: []concat.Component{concat.NewComponent("NonExisting")},
			input:      &Foo{},
			wantErr:    true,
		},
		{
			name:       "Nil input",
			components: []concat.Component{concat.NewComponent("Str1")},
			input:      nil,
			want:       nil,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer, err := concat.New[Foo](tt.components...)
			if tt.wantErr {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}

			got, err := indexer.Index(tt.input, true)
			if tt.wantErr {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}
			require.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestIndexer_Lookup(t *testing.T) {
	tests := []struct {
		name       string
		components []concat.Component
		args       []any
		want       []indexing.Partition
		wantErr    bool
	}{
		{
			name:       "Single component equal",
			components: []concat.Component{concat.NewComponent("Str1")},
			args:       []any{exprs.NewNamed("Str1", exprs.NewEqual("Alice"))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(be.PadOrTrimRight(be.EncodeString("Alice"), concat.DefaultStringSize), false),
					exprs.NewBound(be.PadOrTrimRight(be.EncodeString("Alice"), concat.DefaultStringSize), false),
				),
			},
		},
		{
			name:       "Multiple components equal",
			components: []concat.Component{concat.NewComponent("Str2"), concat.NewComponent("Int")},
			args:       []any{exprs.NewNamed("Str2", exprs.NewEqual("Male")), exprs.NewNamed("Int", exprs.NewEqual(30))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(append(be.PadOrTrimRight(be.EncodeString("Male"), concat.DefaultStringSize), be.EncodeInt64(30)...), false),
					exprs.NewBound(append(be.PadOrTrimRight(be.EncodeString("Male"), concat.DefaultStringSize), be.EncodeInt64(30)...), false),
				),
			},
		},
		{
			name:       "Range",
			components: []concat.Component{concat.NewComponent("Int")},
			args:       []any{exprs.NewNamed("Int", exprs.NewRange[any](exprs.NewBound[any](0, false), exprs.NewBound[any](30, false)))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(be.EncodeInt64(0), false),
					exprs.NewBound(be.EncodeInt64(30), false),
				),
			},
		},
		{
			name:       "In",
			components: []concat.Component{concat.NewComponent("Int")},
			args:       []any{exprs.NewNamed("Int", exprs.NewIn(10, 20, 30))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(be.EncodeInt64(10), false),
					exprs.NewBound(be.EncodeInt64(10), false),
				),
				indexing.NewPartition(
					exprs.NewBound(be.EncodeInt64(20), false),
					exprs.NewBound(be.EncodeInt64(20), false),
				),
				indexing.NewPartition(
					exprs.NewBound(be.EncodeInt64(30), false),
					exprs.NewBound(be.EncodeInt64(30), false),
				),
			},
		},
		{
			name:       "Omitted component",
			components: []concat.Component{concat.NewComponent("Str1"), concat.NewComponent("Str2")},
			args:       []any{exprs.NewNamed("Str1", exprs.NewEqual("Alice"))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(append(be.PadOrTrimRight(be.EncodeString("Alice"), concat.DefaultStringSize), make([]byte, concat.DefaultStringSize)...), false),
					exprs.NewBound(append(be.PadOrTrimRight(be.EncodeString("Alice"), concat.DefaultStringSize), bytes.Repeat([]byte{0xff}, concat.DefaultStringSize)...), false),
				),
			},
		},
		{
			name:       "Nested struct equal",
			components: []concat.Component{concat.NewComponent("Struct.Test")},
			args:       []any{exprs.NewNamed("Struct.Test", exprs.NewEqual(42))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(be.EncodeInt64(42), false),
					exprs.NewBound(be.EncodeInt64(42), false),
				),
			},
		},
		{
			name:       "Pointer to struct equal",
			components: []concat.Component{concat.NewComponent("Pointer.Test")},
			args:       []any{exprs.NewNamed("Pointer.Test", exprs.NewEqual(42))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(be.EncodeInt64(42), false),
					exprs.NewBound(be.EncodeInt64(42), false),
				),
			},
		},
		{
			name:       "Descending order equal",
			components: []concat.Component{concat.NewComponent("Int").Desc()},
			args:       []any{exprs.NewNamed("Int", exprs.NewEqual(30))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(be.Inverse(be.EncodeInt64(30)), false),
					exprs.NewBound(be.Inverse(be.EncodeInt64(30)), false),
				),
			},
		},
		{
			name:       "Slice equal",
			components: []concat.Component{concat.NewComponent("StrSlice")},
			args:       []any{exprs.NewNamed("StrSlice", exprs.NewEqual("Alice"))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(be.PadOrTrimRight(be.EncodeString("Alice"), concat.DefaultStringSize), false),
					exprs.NewBound(be.PadOrTrimRight(be.EncodeString("Alice"), concat.DefaultStringSize), false),
				),
			},
		},
		{
			name:       "Non-existing field",
			components: []concat.Component{concat.NewComponent("Str1")},
			args:       []any{exprs.NewNamed("NonExisting", exprs.NewEqual("Value"))},
			wantErr:    true,
		},
		{
			name:       "Too many arguments",
			components: []concat.Component{concat.NewComponent("Str1")},
			args: []any{
				exprs.NewNamed("Str1", exprs.NewEqual("Alice")),
				exprs.NewNamed("Str2", exprs.NewEqual("Bob")),
			},
			wantErr: true,
		},
		{
			name:       "Duplicate path",
			components: []concat.Component{concat.NewComponent("Str1"), concat.NewComponent("Str2")},
			args:       []any{exprs.NewNamed("Str1", exprs.NewEqual("Alice")), exprs.NewNamed("Str1", exprs.NewEqual("Bob"))},
			wantErr:    true,
		},
		{
			name:       "Invalid argument",
			components: []concat.Component{concat.NewComponent("Str1")},
			args:       []any{"InvalidArg"},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer, err := concat.New[Foo](tt.components...)
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
