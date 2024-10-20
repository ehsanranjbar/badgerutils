package concat_test

import (
	"bytes"
	"testing"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec"
	"github.com/ehsanranjbar/badgerutils/codec/be"
	"github.com/ehsanranjbar/badgerutils/codec/lex"
	"github.com/ehsanranjbar/badgerutils/exprs"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/indexing/concat"
	"github.com/ehsanranjbar/badgerutils/iters"
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
					Key: be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize),
				},
			},
		},
		{
			name:       "Multiple components",
			components: []concat.Component{concat.NewComponent("Str2"), concat.NewComponent("Int").WithSize(8)},
			input:      &Foo{Str2: "Male", Int: 30},
			want: []badgerutils.RawKVPair{
				{
					Key: append(be.PadOrTruncRight([]byte("Male"), concat.DefaultMaxComponentSize), lex.EncodeInt64(30)...),
				},
			},
		},
		{
			name:       "Nested struct",
			components: []concat.Component{concat.NewComponent("Struct.Test").WithSize(8)},
			input:      &Foo{Struct: Bar{Test: 42}},
			want:       []badgerutils.RawKVPair{{Key: lex.EncodeInt64(42)}},
		},
		{
			name:       "Pointer to struct",
			components: []concat.Component{concat.NewComponent("Pointer.Test").WithSize(8)},
			input:      &Foo{Pointer: &Bar{Test: 42}},
			want:       []badgerutils.RawKVPair{{Key: lex.EncodeInt64(42)}},
		},
		{
			name:       "Bytes",
			components: []concat.Component{concat.NewComponent("Bytes")},
			input:      &Foo{Bytes: []byte{1, 2, 3}},
			want: []badgerutils.RawKVPair{
				{Key: be.PadOrTruncRight([]byte{1, 2, 3}, concat.DefaultMaxComponentSize)},
			},
		},
		{
			name:       "Array",
			components: []concat.Component{concat.NewComponent("Array").WithSize(8)},
			input:      &Foo{Array: [3]int{1, 2, 3}},
			want: []badgerutils.RawKVPair{
				{Key: lex.EncodeInt64(1)},
				{Key: lex.EncodeInt64(2)},
				{Key: lex.EncodeInt64(3)},
			},
		},
		{
			name:       "Slice",
			components: []concat.Component{concat.NewComponent("StrSlice")},
			input:      &Foo{StrSlice: []string{"Alice", "Bob"}},
			want: []badgerutils.RawKVPair{
				{Key: be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize)},
				{Key: be.PadOrTruncRight([]byte("Bob"), concat.DefaultMaxComponentSize)},
			},
		},
		{
			name:       "Array x Slice",
			components: []concat.Component{concat.NewComponent("Array").WithSize(8), concat.NewComponent("StrSlice")},
			input:      &Foo{Array: [3]int{1, 2, 3}, StrSlice: []string{"Alice", "Bob"}},
			want: []badgerutils.RawKVPair{
				{Key: append(lex.EncodeInt64(1), be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize)...)},
				{Key: append(lex.EncodeInt64(1), be.PadOrTruncRight([]byte("Bob"), concat.DefaultMaxComponentSize)...)},
				{Key: append(lex.EncodeInt64(2), be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize)...)},
				{Key: append(lex.EncodeInt64(2), be.PadOrTruncRight([]byte("Bob"), concat.DefaultMaxComponentSize)...)},
				{Key: append(lex.EncodeInt64(3), be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize)...)},
				{Key: append(lex.EncodeInt64(3), be.PadOrTruncRight([]byte("Bob"), concat.DefaultMaxComponentSize)...)},
			},
		},
		{
			name:       "Descending order",
			components: []concat.Component{concat.NewComponent("Int").WithSize(8).Desc()},
			input:      &Foo{Int: 30},
			want:       []badgerutils.RawKVPair{{Key: lex.Invert(lex.EncodeInt64(30))}},
		},
		{
			name:       "Custom size",
			components: []concat.Component{concat.NewComponent("Str1").WithSize(10)},
			input:      &Foo{Str1: "Alice"},
			want: []badgerutils.RawKVPair{
				{Key: be.PadOrTruncRight([]byte("Alice"), 10)},
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
			lxf := lex.SimpleLexifier{}
			indexer, err := concat.New[Foo](
				codec.NewConvertPathExtractor(codec.NewReflectPathExtractor[Foo](), lxf.LexifyRVMulti),
				tt.components...,
			)

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
	lxf := lex.SimpleLexifier{}

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
			args:       []any{exprs.NewNamed("Str1", exprs.NewEqual(lxf.MustLexifyAny("Alice")))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), false),
					exprs.NewBound(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), false),
				),
			},
		},
		{
			name:       "Multiple components equal",
			components: []concat.Component{concat.NewComponent("Str2"), concat.NewComponent("Int").WithSize(8)},
			args: []any{
				exprs.NewNamed("Str2", exprs.NewEqual(lxf.MustLexifyAny("Male"))),
				exprs.NewNamed("Int", exprs.NewEqual(lxf.MustLexifyAny(int(30)))),
			},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(append(be.PadOrTruncRight([]byte("Male"), concat.DefaultMaxComponentSize), lex.EncodeInt64(30)...), false),
					exprs.NewBound(append(be.PadOrTruncRight([]byte("Male"), concat.DefaultMaxComponentSize), lex.EncodeInt64(30)...), false),
				),
			},
		},
		{
			name:       "Range",
			components: []concat.Component{concat.NewComponent("Int").WithSize(8)},
			args: []any{exprs.NewNamed(
				"Int",
				exprs.NewRange(
					exprs.NewBound(lxf.MustLexifyAny(int(0)), false),
					exprs.NewBound(lxf.MustLexifyAny(int(30)), false),
				),
			)},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(lex.EncodeInt64(0), false),
					exprs.NewBound(lex.EncodeInt64(30), false),
				),
			},
		},
		{
			name:       "In",
			components: []concat.Component{concat.NewComponent("Int").WithSize(8)},
			args: []any{exprs.NewNamed("Int", exprs.NewIn(
				lxf.MustLexifyAny(int(10)),
				lxf.MustLexifyAny(int(20)),
				lxf.MustLexifyAny(int(30)),
			))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(lex.EncodeInt64(10), false),
					exprs.NewBound(lex.EncodeInt64(10), false),
				),
				indexing.NewPartition(
					exprs.NewBound(lex.EncodeInt64(20), false),
					exprs.NewBound(lex.EncodeInt64(20), false),
				),
				indexing.NewPartition(
					exprs.NewBound(lex.EncodeInt64(30), false),
					exprs.NewBound(lex.EncodeInt64(30), false),
				),
			},
		},
		{
			name:       "Omitted component",
			components: []concat.Component{concat.NewComponent("Str1"), concat.NewComponent("Str2")},
			args:       []any{exprs.NewNamed("Str1", exprs.NewEqual(lxf.MustLexifyAny("Alice")))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(append(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), make([]byte, concat.DefaultMaxComponentSize)...), false),
					exprs.NewBound(append(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), bytes.Repeat([]byte{0xff}, concat.DefaultMaxComponentSize)...), false),
				),
			},
		},
		{
			name:       "Nested struct equal",
			components: []concat.Component{concat.NewComponent("Struct.Test").WithSize(8)},
			args:       []any{exprs.NewNamed("Struct.Test", exprs.NewEqual(lxf.MustLexifyAny(int64(42))))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(lex.EncodeInt64(42), false),
					exprs.NewBound(lex.EncodeInt64(42), false),
				),
			},
		},
		{
			name:       "Pointer to struct equal",
			components: []concat.Component{concat.NewComponent("Pointer.Test").WithSize(8)},
			args:       []any{exprs.NewNamed("Pointer.Test", exprs.NewEqual(lxf.MustLexifyAny(int64(42))))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(lex.EncodeInt64(42), false),
					exprs.NewBound(lex.EncodeInt64(42), false),
				),
			},
		},
		{
			name:       "Descending order equal",
			components: []concat.Component{concat.NewComponent("Int").WithSize(8).Desc()},
			args:       []any{exprs.NewNamed("Int", exprs.NewEqual(lxf.MustLexifyAny(int64(30))))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(lex.Invert(lex.EncodeInt64(30)), false),
					exprs.NewBound(lex.Invert(lex.EncodeInt64(30)), false),
				),
			},
		},
		{
			name:       "Slice equal",
			components: []concat.Component{concat.NewComponent("StrSlice")},
			args:       []any{exprs.NewNamed("StrSlice", exprs.NewEqual(lxf.MustLexifyAny("Alice")))},
			want: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), false),
					exprs.NewBound(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), false),
				),
			},
		},
		{
			name:       "Non-existing field",
			components: []concat.Component{concat.NewComponent("Str1")},
			args:       []any{exprs.NewNamed("NonExisting", exprs.NewEqual(lxf.MustLexifyAny("Value")))},
			wantErr:    true,
		},
		{
			name:       "Too many arguments",
			components: []concat.Component{concat.NewComponent("Str1")},
			args: []any{
				exprs.NewNamed("Str1", exprs.NewEqual(lxf.MustLexifyAny("Alice"))),
				exprs.NewNamed("Str2", exprs.NewEqual(lxf.MustLexifyAny("Bob"))),
			},
			wantErr: true,
		},
		{
			name:       "Duplicate path",
			components: []concat.Component{concat.NewComponent("Str1"), concat.NewComponent("Str2")},
			args:       []any{exprs.NewNamed("Str1", exprs.NewEqual(lxf.MustLexifyAny("Alice"))), exprs.NewNamed("Str1", exprs.NewEqual(lxf.MustLexifyAny("Bob")))},
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
			indexer, err := concat.New[Foo](
				codec.NewConvertPathExtractor(codec.NewReflectPathExtractor[Foo](), lxf.LexifyRVMulti),
				tt.components...,
			)
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
