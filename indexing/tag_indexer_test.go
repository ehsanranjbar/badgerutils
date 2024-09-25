package indexing

import (
	"encoding/binary"
	"testing"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/stretchr/testify/require"
)

type TestStruct struct {
	Name  string `index:"name"`
	Age   int    `index:"age_idx"`
	Email string `index:""`
}

func TestTagIndexer_Index(t *testing.T) {
	indexer, err := NewTagIndexer[TestStruct]()
	require.NoError(t, err)

	tests := []struct {
		name      string
		input     TestStruct
		wantPairs []badgerutils.RawKVPair
	}{
		{
			name: "valid struct",
			input: TestStruct{
				Name:  "John",
				Age:   30,
				Email: "john@example.com",
			},
			wantPairs: []badgerutils.RawKVPair{
				{
					Key:   []byte("nameJohn"),
					Value: nil,
				},
				{
					Key:   append([]byte("age_idx"), binary.LittleEndian.AppendUint64(nil, uint64(30))...),
					Value: nil,
				},
				{
					Key:   append([]byte("Email"), []byte("john@example.com")...),
					Value: nil,
				},
			},
		},
		{
			name: "empty struct",
			input: TestStruct{
				Name:  "",
				Age:   0,
				Email: "",
			},
			wantPairs: []badgerutils.RawKVPair{
				{
					Key:   []byte("name"),
					Value: nil,
				},
				{
					Key:   append([]byte("age_idx"), binary.LittleEndian.AppendUint64(nil, uint64(0))...),
					Value: nil,
				},
				{
					Key:   []byte("Email"),
					Value: nil,
				},
			},
		},
		{
			name: "partial struct",
			input: TestStruct{
				Name:  "Alice",
				Age:   0,
				Email: "",
			},
			wantPairs: []badgerutils.RawKVPair{
				{
					Key:   []byte("nameAlice"),
					Value: nil,
				},
				{
					Key:   append([]byte("age_idx"), binary.LittleEndian.AppendUint64(nil, uint64(0))...),
					Value: nil,
				},
				{
					Key:   []byte("Email"),
					Value: nil,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPairs := indexer.Index(tt.input, false)
			require.ElementsMatch(t, tt.wantPairs, gotPairs)
		})
	}
}

func TestTagIndexer_Lookup(t *testing.T) {
	indexer, err := NewTagIndexer[TestStruct]()
	require.NoError(t, err)

	tests := []struct {
		name      string
		args      []any
		wantKey   []byte
		wantError bool
	}{
		{
			name:      "valid string index",
			args:      []any{"name", "John"},
			wantKey:   []byte("nameJohn"),
			wantError: false,
		},
		{
			name:      "valid int index",
			args:      []any{"age_idx", int64(30)},
			wantKey:   append([]byte("age_idx"), binary.LittleEndian.AppendUint64(nil, uint64(30))...),
			wantError: false,
		},
		{
			name:      "missing index name",
			args:      []any{},
			wantKey:   nil,
			wantError: true,
		},
		{
			name:      "invalid index name type",
			args:      []any{123},
			wantKey:   nil,
			wantError: true,
		},
		{
			name:      "non-existent index name",
			args:      []any{"nonexistent"},
			wantKey:   nil,
			wantError: true,
		},
		{
			name:      "unsupported kind",
			args:      []any{"email", 123},
			wantKey:   nil,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIter, err := indexer.Lookup(tt.args...)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				gotPartitions, err := iters.Collect(gotIter)
				require.NoError(t, err)
				require.Len(t, gotPartitions, 1)
				gotKey := gotPartitions[0].Low
				require.Equal(t, tt.wantKey, gotKey)
			}
		})
	}
}
