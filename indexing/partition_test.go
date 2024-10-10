package indexing_test

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/iters"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
	"github.com/stretchr/testify/require"
)

func TestPartitionString(t *testing.T) {
	tests := []struct {
		partition indexing.Partition
		expected  string
	}{
		{
			partition: indexing.NewPartition(indexing.NewBound([]byte{0x01}, false), indexing.NewBound([]byte{0x02}, false)),
			expected:  "[0x01, 0x02]",
		},
		{
			partition: indexing.NewPartition(indexing.NewBound[[]byte]([]byte{0x01}, true), indexing.NewBound[[]byte]([]byte{0x02}, true)),
			expected:  "(0x01, 0x02)",
		},
		{
			partition: indexing.NewPartition(indexing.EmptyBound[[]byte](), indexing.EmptyBound[[]byte]()),
			expected:  "[0x00, ∞]",
		},
	}

	for _, test := range tests {
		result := test.partition.String()
		require.Equal(t, test.expected, result)
	}
}

func TestLookupPartitions(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()

	store := refstore.New(txn)
	var n uint64 = 0
	for i := 'A'; i <= 'Z'; i++ {
		store.Set(binary.BigEndian.AppendUint64(nil, n), refstore.NewRefEntry([]byte{byte(i)}))
		n++

		for j := 'A'; j <= 'Z'; j++ {
			store.Set(binary.BigEndian.AppendUint64(nil, n), refstore.NewRefEntry([]byte{byte(i), byte(j)}))
			n++
		}
	}

	tests := []struct {
		name           string
		parts          []indexing.Partition
		expectedCount  int
		expectedRanges []uint64Range
	}{
		{
			name: "(∞, ∞)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					indexing.EmptyBound[[]byte](),
					indexing.EmptyBound[[]byte](),
				),
			},
			expectedCount: 702,
			expectedRanges: []uint64Range{
				{
					low:  indexing.NewBound[uint64](0, false),
					high: indexing.NewBound[uint64](702, false),
				},
			},
		},
		{
			name: "[A, B)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					indexing.NewBound([]byte{'A'}, false),
					indexing.NewBound([]byte{'B'}, true),
				),
			},
			expectedCount: 27,
			expectedRanges: []uint64Range{
				{
					low:  indexing.NewBound[uint64](0, false),
					high: indexing.NewBound[uint64](26, false),
				},
			},
		},
		{
			name: "(A, B]",
			parts: []indexing.Partition{
				indexing.NewPartition(
					indexing.NewBound([]byte{'A'}, true),
					indexing.NewBound([]byte{'B'}, false),
				),
			},
			expectedCount: 27,
			expectedRanges: []uint64Range{
				{
					low:  indexing.NewBound[uint64](1, false),
					high: indexing.NewBound[uint64](27, false),
				},
			},
		},
		{
			name: "[X, Y)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					indexing.NewBound([]byte{'X'}, false),
					indexing.NewBound([]byte{'Y'}, true),
				),
			},
			expectedCount: 27,
			expectedRanges: []uint64Range{
				{
					low:  indexing.NewBound[uint64](621, false),
					high: indexing.NewBound[uint64](647, false),
				},
			},
		},
		{
			name: "[A, B), [X, Y)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					indexing.NewBound([]byte{'A'}, false),
					indexing.NewBound([]byte{'B'}, true),
				),
				indexing.NewPartition(
					indexing.NewBound([]byte{'X'}, false),
					indexing.NewBound([]byte{'Y'}, true),
				),
			},
			expectedCount: 54,
			expectedRanges: []uint64Range{
				{
					low:  indexing.NewBound[uint64](0, false),
					high: indexing.NewBound[uint64](26, false),
				},
				{
					low:  indexing.NewBound[uint64](621, false),
					high: indexing.NewBound[uint64](647, false),
				},
			},
		},
		{
			name: "(∞, B)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					indexing.EmptyBound[[]byte](),
					indexing.NewBound([]byte{'B'}, true),
				),
			},
			expectedCount: 27,
			expectedRanges: []uint64Range{
				{
					low:  indexing.NewBound[uint64](0, false),
					high: indexing.NewBound[uint64](26, false),
				},
			},
		},
		{
			name: "[Y, ∞)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					indexing.NewBound([]byte{'Y'}, false),
					indexing.EmptyBound[[]byte](),
				),
			},
			expectedCount: 54,
			expectedRanges: []uint64Range{
				{
					low:  indexing.NewBound[uint64](648, false),
					high: indexing.NewBound[uint64](701, false),
				},
			},
		},
	}

	opts := []badger.IteratorOptions{
		badger.DefaultIteratorOptions,
		{
			PrefetchValues: true,
			PrefetchSize:   100,
			Reverse:        true,
			AllVersions:    false,
		},
	}

	for _, test := range tests {
		for _, opt := range opts {
			t.Run(test.name, func(t *testing.T) {
				it := indexing.LookupPartitions(store, iters.Slice(test.parts), opt)
				defer it.Close()

				actual, err := iters.Collect(it)
				require.NoError(t, err)
				require.Len(t, actual, test.expectedCount)
				require.True(t, areWithinPartitions(actual, test.expectedRanges), "collected keys are not within expected ranges")

				// Checking multi-partition sorting is a bit difficult, so we skip it for now.
				if len(test.expectedRanges) < 2 {
					cmpFunc := bytes.Compare
					if opt.Reverse {
						cmpFunc = func(a, b []byte) int {
							return -bytes.Compare(a, b)
						}
					}
					require.True(t, slices.IsSortedFunc(actual, cmpFunc), "collected keys are not sorted")
				}
			})
		}
	}
}

type uint64Range struct {
	low, high indexing.Bound[uint64]
}

func (r uint64Range) IsWithin(value uint64) bool {
	if !r.low.IsEmpty() {
		if r.low.Exclusive() {
			if value <= r.low.Value() {
				return false
			}
		} else {
			if value < r.low.Value() {
				return false
			}
		}
	}

	if !r.high.IsEmpty() {
		if r.high.Exclusive() {
			if value >= r.high.Value() {
				return false
			}
		} else {
			if value > r.high.Value() {
				return false
			}
		}
	}

	return true
}

func areWithinPartitions(items [][]byte, ranges []uint64Range) bool {
	for _, item := range items {
		if !isWithinPartitions(item, ranges) {
			return false
		}
	}

	return true
}

func isWithinPartitions(item []byte, ranges []uint64Range) bool {
	for _, r := range ranges {
		if r.IsWithin(binary.BigEndian.Uint64(item)) {
			return true
		}
	}

	return false
}
