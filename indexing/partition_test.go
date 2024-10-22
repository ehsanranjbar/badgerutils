package indexing_test

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/exprs"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/iters"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
	"github.com/ehsanranjbar/badgerutils/testutil"
	"github.com/stretchr/testify/require"
)

func TestLookupPartitions(t *testing.T) {
	txn := testutil.PrepareTxn(t, true)

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
		expectedRanges []exprs.Range[uint64]
	}{
		{
			name: "[0x00, ∞)",
			parts: []indexing.Partition{
				indexing.NewPartition(nil, nil),
			},
			expectedCount: 702,
			expectedRanges: []exprs.Range[uint64]{
				exprs.NewRange(exprs.NewBound[uint64](0, false), exprs.NewBound[uint64](702, false)),
			},
		},
		{
			name: "[A, B)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound([]byte{'A'}, false),
					exprs.NewBound([]byte{'B'}, true),
				),
			},
			expectedCount: 27,
			expectedRanges: []exprs.Range[uint64]{
				exprs.NewRange(exprs.NewBound[uint64](0, false), exprs.NewBound[uint64](26, false)),
			},
		},
		{
			name: "(A, B]",
			parts: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound([]byte{'A'}, true),
					exprs.NewBound([]byte{'B'}, false),
				),
			},
			expectedCount: 27,
			expectedRanges: []exprs.Range[uint64]{
				exprs.NewRange(exprs.NewBound[uint64](1, false), exprs.NewBound[uint64](27, false)),
			},
		},
		{
			name: "[X, Y)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound([]byte{'X'}, false),
					exprs.NewBound([]byte{'Y'}, true),
				),
			},
			expectedCount: 27,
			expectedRanges: []exprs.Range[uint64]{
				exprs.NewRange(exprs.NewBound[uint64](621, false), exprs.NewBound[uint64](647, false)),
			},
		},
		{
			name: "[A, B), [X, Y)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound([]byte{'A'}, false),
					exprs.NewBound([]byte{'B'}, true),
				),
				indexing.NewPartition(
					exprs.NewBound([]byte{'X'}, false),
					exprs.NewBound([]byte{'Y'}, true),
				),
			},
			expectedCount: 54,
			expectedRanges: []exprs.Range[uint64]{
				exprs.NewRange(exprs.NewBound[uint64](0, false), exprs.NewBound[uint64](26, false)),
				exprs.NewRange(exprs.NewBound[uint64](621, false), exprs.NewBound[uint64](647, false)),
			},
		},
		{
			name: "(∞, B)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					nil,
					exprs.NewBound([]byte{'B'}, true),
				),
			},
			expectedCount: 27,
			expectedRanges: []exprs.Range[uint64]{
				exprs.NewRange(exprs.NewBound[uint64](0, false), exprs.NewBound[uint64](26, false)),
			},
		},
		{
			name: "[Y, ∞)",
			parts: []indexing.Partition{
				indexing.NewPartition(
					exprs.NewBound([]byte{'Y'}, false),
					nil,
				),
			},
			expectedCount: 54,
			expectedRanges: []exprs.Range[uint64]{
				exprs.NewRange(exprs.NewBound[uint64](648, false), exprs.NewBound[uint64](701, false)),
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

func areWithinPartitions(items [][]byte, ranges []exprs.Range[uint64]) bool {
	for _, item := range items {
		if !isWithinPartitions(item, ranges) {
			return false
		}
	}

	return true
}

func isWithinPartitions(item []byte, ranges []exprs.Range[uint64]) bool {
	for _, r := range ranges {
		if isWithin(r, binary.BigEndian.Uint64(item)) {
			return true
		}
	}

	return false
}

func isWithin(r exprs.Range[uint64], value uint64) bool {
	if !r.Low().IsEmpty() {
		if r.Low().Exclusive() {
			if value <= r.Low().Value() {
				return false
			}
		} else {
			if value < r.Low().Value() {
				return false
			}
		}
	}

	if !r.High().IsEmpty() {
		if r.High().Exclusive() {
			if value >= r.High().Value() {
				return false
			}
		} else {
			if value > r.High().Value() {
				return false
			}
		}
	}

	return true
}
