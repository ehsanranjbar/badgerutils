package iters_test

import (
	"encoding/binary"
	"testing"

	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
	"github.com/ehsanranjbar/badgerutils/testutil"
	"github.com/stretchr/testify/require"
)

type TestIndexer struct{}

func (i TestIndexer) Index(v *StructA, update bool) []badgerutils.RawKVPair {
	if v == nil {
		return nil
	}

	return []badgerutils.RawKVPair{
		badgerutils.NewRawKVPair(append([]byte("A_idx"), binary.BigEndian.AppendUint64(nil, uint64(v.A))...), nil),
	}
}

func TestLookupIterator(t *testing.T) {
	store := sstore.New[StructA](nil)

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn)

	var (
		keys   = [][]byte{{1}, {2}}
		values = []*StructA{{A: 1}, {A: 2}}
	)

	for i, key := range keys {
		err := ins.Set(key, values[i])
		require.NoError(t, err)
	}

	iter := iters.Lookup(ins, iters.Slice(keys))
	defer iter.Close()

	actual, err := iters.Collect(iter)
	require.NoError(t, err)
	require.Len(t, actual, 2)
	require.Equal(t, values, actual)
}
