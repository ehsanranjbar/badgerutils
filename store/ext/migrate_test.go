package ext_test

import (
	"encoding/binary"
	"encoding/json"
	"strconv"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/iters"
	extstore "github.com/ehsanranjbar/badgerutils/store/ext"
	"github.com/ehsanranjbar/badgerutils/testutil"
	"github.com/stretchr/testify/require"
)

type StructA struct {
	A int
}

func (s StructA) MarshalBinary() ([]byte, error) {
	return json.Marshal(s)
}

func (s *StructA) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, s)
}

type StructAIndexer struct{}

func (i StructAIndexer) Index(v *StructA, update bool) ([]badgerutils.RawKVPair, error) {
	if v == nil {
		return nil, nil
	}

	return []badgerutils.RawKVPair{
		badgerutils.NewRawKVPair(append([]byte("A_idx"), binary.BigEndian.AppendUint64(nil, uint64(v.A))...), nil),
	}, nil
}

func (i StructAIndexer) Lookup(args ...any) (badgerutils.Iterator[indexing.Partition], error) {
	return nil, nil
}

type StructB struct {
	B string
}

func (s StructB) MarshalBinary() ([]byte, error) {
	return json.Marshal(s)
}

func (s *StructB) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, s)
}

type StructBIndexer struct{}

func (i StructBIndexer) Index(v *StructB, update bool) ([]badgerutils.RawKVPair, error) {
	if v == nil {
		return nil, nil
	}

	return []badgerutils.RawKVPair{
		badgerutils.NewRawKVPair(append([]byte("B_idx1"), []byte(v.B)...), nil),
		badgerutils.NewRawKVPair(append([]byte("B_idx2"), []byte(v.B)...), nil),
	}, nil
}

func (i StructBIndexer) Lookup(args ...any) (badgerutils.Iterator[indexing.Partition], error) {
	return nil, nil
}

func TestMigrate(t *testing.T) {
	txn := testutil.PrepareTxn(t, true)

	storeA := extstore.New[StructA](txn)
	err := storeA.AddExtension("A", indexing.NewExtension(StructAIndexer{}))
	require.NoError(t, err)

	var (
		keys   = [][]byte{[]byte("1"), []byte("2"), []byte("3")}
		values = []*StructA{{A: 1}, {A: 2}, {A: 3}}
	)

	for i, key := range keys {
		err = storeA.Set(key, values[i])
		require.NoError(t, err)
	}

	storeB, err := extstore.Migrate(
		storeA,
		map[string]extstore.Extension[StructB]{
			"B": indexing.NewExtension(StructBIndexer{}),
		},
		func(t *StructA, i *badger.Item) (*StructB, error) {
			return &StructB{B: strconv.Itoa(t.A)}, nil
		},
	)
	require.NoError(t, err)

	for i, key := range keys {
		v, err := storeB.Get(key)
		require.NoError(t, err)
		require.Equal(t, &StructB{B: strconv.Itoa(values[i].A)}, v)
	}

	// Check extension stores
	count := iters.ConsumeAndCount(txn.NewIterator(badger.IteratorOptions{
		Prefix: []byte("ext"),
	}))
	require.Equal(t, uint(7), count)
}
