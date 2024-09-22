package indexing_test

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/iters"
	extstore "github.com/ehsanranjbar/badgerutils/store/extensible"
	"github.com/stretchr/testify/require"
)

type TestStruct struct {
	A int
	B string
}

func (t TestStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *TestStruct) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

type TestIndexer struct{}

func (i TestIndexer) Index(v *TestStruct, update bool) []badgerutils.RawKVPair {
	if v == nil {
		return nil
	}

	return []badgerutils.RawKVPair{
		badgerutils.NewRawKVPair(append([]byte("A_idx"), binary.LittleEndian.AppendUint64(nil, uint64(-v.A))...), nil),
		badgerutils.NewRawKVPair(append([]byte("B_idx"), []byte(v.B)...), nil),
	}
}

func (i TestIndexer) Lookup(args ...any) (badgerutils.Iterator[indexing.Partition], error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("no arguments")
	}

	switch args[0] {
	case "A":
		b := []byte("A_idx")
		if len(args) > 1 && args[1] != nil {
			b = append(b, binary.LittleEndian.AppendUint64(nil, uint64(-args[1].(int)))...)
		}
		return iters.Slice([]indexing.Partition{indexing.NewPrefixPartition(b)}), nil
	case "B":
		b := []byte("B_idx")
		if len(args) > 1 && args[1] != nil {
			b = append(b, []byte(args[1].(string))...)
		}
		return iters.Slice([]indexing.Partition{indexing.NewPrefixPartition(b)}), nil
	}

	return nil, fmt.Errorf("invalid index: %s", args[0])
}

func TestObjectStore(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := extstore.New[TestStruct](txn)
	idx := TestIndexer{}
	ext := indexing.NewExtension(idx)
	store.AddExtension("test", ext)

	var (
		keys    = [][]byte{{1}, {2}, {3}}
		objects = []*TestStruct{
			{A: 1, B: "foo"},
			{A: 2, B: "bar"},
			{A: 3, B: "baz"},
		}
	)

	for i, key := range keys {
		err := store.Set(key, objects[i])
		require.NoError(t, err)
	}

	t.Run("Lookup_A", func(t *testing.T) {
		it, err := ext.Lookup(badger.DefaultIteratorOptions, "A", nil)
		require.NoError(t, err)
		defer it.Close()

		actual, err := iters.Collect(it)
		require.NoError(t, err)
		require.Len(t, actual, len(keys))
		require.Equal(t, [][]byte{{3}, {2}, {1}}, actual)
	})

	t.Run("Lookup_B", func(t *testing.T) {
		it, err := ext.Lookup(badger.DefaultIteratorOptions, "B", nil)
		require.NoError(t, err)
		defer it.Close()

		actual, err := iters.Collect(it)
		require.NoError(t, err)
		require.Len(t, actual, len(keys))
		require.Equal(t, [][]byte{{2}, {3}, {1}}, actual)
	})
}
