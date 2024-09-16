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

func (i TestIndexer) Lookup(args ...any) ([]byte, []byte, error) {
	if len(args) < 1 {
		return nil, nil, fmt.Errorf("no arguments")
	}

	switch args[0] {
	case "A":
		b := []byte("A_idx")
		if len(args) > 1 && args[1] != nil {
			b = append(b, binary.LittleEndian.AppendUint64(nil, uint64(-args[1].(int)))...)
		}
		return b, nil, nil
	case "B":
		b := []byte("B_idx")
		if len(args) > 1 && args[1] != nil {
			b = append(b, []byte(args[1].(string))...)
		}
		return b, nil, nil
	}

	return nil, nil, fmt.Errorf("invalid index: %s", args[0])
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

	t.Run("GetByRef_A", func(t *testing.T) {
		for _, obj := range objects {
			lb, _, err := idx.Lookup("A", obj.A)
			require.NoError(t, err)
			k, err := ext.GetByRef(lb)
			require.NoError(t, err)
			v, err := store.Get(k)
			require.NoError(t, err)
			require.Equal(t, obj, v)
		}
	})

	t.Run("GetByRef_B", func(t *testing.T) {
		for _, obj := range objects {
			lb, _, err := idx.Lookup("B", obj.B)
			require.NoError(t, err)
			k, err := ext.GetByRef(lb)
			require.NoError(t, err)
			v, err := store.Get(k)
			require.NoError(t, err)
			require.Equal(t, obj, v)
		}
	})

	t.Run("IterateByRef_A", func(t *testing.T) {
		lb, _, err := idx.Lookup("A", nil)
		require.NoError(t, err)
		it := ext.GetRefIterator(badger.IteratorOptions{
			Prefix: lb,
		})
		defer it.Close()

		actual, err := iters.Collect(it)
		require.NoError(t, err)
		require.Len(t, actual, len(keys))
		require.Equal(t, [][]byte{{3}, {2}, {1}}, actual)
	})

	t.Run("IterateByRef_B", func(t *testing.T) {
		lb, _, err := idx.Lookup("B", nil)
		require.NoError(t, err)
		it := ext.GetRefIterator(badger.IteratorOptions{
			Prefix: lb,
		})
		defer it.Close()

		actual, err := iters.Collect(it)
		require.NoError(t, err)
		require.Len(t, actual, len(keys))
		require.Equal(t, [][]byte{{2}, {3}, {1}}, actual)
	})

	t.Run("GetByRefNotFound", func(t *testing.T) {
		k, err := ext.GetByRef([]byte("not_found"))
		require.Error(t, err)
		require.Nil(t, k)
	})
}
