package iters_test

import (
	"encoding/json"
	"strconv"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/iters"
	"github.com/stretchr/testify/require"
)

type StructA struct {
	A int
}

func (t StructA) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *StructA) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

type StructB struct {
	B string
}

func (t StructB) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *StructB) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

func TestMap(t *testing.T) {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	store := badgerutils.NewSerializedStore[StructA](txn)

	var (
		keys   = [][]byte{[]byte("foo1"), []byte("foo2")}
		values = []*StructA{{A: 1}, {A: 2}}
	)

	for i, key := range keys {
		err := store.Set(key, values[i])
		require.NoError(t, err)
	}

	iter := iters.Map(store.NewIterator(badger.DefaultIteratorOptions), func(v *StructA) StructB {
		return StructB{B: strconv.Itoa(v.A)}
	})
	defer iter.Close()

	i := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		require.NoError(t, err)
		require.Equal(t, strconv.Itoa(values[i].A), value.B)
		i++
	}
}
