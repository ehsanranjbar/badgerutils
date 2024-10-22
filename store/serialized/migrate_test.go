package serialized_test

import (
	"encoding/json"
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/store/serialized"
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

type StructB struct {
	B string
}

func (s StructB) MarshalBinary() ([]byte, error) {
	return json.Marshal(s)
}

func (s *StructB) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, s)
}

func TestMigrate(t *testing.T) {
	txn := testutil.PrepareTxn(t, true)

	storeA := serialized.New[StructA](txn)

	var (
		keys   = [][]byte{[]byte("1"), []byte("2"), []byte("3")}
		values = []*StructA{{A: 1}, {A: 2}, {A: 3}}
	)

	for i, key := range keys {
		err := storeA.Set(key, values[i])
		require.NoError(t, err)
	}

	storeB, err := serialized.Migrate(storeA, func(a *StructA, item *badger.Item) (*StructB, error) {
		return &StructB{B: fmt.Sprintf("%d", a.A)}, nil
	})
	require.NoError(t, err)

	for i, key := range keys {
		item, value, err := storeB.GetWithItem(key)
		require.NoError(t, err)
		require.NotNil(t, item)
		require.Equal(t, &StructB{B: fmt.Sprintf("%d", values[i].A)}, value)
	}
}
