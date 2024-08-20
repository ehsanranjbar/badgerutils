package serialized

import (
	"encoding"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// Migrate migrates a store from one type to another.
func Migrate[T, U encoding.BinaryMarshaler,
	PT PointerBinaryUnmarshaler[T],
	PU PointerBinaryUnmarshaler[U],
](
	src *Store[T, PT],
	convert func(*T, *badger.Item) (*U, error),
) (*Store[U, PU], error) {
	dst := &Store[U, PU]{base: src.base}

	iter := src.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		v, err := iter.Value()
		if err != nil {
			return nil, fmt.Errorf("failed to get value: %w", err)
		}

		u, err := convert(v, item)
		if err != nil {
			return nil, fmt.Errorf("failed to convert: %w", err)
		}

		err = dst.Set(item.Key(), u)
		if err != nil {
			return nil, fmt.Errorf("failed to set: %w", err)
		}
	}

	return dst, nil
}
