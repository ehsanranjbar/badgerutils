package extensible

import (
	"encoding"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	sstore "github.com/ehsanranjbar/badgerutils/store/serialized"
)

// Migrate migrates the data from store of type T to store of type U, drop all the old indexes in the src store and
// create new indexes for the dst store.
func Migrate[
	T, U encoding.BinaryMarshaler,
	PT sstore.PointerBinaryUnmarshaler[T],
	PU sstore.PointerBinaryUnmarshaler[U],
](
	src *Store[T, PT],
	exts map[string]Extension[U],
	convert func(*T, *badger.Item) (*U, error),
) (*Store[U, PU], error) {
	if err := src.DropAllExtensions(); err != nil {
		return nil, fmt.Errorf("failed to drop all extensions: %w", err)
	}

	dst := New[U, PU](src.base)

	it := src.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		k := it.Key()
		v, err := it.Value()
		if err != nil {
			return nil, err
		}
		u, err := convert(v, it.Item())
		if err != nil {
			return nil, err
		}

		if err := dst.Set(k, u); err != nil {
			return nil, err
		}
	}

	for name, ext := range exts {
		if err := dst.AddExtension(name, ext); err != nil {
			return nil, fmt.Errorf("failed to add extension %q: %w", name, err)
		}
	}

	return dst, nil
}
