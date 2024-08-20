package indexing

import (
	"encoding/binary"

	roaring "github.com/RoaringBitmap/roaring/v2"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/ehsanranjbar/badgerutils"
)

// PopulateRoaring32 populates a roaring bitmap with 32-bit integers from an iterator of byte slices.
func PopulateRoaring32(bm *roaring.Bitmap, iter badgerutils.Iterator[[]byte]) error {
	for iter.Rewind(); iter.Valid(); iter.Next() {
		v, err := iter.Value()
		if err != nil {
			return err
		}
		i := binary.LittleEndian.Uint32(padOrTrim(v, 32))
		bm.Add(i)
	}
	return nil
}

func padOrTrim(b []byte, l int) []byte {
	if len(b) == l {
		return b
	}
	if len(b) > l {
		return b[l-len(b):]
	}
	return append(make([]byte, l-len(b)), b...)
}

// PopulateRoaring64 populates a roaring bitmap with 64-bit integers from an iterator of byte slices.
func PopulateRoaring64(bm *roaring64.Bitmap, iter badgerutils.Iterator[[]byte]) error {
	for iter.Rewind(); iter.Valid(); iter.Next() {
		v, err := iter.Value()
		if err != nil {
			return err
		}
		i := binary.LittleEndian.Uint64(padOrTrim(v, 64))
		bm.Add(i)
	}
	return nil
}
