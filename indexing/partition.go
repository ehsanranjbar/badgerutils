package indexing

import (
	"bytes"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
)

type Partition struct {
	Type PartitionType
	Low  []byte
	High []byte
}

type PartitionType int

const (
	PartitionTypeInvalid PartitionType = iota
	PartitionTypePrefix                // Using low as prefix
	PartitionTypeRange                 // Using low and high as range
)

func NewPrefixPartition(low []byte) Partition {
	return Partition{
		Type: PartitionTypePrefix,
		Low:  low,
	}
}

func NewRangePartition(low, high []byte) Partition {
	return Partition{
		Type: PartitionTypeRange,
		Low:  low,
		High: high,
	}
}

func NewPartitionLookupIterator(
	store badgerutils.BadgerStore,
	ranges badgerutils.Iterator[Partition],
	opts badger.IteratorOptions,
) badgerutils.Iterator[[]byte] {
	return &PartitionLookupIterator{
		store:      refstore.New(store),
		partitions: ranges,
		opts:       opts,
		iter:       nil,
	}
}

type PartitionLookupIterator struct {
	store      *refstore.Store
	partitions badgerutils.Iterator[Partition]
	current    Partition
	opts       badger.IteratorOptions
	iter       badgerutils.Iterator[[]byte]
}

func (i *PartitionLookupIterator) Close() {
	if i.iter != nil {
		i.iter.Close()
	}
	i.partitions.Close()
}

func (i *PartitionLookupIterator) Item() *badger.Item {
	if i.iter != nil {
		return i.iter.Item()
	}

	return nil
}

func (i *PartitionLookupIterator) Rewind() {
	i.partitions.Rewind()

	i.loadCurrent()
}

func (i *PartitionLookupIterator) loadCurrent() {
	if i.iter != nil {
		i.iter.Close()
	}

	if i.partitions.Valid() {
		i.current, _ = i.partitions.Value()
		var prefix []byte
		if i.current.Type == PartitionTypePrefix {
			prefix = i.current.Low
		}
		i.iter = i.store.NewIterator(badger.IteratorOptions{
			PrefetchSize:   i.opts.PrefetchSize,
			PrefetchValues: i.opts.PrefetchValues,
			Reverse:        i.opts.Reverse,
			AllVersions:    i.opts.AllVersions,
			InternalAccess: i.opts.InternalAccess,
			Prefix:         prefix,
			SinceTs:        i.opts.SinceTs,
		})

		if i.current.Type == PartitionTypePrefix {
			i.iter.Rewind()
		} else {
			i.iter.Seek(i.current.Low)
		}

		i.partitions.Next()
	}
}

func (i *PartitionLookupIterator) Seek(key []byte) {}

func (i *PartitionLookupIterator) Valid() bool {
	return i.partitions.Valid() || i.isCurrentValid()
}

func (i *PartitionLookupIterator) isCurrentValid() bool {
	return i.iter != nil && i.iter.Valid() && (i.current.Type == PartitionTypePrefix || i.isInRange())
}

func (i *PartitionLookupIterator) isInRange() bool {
	return i.current.High == nil || bytes.Compare(i.iter.Key(), i.current.High) <= 0
}

func (i *PartitionLookupIterator) Next() {
	i.iter.Next()
	if i.isCurrentValid() {
		return
	}

	i.loadCurrent()
}

func (i *PartitionLookupIterator) Key() []byte {
	if i.iter != nil {
		return i.iter.Key()
	}

	return nil
}

func (i *PartitionLookupIterator) Value() ([]byte, error) {
	if i.iter != nil {
		return i.iter.Value()
	}

	return nil, nil
}
