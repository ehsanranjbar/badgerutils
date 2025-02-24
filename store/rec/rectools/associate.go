package rectools

import (
	"context"
	"errors"
	"fmt"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils"
	"github.com/ehsanranjbar/badgerutils/codec"
	"github.com/ehsanranjbar/badgerutils/iters"
	extstore "github.com/ehsanranjbar/badgerutils/store/ext"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
	recstore "github.com/ehsanranjbar/badgerutils/store/rec"
	refstore "github.com/ehsanranjbar/badgerutils/store/ref"
)

var dataStorePrefix = []byte{'d'}

// Association is a 1-n relation between entities of two store (parent and child).
type Association[
	PI comparable,
	PT any,
	PR recstore.Record[PI, PT],
	CI comparable,
	CT any,
	CR recstore.Record[CI, CT],
] struct {
	name         string
	parentStore  *recstore.Store[PI, PT, PR]
	childStore   *recstore.Store[CI, CT, CR]
	allowOrphans bool
	pidFunc      func(*CT) (PI, error)
	p2c          *refstore.Store
	c2p          *refstore.Store
	init         sync.Once
	initialized  bool
}

// Associate relates two stores in a parent-child relation.
func Associate[
	PI comparable,
	PT any,
	PR recstore.Record[PI, PT],
	CI comparable,
	CT any,
	CR recstore.Record[CI, CT],
](
	name string,
	parentStore *recstore.Store[PI, PT, PR],
	childStore *recstore.Store[CI, CT, CR],
) *Association[PI, PT, PR, CI, CT, CR] {
	asc := &Association[PI, PT, PR, CI, CT, CR]{
		name:        name,
		parentStore: parentStore,
		childStore:  childStore,
	}

	parentStore.WithExtension(name, &associationPExt[PI, PT, PR, CI, CT, CR]{asc: asc})
	childStore.WithExtension(name, &associationCExt[PI, PT, PR, CI, CT, CR]{asc: asc})

	return asc
}

// AllowOrphans allows children to exist without a parent.
func (a *Association[PI, PT, PR, CI, CT, CR]) AllowOrphans() *Association[PI, PT, PR, CI, CT, CR] {
	if a.initialized {
		panic("association already initialized")
	}

	a.allowOrphans = true
	return a
}

// WithPIDFunc sets a function to get parent id from child.
func (a *Association[PI, PT, PR, CI, CT, CR]) WithPIDFunc(f func(*CT) (PI, error)) *Association[PI, PT, PR, CI, CT, CR] {
	if a.initialized {
		panic("association already initialized")
	}

	a.pidFunc = f
	return a
}

// Name returns the name of the association.
func (a *Association[PI, PT, PR, CI, CT, CR]) Name() string {
	return a.name
}

// Instantiate creates a new Instance.
func (a *Association[PI, PT, PR, CI, CT, CR]) Instantiate(txn *badger.Txn) *AssociationInstance[PI, PT, PR, CI, CT, CR] {
	a.init.Do(func() {
		a.initialized = true
	})

	return &AssociationInstance[PI, PT, PR, CI, CT, CR]{
		name:        a.name,
		parentStore: a.parentStore.Instantiate(txn),
		pidCodec:    a.parentStore.IdCodec(),
		childStore:  a.childStore.Instantiate(txn),
		cidCodec:    a.childStore.IdCodec(),
		pidFunc:     a.pidFunc,
		p2c:         a.p2c.Instantiate(txn).(*refstore.Instance),
		c2p:         a.c2p.Instantiate(txn).(*refstore.Instance),
	}
}

// AssociationInstance is an instance of Association that can be used to interact with the stores.
type AssociationInstance[
	PI comparable,
	PT any,
	PR recstore.Record[PI, PT],
	CI comparable,
	CT any,
	CR recstore.Record[CI, CT],
] struct {
	name        string
	parentStore *recstore.Instance[PI, PT, PR]
	pidCodec    codec.Codec[PI]
	childStore  *recstore.Instance[CI, CT, CR]
	cidCodec    codec.Codec[CI]
	pidFunc     func(*CT) (PI, error)
	p2c         *refstore.Instance
	c2p         *refstore.Instance
}

// Name returns the name of the association.
func (a *AssociationInstance[PI, PT, PR, CI, CT, CR]) Name() string {
	return a.name
}

// Set sets the parent and children of the given parent id.
func (a *AssociationInstance[PI, PT, PR, CI, CT, CR]) Set(p *PT, cs ...*CT) error {
	if p != nil {
		opts := make([]any, 0, len(cs))
		for _, c := range cs {
			opts = append(opts, extstore.WithExtOption(a.name, c))
		}

		return a.parentStore.Set(p, opts...)
	}

	for _, c := range cs {
		err := a.childStore.Set(c)
		if err != nil {
			return fmt.Errorf("failed to set child: %w", err)
		}
	}

	return nil
}

// GetParent returns the parent entity of the given child id.
func (a *AssociationInstance[PI, PT, PR, CI, CT, CR]) GetParent(cid CI) (*PT, error) {
	pid, err := a.GetParentId(cid)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return a.parentStore.Get(pid)
}

// GetParentId returns the parent id of the given child id.
func (a *AssociationInstance[PI, PT, PR, CI, CT, CR]) GetParentId(cid CI) (pid PI, err error) {
	ck, err := a.cidCodec.Encode(cid)
	if err != nil {
		return pid, fmt.Errorf("failed to encode child id: %w", err)
	}

	pk, err := a.c2p.Get(ck)
	if err != nil {
		return pid, fmt.Errorf("failed to get parent key: %w", err)
	}

	pid, err = a.pidCodec.Decode(pk)
	if err != nil {
		return pid, fmt.Errorf("failed to decode parent id: %w", err)
	}

	return pid, nil
}

// GetChildren returns the children of the given parent id.
func (a *AssociationInstance[PI, PT, PR, CI, CT, CR]) GetChildrenIterator(pid PI, opts badger.IteratorOptions) (badgerutils.Iterator[CI, *CT], error) {
	pk, err := a.pidCodec.Encode(pid)
	if err != nil {
		return nil, fmt.Errorf("failed to encode parent id: %w", err)
	}

	opts.Prefix = append(pk, opts.Prefix...)
	it := iters.Lookup(
		a.childStore,
		iters.Map(
			a.p2c.NewIterator(opts),
			func(k []byte, _ *badger.Item) (CI, error) {
				cid, err := a.cidCodec.Decode(k)
				if err != nil {
					return cid, fmt.Errorf("failed to decode child id: %w", err)
				}

				return cid, nil
			},
		),
	)

	return it, nil
}

type associationPExt[
	PI comparable,
	PT any,
	PR recstore.Record[PI, PT],
	CI comparable,
	CT any,
	CR recstore.Record[CI, CT],
] struct {
	asc *Association[PI, PT, PR, CI, CT, CR]
}

func (e associationPExt[PI, PT, PR, CI, CT, CR]) Instantiate(txn *badger.Txn) extstore.ExtensionInstance[PT] {
	e.asc.init.Do(func() {
		e.asc.initialized = true
	})

	return &associationPExtIns[PI, PT, PR, CI, CT, CR]{
		name:       e.asc.name,
		cidCodec:   e.asc.childStore.IdCodec(),
		childStore: e.asc.childStore.Instantiate(txn),
		p2c:        e.asc.p2c.Instantiate(txn).(*refstore.Instance),
	}
}

func (e associationPExt[PI, PT, PR, CI, CT, CR]) RegisterStore(store badgerutils.Instantiator[badgerutils.BadgerStore]) {
	e.asc.p2c = refstore.New(store)
}

type associationPExtIns[
	PI comparable,
	PT any,
	PR recstore.Record[PI, PT],
	CI comparable,
	CT any,
	CR recstore.Record[CI, CT],
] struct {
	name       string
	cidCodec   codec.Codec[CI]
	childStore *recstore.Instance[CI, CT, CR]
	p2c        *refstore.Instance
}

func (e *associationPExtIns[PI, PT, PR, CI, CT, CR]) OnDelete(_ context.Context, key []byte, value *PT) error {
	it := e.p2c.NewIterator(badger.IteratorOptions{
		PrefetchSize: 100,
		Prefix:       key,
	})
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		ck, _ := it.Value()
		cid, err := e.cidCodec.Decode(ck)
		if err != nil {
			return fmt.Errorf("failed to decode child id: %w", err)
		}

		err = e.childStore.Delete(cid)
		if err != nil {
			return fmt.Errorf("failed to delete child: %w", err)
		}
	}

	return nil
}

func (e *associationPExtIns[PI, PT, PR, CI, CT, CR]) OnSet(_ context.Context, _ []byte, _, new *PT, opts ...any) error {
	childs := findAs[*CT](opts)
	for _, child := range childs {
		pid := PR(new).GetId()
		err := e.childStore.Set(
			child,
			extstore.WithExtOption(e.name, pid),
			extstore.WithExtOption(e.name, associationPIDSkipCheckFlag{}),
		)
		if err != nil {
			return fmt.Errorf("failed to set child: %w", err)
		}
	}

	return nil
}

type associationPIDSkipCheckFlag struct{}

type associationCExt[
	PI comparable,
	PT any,
	PR recstore.Record[PI, PT],
	CI comparable,
	CT any,
	CR recstore.Record[CI, CT],
] struct {
	asc *Association[PI, PT, PR, CI, CT, CR]
}

func (e associationCExt[PI, PT, PR, CI, CT, CR]) Instantiate(txn *badger.Txn) extstore.ExtensionInstance[CT] {
	e.asc.init.Do(func() {
		e.asc.initialized = true
	})

	return &associationCExtIns[PI, PT, PR, CI, CT, CR]{
		name:     e.asc.name,
		pidCodec: e.asc.parentStore.IdCodec(),
		// Trick to avoid cyclic dependency since we only need to check if the parent exists.
		parentStore: pstore.New(nil, append(e.asc.parentStore.Prefix(), dataStorePrefix...)).Instantiate(txn),
		pidFunc:     e.asc.pidFunc,
		p2c:         e.asc.p2c.Instantiate(txn).(*refstore.Instance),
		c2p:         e.asc.c2p.Instantiate(txn).(*refstore.Instance),
		allowOrphan: e.asc.allowOrphans,
	}
}

func (e associationCExt[PI, PT, PR, CI, CT, CR]) RegisterStore(store badgerutils.Instantiator[badgerutils.BadgerStore]) {
	e.asc.c2p = refstore.New(store)
}

type associationCExtIns[
	PI comparable,
	PT any,
	PR recstore.Record[PI, PT],
	CI comparable,
	CT any,
	CR recstore.Record[CI, CT],
] struct {
	name        string
	pidCodec    codec.Codec[PI]
	parentStore badgerutils.BadgerStore
	pidFunc     func(*CT) (PI, error)
	p2c         *refstore.Instance
	c2p         *refstore.Instance
	allowOrphan bool
}

func (e *associationCExtIns[PI, PT, PR, CI, CT, CR]) OnDelete(_ context.Context, key []byte, value *CT) error {
	pid, err := e.getParentId(key, value)
	if err != nil {
		return err
	}

	// Without pidFunc there should be a ref in c2p store.
	if e.pidFunc == nil {
		err = e.c2p.Delete(key)
		if err != nil {
			return fmt.Errorf("failed to delete ref from c2p store: %w", err)
		}
	}

	pk, err := e.pidCodec.Encode(pid)
	if err != nil {
		return fmt.Errorf("failed to encode parent id: %w", err)
	}
	err = e.p2c.Delete(append(pk, key...))
	if err != nil {
		return fmt.Errorf("failed to delete refs from p2c store: %w", err)
	}

	return nil
}

func (e *associationCExtIns[PI, PT, PR, CI, CT, CR]) getParentId(ck []byte, c *CT) (pid PI, err error) {
	if e.pidFunc != nil {
		pid, err = e.pidFunc(c)
		if err != nil {
			return pid, fmt.Errorf("failed to get parent id: %w", err)
		}
	} else {
		pk, err := e.c2p.Get(ck)
		if err != nil {
			return pid, fmt.Errorf("failed to get parent key from c2p store: %w", err)
		}

		pid, err = e.pidCodec.Decode(pk)
		if err != nil {
			return pid, fmt.Errorf("failed to decode parent id: %w", err)
		}
	}

	return pid, nil
}

func (e *associationCExtIns[PI, PT, PR, CI, CT, CR]) OnSet(_ context.Context, key []byte, _, new *CT, opts ...any) error {
	var (
		pid PI
		err error
	)
	if e.pidFunc != nil {
		pid, err = e.pidFunc(new)
		if err != nil {
			return fmt.Errorf("failed to get parent id: %w", err)
		}
	} else {
		var ok bool
		pid, ok = findOneAs[PI](opts)
		if !ok {
			if e.allowOrphan {
				return nil
			}

			return fmt.Errorf("parent id not given as option with no pidFunc")
		}
	}

	pk, err := e.pidCodec.Encode(pid)
	if err != nil {
		return fmt.Errorf("failed to encode parent id: %w", err)
	}

	if _, ok := findOneAs[associationPIDSkipCheckFlag](opts); !ok {
		_, err = e.parentStore.Get(pk)
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("parent not found: %w", err)
		}
		if err != nil {
			return fmt.Errorf("failed to get parent: %w", err)
		}
	}

	err = e.p2c.Set(key, refstore.NewRefEntry(pk))
	if err != nil {
		return fmt.Errorf("failed to set ref in p2c store: %w", err)
	}

	if e.pidFunc == nil {
		err = e.c2p.Set(pk, refstore.NewRefEntry(key))
		if err != nil {
			return fmt.Errorf("failed to set ref in c2p store: %w", err)
		}
	}

	return nil
}
