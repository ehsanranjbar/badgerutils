package concat

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/badgerutils/indexing"
	"github.com/ehsanranjbar/badgerutils/utils/be"
	"github.com/ehsanranjbar/badgerutils/utils/reflecthelpers"
)

var (
	DefaultStringSize = 256
	DefaultBytesSize  = 256
)

// Component represents a component of concatenated keys in index
type Component struct {
	path       string
	descending bool
	size       int
	encodeFunc be.EncodeFunc
}

// NewComponent creates a new component with the given path.
func NewComponent(path string) Component {
	return Component{path: path}
}

// Desc sets the descending flag of the component.
func (comp Component) Desc() Component {
	comp.descending = true
	return comp
}

// Size modifies the size of the component.
func (comp Component) Size(size int) Component {
	if size <= 0 {
		panic("size must be positive")
	}

	comp.size = size
	return comp
}

// WithEncodeFunc sets a custom encode function as the encoder of the component.
func (comp Component) WithEncodeFunc(f be.EncodeFunc) Component {
	comp.encodeFunc = f
	return comp
}

func (comp Component) verify(pt reflect.Type) (*verifiedComponent, error) {
	field, index, err := reflecthelpers.ExtractPath(pt, comp.path)
	if err != nil {
		return nil, fmt.Errorf("invalid component path %s: %w", comp.path, err)
	}
	bt := reflecthelpers.GetBaseType(field.Type)

	vt := &verifiedComponent{
		Component:   comp,
		fieldIndex:  index,
		reflectType: field.Type,
		elemKind:    reflecthelpers.GetElemType(bt).Kind(),
	}

	if vt.encodeFunc == nil {
		vt.encodeFunc = vt.findDefaultEncodeFunc(bt)
	}

	return vt, nil
}

func (vt *verifiedComponent) findDefaultEncodeFunc(t reflect.Type) be.EncodeFunc {
	if (t.Kind() == reflect.Array || t.Kind() == reflect.Slice) && t.Elem().Kind() == reflect.Uint8 {
		if vt.size == 0 {
			vt.size = DefaultBytesSize
		}
		return encodeSizedBytes(vt.size)
	}

	for t.Kind() == reflect.Array || t.Kind() == reflect.Slice {
		vt.array = true
		t = t.Elem()
	}

	if t.Kind() == reflect.String {
		if vt.size == 0 {
			vt.size = DefaultStringSize
		}
		return encodeSizedString(vt.size)
	}

	return be.GetEncodeFunc(t)
}

func encodeSizedString(n int) be.EncodeFunc {
	return func(v any) ([]byte, error) {
		return be.PadOrTrimRight([]byte(v.(string)), n), nil
	}
}

func encodeSizedBytes(n int) be.EncodeFunc {
	return func(v any) ([]byte, error) {
		return be.PadOrTrimRight(v.([]byte), n), nil
	}
}

type verifiedComponent struct {
	Component

	fieldIndex  []int
	reflectType reflect.Type
	array       bool
	elemKind    reflect.Kind
}

func (vc *verifiedComponent) encode(v any) ([][]byte, error) {
	if vc.array {
		return vc.encodeArray(v)
	}

	bz, err := vc.encodeValue(v)
	if err != nil {
		return nil, err
	}

	return [][]byte{bz}, nil
}

func (vc *verifiedComponent) encodeArray(v any) ([][]byte, error) {
	rv := reflect.ValueOf(v)

	l := rv.Len()
	bzs := make([][]byte, l)
	for i := 0; i < l; i++ {
		bz, err := vc.encodeValue(rv.Index(i).Interface())
		if err != nil {
			return nil, fmt.Errorf("failed to encode array element: %w", err)
		}
		bzs[i] = bz
	}

	return bzs, nil
}

func (vc *verifiedComponent) encodeValue(v any) ([]byte, error) {
	bz, err := vc.encodeFunc(v)
	if err != nil {
		return nil, fmt.Errorf("failed to encode value: %w", err)
	}

	if vc.descending {
		return be.Inverse(bytes.Clone(bz)), nil
	}

	return bz, nil
}

// encodeBounds encodes the given bounds to byte slices.
func (vc *verifiedComponent) encodeBounds(low, high indexing.Bound[any]) ([]byte, []byte, error) {
	var (
		lowBz, highBz []byte
		err           error
	)
	if low.IsEmpty() {
		lowBz = make([]byte, vc.size)
	} else {
		lowBz, err = vc.encodeValue(low.Value())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode low bound: %w", err)
		}
	}
	if high.IsEmpty() {
		highBz = bytes.Repeat([]byte{0xff}, vc.size)
	} else {
		highBz, err = vc.encodeValue(high.Value())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode high bound: %w", err)
		}
	}

	return lowBz, highBz, nil
}
