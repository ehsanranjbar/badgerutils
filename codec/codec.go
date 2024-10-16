package codec

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/badgerutils/codec/be"
)

// Codec is an encoder that encodes reflect.Value to bytes.
type Codec struct{}

// NewReflectEncoder creates a new ReflectEncoder.
func NewReflectEncoder() *Codec {
	return &Codec{}
}

// Encode encodes the given value to bytes.
func (e *Codec) Encode(v any) ([]byte, error) {
	switch v := v.(type) {
	case reflect.Value:
		return e.encodeRV(v)
	default:
		return e.encodeRV(reflect.ValueOf(v))
	}
}

func (e *Codec) encodeRV(v reflect.Value) ([]byte, error) {
	switch v.Kind() {
	case reflect.String:
		return []byte(v.String()), nil
	case reflect.Int, reflect.Int64:
		return be.EncodeInt64Lex(int64(v.Int())), nil
	case reflect.Uint, reflect.Uint64:
		return binary.BigEndian.AppendUint64(nil, v.Uint()), nil
	case reflect.Array, reflect.Slice:
		if v.Elem().Kind() == reflect.Uint8 {
			return v.Bytes(), nil
		}
	case reflect.Pointer:
		return e.encodeRV(v.Elem())
	}

	return nil, fmt.Errorf("unsupported type %s", v.Type())
}

// EncodeMany encodes the given reflect.Value to multiple byte slices to support array and slices.
func (e *Codec) EncodeMany(v reflect.Value) ([][]byte, error) {
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return [][]byte{v.Bytes()}, nil
		}

		var res [][]byte
		for i := 0; i < v.Len(); i++ {
			bzs, err := e.EncodeMany(v.Index(i))
			if err != nil {
				return nil, err
			}

			res = append(res, bzs...)
		}
		return res, nil
	default:
		bz, err := e.encodeRV(v)
		if err != nil {
			return nil, err
		}

		return [][]byte{bz}, nil
	}
}
