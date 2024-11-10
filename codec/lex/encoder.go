package lex

import (
	"fmt"
	"reflect"
)

// Encoder encodes values lexicographically.
type Encoder struct{}

// MustEncode encodes the given value and panics if there is an error.
func (e *Encoder) MustEncode(v any) []byte {
	sv, err := e.Encode(v)
	if err != nil {
		panic(err)
	}
	return sv
}

// Encode encodes the given value.
func (e *Encoder) Encode(v any) ([]byte, error) {
	switch v := v.(type) {
	case reflect.Value:
		return e.encodeRV(v)
	default:
		return e.encodeRV(reflect.ValueOf(v))
	}
}

func (e *Encoder) encodeRV(v reflect.Value) ([]byte, error) {
	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			return []byte{0x01}, nil
		}
		return []byte{0x00}, nil
	case reflect.Int8:
		return EncodeInt8(int8(v.Int())), nil
	case reflect.Uint8:
		return []byte{uint8(v.Uint())}, nil
	case reflect.Int16:
		return EncodeInt16(int16(v.Int())), nil
	case reflect.Uint16:
		return EncodeUint16(uint16(v.Uint())), nil
	case reflect.Int32:
		return EncodeInt32(int32(v.Int())), nil
	case reflect.Uint32:
		return EncodeUint32(uint32(v.Uint())), nil
	case reflect.Int, reflect.Int64:
		return EncodeInt64(int64(v.Int())), nil
	case reflect.Uint, reflect.Uint64:
		return EncodeUint64(uint64(v.Uint())), nil
	case reflect.Float32:
		return EncodeFloat32(float32(v.Float())), nil
	case reflect.Float64:
		return EncodeFloat64(v.Float()), nil
	case reflect.String:
		return []byte(v.String()), nil
	case reflect.Array, reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Bytes(), nil
		}
	case reflect.Pointer:
		if v.IsNil() {
			return []byte{}, nil
		}
		return e.encodeRV(v.Elem())
	}

	return nil, fmt.Errorf("unsupported type %s", v.Type())
}
