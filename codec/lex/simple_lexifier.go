package lex

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/badgerutils/codec/be"
)

type simpleValue []byte

// Invert implements the Value interface.
func (v simpleValue) Invert() Value {
	return simpleValue(Invert(bytes.Clone(v)))
}

// Size implements the Value interface.
func (v simpleValue) Size() int {
	return len(v)
}

// Resize implements the Value interface.
func (v simpleValue) Resize(n int) Value {
	return simpleValue(be.PadOrTruncRight(v, n))
}

func (v simpleValue) MarshalBinary() ([]byte, error) {
	return v, nil
}

// SimpleLexifier is a codec that encodes and decodes values lexicographically.
type SimpleLexifier struct{}

// MustLexifyAny encodes the given value to lex.Value and panics if there is an error.
func (e *SimpleLexifier) MustLexifyAny(v any) Value {
	sv, err := e.LexifyAny(v)
	if err != nil {
		panic(err)
	}
	return sv
}

// LexifyAny encodes the given value to lex.Value.
func (e *SimpleLexifier) LexifyAny(v any) (Value, error) {
	switch v := v.(type) {
	case reflect.Value:
		return e.LexifyRV(v)
	default:
		return e.LexifyRV(reflect.ValueOf(v))
	}
}

// LexifyRV encodes the given reflect.Value to lex.Value.
func (e *SimpleLexifier) LexifyRV(v reflect.Value) (Value, error) {
	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			return simpleValue{1}, nil
		}
		return simpleValue{0}, nil
	case reflect.Int, reflect.Int64:
		return simpleValue(EncodeInt64(int64(v.Int()))), nil
	case reflect.Uint, reflect.Uint64:
		return simpleValue(EncodeUint64(uint64(v.Uint()))), nil
	case reflect.Float32:
		return simpleValue(EncodeFloat32(float32(v.Float()))), nil
	case reflect.Float64:
		return simpleValue(EncodeFloat64(v.Float())), nil
	case reflect.String:
		return simpleValue(v.String()), nil
	case reflect.Array, reflect.Slice:
		if v.Elem().Kind() == reflect.Uint8 {
			return simpleValue(v.Bytes()), nil
		}
	case reflect.Pointer:
		if v.IsNil() {
			return simpleValue([]byte{}), nil
		}
		return e.LexifyRV(v.Elem())
	}

	return nil, fmt.Errorf("unsupported type %s", v.Type())
}

// LexifyRVMulti encodes the given reflect.Value to multiple lex.Value to support array and slices.
func (e *SimpleLexifier) LexifyRVMulti(v reflect.Value) ([]Value, error) {
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return []Value{simpleValue(v.Bytes())}, nil
		}

		var res []Value
		for i := 0; i < v.Len(); i++ {
			svs, err := e.LexifyRVMulti(v.Index(i))
			if err != nil {
				return nil, err
			}

			res = append(res, svs...)
		}
		return res, nil
	default:
		sv, err := e.LexifyRV(v)
		if err != nil {
			return nil, err
		}

		return []Value{sv}, nil
	}
}
