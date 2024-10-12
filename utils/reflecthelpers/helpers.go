package reflecthelpers

import (
	"fmt"
	"reflect"
	"strings"
)

// GetBaseType returns the base type of the given type by dereferencing pointers.
func GetBaseType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

// GetElemType returns the element type of the given type by dereferencing pointers, arrays, and slices.
func GetElemType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Array || t.Kind() == reflect.Slice {
		t = t.Elem()
	}
	return t
}

// ExtractPath extracts the field and index of the given path from the given type.
func ExtractPath(t reflect.Type, path string) (reflect.StructField, []int, error) {
	fields := strings.Split(path, ".")

	var field reflect.StructField
	var index []int
	for _, f := range fields {
		var ok bool
		field, ok = GetBaseType(t).FieldByName(f)
		if !ok {
			return reflect.StructField{}, nil, fmt.Errorf("field %s not found", f)
		}
		t = field.Type
		index = append(index, field.Index...)
	}

	return field, index, nil
}

// SafeFieldByIndex returns the field value of the given value by index.
func SafeFieldByIndex(v reflect.Value, index []int) (reflect.Value, bool) {
	for _, i := range index {
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, true
}
