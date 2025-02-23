package adv

import (
	"bytes"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

// Doc is a Record type that is schemaless.
type Doc[I comparable] map[string]any

// GetId implements the Record interface.
func (doc Doc[I]) GetId() I {
	return doc["_id"].(I)
}

// SetId implements the Record interface.
func (doc Doc[I]) SetId(id I) {
	doc["_id"] = id
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (doc Doc[I]) MarshalBinary() ([]byte, error) {
	enc := msgpack.GetEncoder()
	var buf bytes.Buffer
	enc.Reset(&buf)
	defer msgpack.PutEncoder(enc)

	err := enc.EncodeMap(doc)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (doc *Doc[I]) UnmarshalBinary(data []byte) error {
	dec := msgpack.GetDecoder()
	dec.Reset(bytes.NewReader(data))
	defer msgpack.PutDecoder(dec)

	return dec.Decode(doc)
}
