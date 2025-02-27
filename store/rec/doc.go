package rec

import (
	"bytes"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

// Doc is a Record type that is schemaless.
// This type is serialized entirely in msgpack format so MsgpackPathExtractor can be used as flat extractor
// in record store to speed up queries without the need of deserializing the whole document.
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

	err := doc.encode(enc)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// encode encodes the document to the encoder.
func (doc Doc[I]) encode(enc *msgpack.Encoder) error {
	if doc == nil {
		return enc.EncodeNil()
	}
	if err := enc.EncodeMapLen(len(doc)); err != nil {
		return err
	}
	for k, v := range doc {
		if k == "_id" {
			continue
		}

		if err := enc.EncodeString(k); err != nil {
			return err
		}
		if err := enc.Encode(v); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (doc *Doc[I]) UnmarshalBinary(data []byte) error {
	dec := msgpack.GetDecoder()
	dec.Reset(bytes.NewReader(data))
	defer msgpack.PutDecoder(dec)

	return dec.Decode(doc)
}
