package types

import (
	protojson "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MarshalBinary implements encoding.BinaryMarshaler
func (msg *Pet) MarshalBinary() ([]byte, error) {
	return proto.Marshal(msg)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (msg *Pet) UnmarshalBinary(b []byte) error {
	return proto.Unmarshal(b, msg)
}

// MarshalJSON implements json.Marshaler
func (m *Pet) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(m)
}

// UnmarshalJSON implements json.Unmarshaler
func (m *Pet) UnmarshalJSON(b []byte) error {
	return protojson.Unmarshal(b, m)
}
