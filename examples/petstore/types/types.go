package types

import (
	protojson "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// SetId implements Record
func (msg *Category) SetId(id int64) {
	msg.Id = id
}

// MarshalBinary implements encoding.BinaryMarshaler
func (msg *Category) MarshalBinary() ([]byte, error) {
	return proto.Marshal(msg)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (msg *Category) UnmarshalBinary(b []byte) error {
	return proto.Unmarshal(b, msg)
}

// SetId implements Record
func (msg *PetRecord) SetId(id int64) {
	msg.Id = id
}

// MarshalBinary implements encoding.BinaryMarshaler
func (msg *PetRecord) MarshalBinary() ([]byte, error) {
	return proto.Marshal(msg)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (msg *PetRecord) UnmarshalBinary(b []byte) error {
	return proto.Unmarshal(b, msg)
}

// MarshalJSON implements json.Marshaler
func (msg *Pet) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(msg)
}

// UnmarshalJSON implements json.Unmarshaler
func (msg *Pet) UnmarshalJSON(b []byte) error {
	return protojson.Unmarshal(b, msg)
}
