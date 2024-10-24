package testutil

import "encoding/json"

type TestStruct struct {
	A int    `json:"a,omitempty"`
	B string `json:"b,omitempty"`
}

func (t TestStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *TestStruct) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}
