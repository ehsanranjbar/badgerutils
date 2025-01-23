package testutil

import "encoding/json"

type TestStruct struct {
	A int            `json:"a,omitempty"`
	B string         `json:"b,omitempty"`
	C bool           `json:"c,omitempty"`
	D []int          `json:"d,omitempty"`
	E map[string]int `json:"e,omitempty"`
	F *TestStruct    `json:"f,omitempty"`
	G float32        `json:"g,omitempty"`
	H float64        `json:"h,omitempty"`
}

func (t TestStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *TestStruct) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}
