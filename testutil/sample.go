package testutil

import (
	"encoding/json"

	estore "github.com/ehsanranjbar/badgerutils/store/entity"
	pstore "github.com/ehsanranjbar/badgerutils/store/prefix"
)

type SampleStruct struct {
	A int            `json:"a,omitempty"`
	B string         `json:"b,omitempty"`
	C bool           `json:"c,omitempty"`
	D []int          `json:"d,omitempty"`
	E map[string]int `json:"e,omitempty"`
	F *SampleStruct  `json:"f,omitempty"`
	G float32        `json:"g,omitempty"`
	H float64        `json:"h,omitempty"`
}

func (t SampleStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *SampleStruct) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

type SampleEntity struct {
	Id   int64  `json:"-"`
	Name string `json:"name,omitempty"`
}

func NewSampleEntity(name string) *SampleEntity {
	return &SampleEntity{
		Name: name,
	}
}

func (t SampleEntity) GetId() int64 {
	return t.Id
}

func (t *SampleEntity) SetId(id int64) {
	t.Id = id
}

func (t SampleEntity) MarshalBinary() ([]byte, error) {
	return json.Marshal(t)
}

func (t *SampleEntity) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, t)
}

func NewEntityStore(prefix []byte) *estore.Store[int64, SampleEntity, *SampleEntity] {
	var i int64
	return estore.New[int64, SampleEntity](pstore.New(nil, prefix)).
		WithIdFunc(func(_ *SampleEntity) (int64, error) {
			i++
			return i, nil
		})
}
