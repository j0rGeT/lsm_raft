package kv

import "encoding/json"

type SearchResult int

const (
	None SearchResult = iota
	Deleted
	Success
)


type Value struct {
	Key string
	Value []byte
	Deleted bool
}

func (v *Value) Copy() *Value {
	return &Value{
		Key: v.Key,
		Value: v.Value,
		Deleted: v.Deleted,
	}
}

func Get(v *Value) (interface{}, error) {
	var value interface{}
	err := json.Unmarshal(v.Value, &value)
	return value, err
}

func Convert(value interface{}) ([]byte, error){
	return json.Marshal(value)
}

func Decode(data []byte) (Value, error) {
	var value Value
	err := json.Unmarshal(data, &value)
	return value, err
}

func Encode(value Value) ([]byte, error) {
	return json.Marshal(value)
}