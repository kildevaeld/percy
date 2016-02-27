package percy

import (
	"encoding/json"
	"sync"

	"github.com/kildevaeld/dict"
)

type Value struct {
	key   []byte
	value []byte
	s     Serializer
	m     Map
}

func (self *Value) Key() []byte {
	return self.key
}

func (self *Value) Bytes() []byte {
	return self.value
}

func (self *Value) Decode(v interface{}) error {
	return self.s.Decode(self.value, v)
}

func (self *Value) Map() Map {
	if self.m == nil {
		err := self.Decode(&self.m)
		if err != nil {
			return nil
		}
	}
	return self.m
}

func (self *Value) Get(path string) interface{} {
	if self.Map() == nil {
		return nil
	}
	return dict.Map(self.Map()).Get(path)
}

func (self *Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(self.Map())
}

func (self *Value) Clear() {
	self.key = nil
	self.value = nil
	self.s = nil
	self.m = nil
}

func (self *Value) Dispose() {
	self.Clear()
	value_pool.Put(self)
}

var value_pool sync.Pool

func NewValue(key, value []byte, s Serializer) *Value {
	v := value_pool.Get().(*Value)
	v.key = key
	v.value = value
	v.s = s
	return v
}

func DisposeValue(v *Value) {
	v.Clear()
	value_pool.Put(v)
}

func init() {
	value_pool = sync.Pool{
		New: func() interface{} {
			return &Value{}
		},
	}
}
