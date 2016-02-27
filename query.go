package percy

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/boltdb/bolt"
	"github.com/kildevaeld/percy/index"
)

type Query struct {
	query  index.Comparer
	limit  int
	offset int
	store  *Store
	bucket []byte
}

func (self *Query) Limit(n int) *Query {
	self.limit = n
	return self
}

func (self *Query) Offset(n int) *Query {
	self.offset = n
	return self
}

// ENDERS
func (self *Query) All(result interface{}) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	slicev := resultv.Elem()

	elemt := slicev.Type().Elem()

	i := 0
	var err error
	err = self.ForEach(func(v *Value) error {

		if slicev.Len() == i {
			elemp := reflect.New(elemt)

			err = v.Decode(elemp.Interface())

			if err == nil {
				slicev = reflect.Append(slicev, elemp.Elem())
				i++
			} else {
				return err
			}
		}

		return nil
	})

	resultv.Elem().Set(slicev.Slice(0, i))

	return err
}

func (self *Query) One(v interface{}) error {
	if reflect.TypeOf(v).Kind() == reflect.Slice {
		return errors.New("dest cannot be a slice")
	}

	var err error
	err = self.ForEach(func(val *Value) error {

		err = val.Decode(v)

		if err != nil {
			return err
		}

		return StopIter
	})

	if err == StopIter {
		return nil
	}

	return err
}

func (self *Query) ForEach(fn func(*Value) error) error {
	in := self.store.indexes[string(self.bucket)]

	if in == nil {
		return fmt.Errorf("no index for type %s", self.bucket)
	}
	//count := 0
	var err error
	err = self.store.datadb.View(func(t *bolt.Tx) error {

		bucket := t.Bucket(self.bucket)

		v := NewValue(nil, nil, self.store.s)

		if self.query == nil {
			err = bucket.ForEach(func(k, val []byte) error {
				v.key = k
				v.value = val
				v.m = nil
				return fn(v)
			})
		} else {
			err = in.Find(self.query, func(q index.Comparer, key []byte, id []byte) error {
				item := bucket.Get(id)

				v.key = key
				v.value = item
				v.m = nil

				return fn(v)
			})
		}

		v.Dispose()

		return err

	})

	return err
}

func (self *Query) Count() (int, error) {
	var i int
	err := self.ForEach(func(*Value) error {
		i++
		return nil
	})

	return i, err
}
