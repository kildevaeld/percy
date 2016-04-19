package percy

import (
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/google/cayley"
	"github.com/google/cayley/graph/path"
	"github.com/kildevaeld/percy/index"
	"github.com/kildevaeld/percy/utils"
)

func (self *Store) getFirst(p *path.Path) []byte {
	it := p.BuildIterator()
	for cayley.RawNext(it) {
		val := self.graphdb.NameOf(it.Result())
		return []byte(val)
	}
	return nil
}

func (self *Store) Count(bucket []byte) (int, error) {
	if _, ok := self.meta.Buckets[string(bucket)]; !ok {
		return -1, fmt.Errorf("No bucket named: %s", bucket)
	}
	return self.meta.Buckets[string(bucket)].Items, nil
}

func (self *Store) FromPath(path *path.Path, bucket []byte, fn func(v *Value) error) error {

	it := path.BuildIterator()
	//var m iutils.Map
	for cayley.RawNext(it) {
		key := self.graphdb.NameOf(it.Result())

		p := bucket
		if bucket == nil {
			p = self.getFirst(self.Graph(key).Out("/store/model"))
		}

		if p == nil {
			return errors.New("no bucket")
		}

		var val []byte
		self.datadb.View(func(t *bolt.Tx) error {
			val = t.Bucket(p).Get([]byte(key))
			return nil
		})

		if val == nil {
			continue
		}

		v := NewValue([]byte(key), val, self.s)
		err := fn(v)
		v.Dispose()

		if err != nil {
			return err
		}

	}

	return nil

}

func (self *Store) Query(bucketName []byte, query index.Comparer) *Query {
	return &Query{
		query:  query,
		store:  self,
		bucket: bucketName,
	}
}

func (self *Store) Get(bucket []byte, id interface{}, item interface{}) error {
	var idBytes []byte

	if str, ok := id.(string); ok {
		if utils.IsSidHex(str) {
			idBytes = []byte(utils.SidHex(str))
		} else {
			idBytes = []byte(str)
		}
	} else if str, ok := id.(utils.Sid); ok {
		idBytes = []byte(str)
	} else if b, ok := id.([]byte); ok {
		idBytes = b
	}

	if idBytes == nil {
		return errors.New("invalid id")
	}

	return self.datadb.View(func(t *bolt.Tx) error {

		b := t.Bucket(bucket)

		if b == nil {
			return nil
		}

		bb := b.Get(idBytes)

		if bb == nil {
			return nil
		}

		return self.s.Decode(bb, item)

	})

}
