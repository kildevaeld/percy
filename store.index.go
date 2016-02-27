package percy

import (
	"errors"

	"github.com/boltdb/bolt"

	"github.com/kildevaeld/dict"
	"github.com/kildevaeld/percy/index"
	"github.com/kildevaeld/percy/utils"
)

var StopIter = index.StopIter

type IndexEntry struct {
	Predicate []byte
	Targets   [][]byte
}

func (self *Store) EnsureIndexes(indexes map[string]interface{}) error {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.log("Ensure indexes %s", utils.DeferKeys(indexes))
	err := self.datadb.View(func(t *bolt.Tx) error {
		meta := t.Bucket(metaBucket)

		if meta == nil {
			return errors.New("database not initialized")
		}

		for k, item := range indexes {

			if err := self.ensureIndex(t, k, item, true); err != nil {
				return err
			}
		}

		for b, v := range self.indexes {
			m, ok := self.meta.Buckets[b]
			if !ok {
				m = &BucketMeta{
					Name:  b,
					Items: 0,
				}
			}
			if len(m.Indexes) < len(v.Indexes()) {
				m.Indexes = make([]index.Index, len(v.Indexes()))
			}
			x := 0
			for _, i := range v.Indexes() {
				m.Indexes[x] = i
				x++
			}
			m.Indexes = m.Indexes[:len(v.Indexes())]

			self.meta.Buckets[b] = m
		}

		return nil

	})

	if err != nil {
		return err
	}

	return self.datadb.Update(func(t *bolt.Tx) error {
		return self.update_meta(t)
	})

}

func (self *Store) ensureIndex(t *bolt.Tx, bucket string, v interface{}, update bool) error {
	var i *index.Indexer
	var err error
	var ok bool

	finder := func(bucket string) func([]byte) dict.Map {
		return func(key []byte) dict.Map {

			return nil
		}
	}

	if i, ok = self.indexes[bucket]; !ok {
		self.log("Creating new indexer for bucket: %s", bucket)
		i, err = index.NewIndexer(self.indexdb, []byte(bucket))
		if err != nil {
			return err
		}

		if self._log != nil {
			i.Debug(true)
		}

		self.log("Updating indexset for bucket: %s", bucket)
		err = i.UpdateIndexSet(v, finder(bucket))
		if err != nil {
			return err
		}
		self.indexes[bucket] = i

		go self.updateIndexes([]byte(bucket), i)

	} else {
		self.log("Updating indexset for bucket: %s", bucket)
		self.indexes[bucket].UpdateIndexSet(v, finder(bucket))
	}

	return nil
}

func (self *Store) updateIndexes(bucket []byte, index *index.Indexer) error {
	self.log("Updating indexes for bucket: %s", bucket)
	self.datadb.View(func(t *bolt.Tx) error {

		b, e := t.CreateBucketIfNotExists(bucket)
		if e != nil {
			return e
		}

		var m Map
		return b.ForEach(func(k, v []byte) error {

			if err := self.s.Decode(v, &m); err != nil {
				return err
			}

			return index.UpdateIndex(k, m)

			return nil
		})

	})

	return nil

}

func (self *Store) Indexes(bucket string, fn func(entry IndexEntry) error) error {
	self.lock.RLock()
	defer self.lock.RUnlock()
	indexer := self.indexes[bucket]

	if indexer == nil {
		return errors.New("bucket not defined: " + bucket)
	}

	err := indexer.Entries(func(entry *index.Entry) error {

		e := IndexEntry{
			Predicate: entry.Id,
			Targets:   entry.Entries,
		}

		if err := fn(e); err != nil {
			if err == StopIter {
				return index.StopIter
			}
			return err
		}

		return nil
	})

	return err
}
