package index

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/fatih/structs"
	"github.com/hashicorp/go-multierror"

	"github.com/kildevaeld/dict"
	"github.com/kildevaeld/percy/utils"
)

var keyType = []byte("$key:")
var keyLen = len(keyType)
var space = []byte(" ")

const MaxPredicateLength = 200

var StopIter = errors.New("IndexStopErr")

type IndexEntry struct {
	Predicate []byte
	Targets   [][]byte
}

func (i IndexEntry) String() string {
	return fmt.Sprintf("%s => %s", i.Predicate, i.Targets)
}

type QueryResult struct {
	Query Comparer
	Entry *Entry
}

type Indexer struct {
	lock      sync.RWMutex
	_log      *log.Logger
	bucket    []byte
	ibucket   []byte
	indexes   map[string]Index
	db        *bolt.DB
	pool      sync.Pool
	entryPool sync.Pool
	//stype   reflect.Type
}

func (self *Indexer) Meta() {

}

func (self *Indexer) Indexes() map[string]Index {
	return self.indexes
}

func (self *Indexer) Entries(fn func(entry *Entry) error) error {
	var err error
	err = self.db.View(func(t *bolt.Tx) error {

		bucket := t.Bucket(self.bucket)
		entry := self.entryPool.Get().(*Entry)

		err = bucket.ForEach(func(k, v []byte) error {

			entry.Id = nil
			entry.Entries = entry.Entries[:0]

			if err = entry.Unmarshal(v); err != nil {
				return err
			}

			if fn != nil {
				if err = fn(entry); err != nil {
					return err
				}
			}

			return nil
		})

		if err == StopIter {
			err = nil
		}

		entry.Id = nil
		entry.Entries = entry.Entries[:0]

		self.entryPool.Put(entry)

		return err

	})

	return err
}

func (self *Indexer) Find(query Comparer, fn func(q Comparer, k []byte, v []byte) error) error {

	var wg sync.WaitGroup

	worker := func(result *QueryResult) {

		for _, i := range result.Entry.Entries {
			fn(result.Query, result.Entry.Id, i)
		}

		DisposeEntry(result.Entry)
		//self.entryPool.Put(result.Entry)
		result.Entry = nil
		self.pool.Put(result)
		wg.Done()
	}

	if fn == nil {
		fn = func(q Comparer, k []byte, v []byte) error {
			return nil
		}
	}

	for _, path := range query.Paths() {

		self.db.View(func(t *bolt.Tx) error {

			bucket := t.Bucket(self.bucket)

			cursor := bucket.Cursor()

			var entry *Entry
			self.log("seek prefix: %s", path)
			for k, v := cursor.Seek([]byte(path)); bytes.HasPrefix(k, []byte(path)); k, v = cursor.Next() {
				self.log("found key for prefix: %s", path)

				if k == nil && len(k) == 0 {
					continue
				}

				if !query.Compare(path, v, k) {
					continue
				}

				entry = NewEntry() /*self.entryPool.Get().(*Entry)*/
				//entry = self.entryPool.Get().(*Entry)
				//entry.Id = nil
				//entry.Entries = nil

				if err := entry.Unmarshal(v); err != nil {

					return err
				}

				qr := self.pool.Get().(*QueryResult)
				qr.Entry = entry
				qr.Query = query

				wg.Add(1)
				go worker(qr)

			}

			return nil
		})

	}

	wg.Wait()

	return nil
}

func (self *Indexer) updateIndexForPredicate(key []byte, entry *Entry, predicate []byte, predicateLen int) error {
	var v []byte
	var err error

	entry.Reset()
	self.log("update key %x for predicate", key)
	err = self.db.View(func(t *bolt.Tx) error {

		bucket := t.Bucket(self.bucket)

		v = bucket.Get(predicate[:predicateLen])

		if v != nil {
			err = entry.Unmarshal(v)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	if utils.ByteSliceContains(entry.Entries, key) != -1 {
		return nil
	}

	err = self.db.Update(func(t *bolt.Tx) error {
		bucket := t.Bucket(self.bucket)
		if err = self.createPredicate(bucket, predicate[:predicateLen], key); err != nil {
			return err
		}

		v = bucket.Get(key)
		entry.Id = key
		if v == nil {
			entry.Entries = [][]byte{predicate[:predicateLen]}
		} else {
			err = entry.Unmarshal(v)

			if err != nil {
				return err
			}

			entry.Entries = append(entry.Entries, predicate[:predicateLen])
		}

		var b []byte
		if b, err = entry.Marshal(); err != nil {
			return err
		}
		self.log("Added predicate %s to %x\n", predicate[:predicateLen], key)
		return bucket.Put(key, b)

	})

	return err
}

func (self *Indexer) updateIndex(key []byte, index Index, m utils.Map, entry *Entry, predicate []byte) error {

	val := m.Get(index.Path)

	if val == nil {
		self.log("No value for path: %s", index.Path)
		return nil
	}
	var size int
	prefixLen := len(index.Name) + 1

	copy(predicate[:], []byte(index.Name+":"))
	tmp := make([]byte, MaxPredicateLength)
	copy(tmp, predicate[:prefixLen])

	return utils.ForEach(val, func(i int, v interface{}) error {

		size = utils.ByteSize(v)
		if size == -1 {
			return fmt.Errorf("cannot index %s : %v: %T", index.Path, v, v)
		}

		if size+prefixLen > MaxPredicateLength {
			return fmt.Errorf("predicate to long: %d", size+prefixLen)
		}

		if size == 0 {
			return nil
		}

		err := utils.PutByte(predicate[:], prefixLen, v)

		if err != nil {
			return err
		}

		//fmt.Printf("PRedicate %s - %d - %d - %d\n", predicate[:prefixLen+size], size, prefixLen)
		return self.updateIndexForPredicate(key, entry, predicate[:prefixLen+size], prefixLen+size)

	})

}

func (self *Indexer) UpdateIndex(key []byte, v interface{}) error {
	var result error

	var m utils.Map

	switch v2 := v.(type) {
	case utils.Map:
		m = v2
	case *utils.Map:
		m = *v2
	case map[string]interface{}:
		m = v2
	case *map[string]interface{}:
		m = *v2
	}

	if m == nil {
		t := reflect.TypeOf(v)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Kind() == reflect.Struct {
			m = utils.Map(structs.Map(v))
		}
	}

	if m == nil {
		return fmt.Errorf("could not reflect type: %T", v)
	}

	buffer := make([]byte, MaxPredicateLength)
	entry := &Entry{}
	for _, index := range self.indexes {
		self.log("Adding index %s for key %s", index.Name, key)
		if err := self.updateIndex(key, index, m, entry, buffer); err != nil {
			result = multierror.Append(result, err)

		}

	}

	return result
}

func (self *Indexer) removeIndexSets(sets []Index) error {

	return nil
}

func (self *Indexer) createPredicate(b *bolt.Bucket, predicate []byte, key []byte) error {

	entry := &Entry{
		Id: predicate,
	}

	val := b.Get(predicate)

	if val == nil {
		entry.Entries = [][]byte{key}
	} else {
		entry.Unmarshal(val)
		if utils.ByteSliceContains(entry.Entries, key) == -1 {
			entry.Entries = append(entry.Entries, key)
		}

	}

	by, err := entry.Marshal()

	if err != nil {
		return err
	}

	return b.Put(predicate, by)
}

func (self *Indexer) getEntriesForPrefix(prefix []byte, fn func(entry *Entry) error) error {
	var err error

	entry := self.entryPool.Get().(*Entry)

	err = self.db.View(func(t *bolt.Tx) error {

		bucket := t.Bucket(self.bucket)
		cursor := bucket.Cursor()

		for k, v := cursor.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			entry.Id = nil
			entry.Entries = entry.Entries[:0]

			if k == nil && len(k) == 0 {
				return nil
			}

			if err = entry.Unmarshal(v); err != nil {
				return err
			}

			if err = fn(entry); err != nil {
				return err
			}

		}

		return nil
	})

	entry.Id = nil
	entry.Entries = entry.Entries[:0]
	self.entryPool.Put(entry)

	return err
}

func (self *Indexer) updateIndexSets(sets []Index, fn func(key []byte) dict.Map) error {

	//var c *bolt.Cursor
	var prefix []byte
	var err error
	key := make([]byte, MaxPredicateLength)
	prefix = make([]byte, MaxPredicateLength)
	copy(key, keyType)

	var updateList []Entry
	var removeList []Entry
	for _, index := range sets {
		indexFound := false
		prefixLen := len(index.Name)
		copy(prefix, []byte(index.Name))
		self.log("seek index name %s", index.Name)
		err = self.getEntriesForPrefix([]byte(index.Name), func(entry *Entry) error {
			indexFound = true

			for _, e := range entry.Entries {
				if m := fn(e); m != nil {

					val := m.Get(index.Name)

					if val == nil {
						removeList = append(removeList, Entry{
							Id:      entry.Id,
							Entries: [][]byte{e},
						})

						continue

					}

					size := utils.ByteSize(val)

					if size == -1 {
						return fmt.Errorf("cannot index value %T", val)
					}

					if prefix, err = utils.ToByte(prefix[prefixLen:], val); err != nil {
						return err
					}

					// Equal. Stay in entry
					if bytes.Equal(prefix[:size+prefixLen], entry.Id) {
						continue
					}
					// Delete entry
					removeList = append(removeList, Entry{
						Id:      entry.Id,
						Entries: [][]byte{e},
					})
					//entry.Entries = append(entry.Entries[:i], entry.Entries[i+1:]...)

				} else {
					removeList = append(removeList, Entry{
						Id:      entry.Id,
						Entries: [][]byte{e},
					})
				}
			}

			return nil
		})

		if err != nil {
			return err
		}

		err = self.db.Update(func(t *bolt.Tx) error {

			bucket := t.Bucket(self.bucket)

			/*for _, b := range removeList {

			}*/

			for _, b := range updateList {
				self.log("Adding key %s to predicate", b.Entries[0])
				err = self.createPredicate(bucket, b.Id, b.Entries[0])
				if err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			return err
		}

	}

	return err

	/*for _, index := range sets {

		err := self.db.Update(func(t *bolt.Tx) error {

			entry := &Entry{
				Id: []byte(""),
			}

			bucket := t.Bucket(self.bucket)

			c = bucket.Cursor()

			prefixLen := len(index.Name)
			copy(prefix, []byte(index.Name))
			self.log("seek index name %s", prefix[:prefixLen])
			for k, v = c.Seek(prefix[:prefixLen]); bytes.HasPrefix(k, prefix[:prefixLen]); k, v = c.Next() {
				if err = entry.Unmarshal(v); err != nil {
					return err
				}

				indexFound = true

				var remove [][]byte
				self.log("found index entry for %s", k)
				for i, e := range entry.Entries {

					copy(key[:keyLen], e)

					if m := fn(e); m != nil {

						val := m.Get(index.Path)

						if val == nil {
							remove = append(remove, e)

						} else {
							size := utils.ByteSize(val)

							if size == -1 {
								return fmt.Errorf("cannot index value %T", val)
							}

							if prefix, err = utils.ToByte(prefix[prefixLen:], val); err != nil {
								return err
							}

							// Equal. Stay in entry
							if bytes.Equal(prefix[:size+prefixLen], k) {
								continue
							}
							// Delete entry
							entry.Entries = append(entry.Entries[:i], entry.Entries[i+1:]...)

							if err = self.createPredicate(bucket, prefix[:size+prefixLen], key[:keyLen+len(e)]); err != nil {
								return err
							}

						}

					} else {
						self.log("Creating new index for %s", index.Name)

					}
				}

				if len(entry.Entries) == 0 {
					if err = bucket.Delete(k); err != nil {
						return err
					}
				} else {
					v, _ = utils.Ensure(v, entry.Size())
					i, e := entry.MarshalTo(v)

					if e != nil {
						return e
					}

					if err = bucket.Put(k, v[:i]); err != nil {
						return err
					}
				}

			}

			return nil

		})

		if err != nil {
			return err
		}

	}

	return nil*/
}

func (self *Indexer) RemoveIndex(key []byte) error {

	return nil
}

func indexSetFromStruct(v interface{}) ([]Index, error) {
	t := reflect.TypeOf(v)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var indexes []Index
	fields := structs.Fields(v)

	for _, field := range fields {
		i := field.Tag("index")
		tag := field.Name()
		unique := false
		if i != "" {
			split := strings.Split(i, ",")

			if split[0] != "" {
				tag = split[0]
			}

			if len(split) == 2 && split[1] == "unique" {
				unique = true
			}

			indexes = append(indexes, Index{
				Name:   tag,
				Unique: unique,
				Path:   field.Name(),
			})

		}
	}

	return indexes, nil
}

func (self *Indexer) UpdateIndexSet(v interface{}, fn func(key []byte) dict.Map) error {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.log("Updating index set: %s", self.bucket)
	t := reflect.TypeOf(v)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	var err error
	var indexSet []Index
	if v2, ok := v.(Index); ok {
		indexSet = []Index{v2}
	} else if t.Kind() == reflect.Struct {
		indexSet, err = indexSetFromStruct(v)

		if err != nil {
			return err
		}

	} else {
		switch v2 := v.(type) {
		case Index:
			indexSet = []Index{v2}
		case *Index:
			indexSet = []Index{*v2}
		case []Index:
			indexSet = v2
		case map[string]Index:
			for k, v3 := range v2 {
				v3.Name = k
				indexSet = append(indexSet, v3)
			}
		default:
			return fmt.Errorf("expected struct, Index, *Index, []Index or map[string]Index. Got: %T", v)
		}
	}

	if self.indexes == nil {
		self.indexes = make(map[string]Index)
	}

	var remove []Index
	var update = make(map[string]Index)

	for _, index := range indexSet {
		if index.Name == "$key" {
			return errors.New("index name cannot be $id")
		}
		update[index.Name] = index
	}

	for name, index := range self.indexes {
		if new, ok := update[name]; ok {
			if !new.Equal(index) {
				remove = append(remove, index)
			} else {
				delete(update, name)
			}
		} else {
			remove = append(remove, index)
			delete(self.indexes, name)
		}

	}

	for k, i := range update {
		self.indexes[k] = i
	}
	self.log("  Removing %d indexsets", len(remove))
	if err = self.removeIndexSets(remove); err != nil {
		return err
	}

	i := 0
	for _, v := range update {
		indexSet[i] = v
		i++
	}
	self.log("  Updating %d indexsets", len(update))
	if err = self.updateIndexSets(indexSet[:i], fn); err != nil {
		return err
	}

	return nil
}

func (self *Indexer) log(str string, args ...interface{}) {
	if self._log != nil {
		self._log.Printf(str, args...)
	}
}

func (self *Indexer) Debug(debug bool) {
	if debug && self._log == nil {
		self._log = log.New(os.Stderr, "[INDEX] ", log.LstdFlags)
	} else if !debug && self._log != nil {
		self._log = nil
	}
}

func NewIndexer(db *bolt.DB, bucket []byte) (*Indexer, error) {
	//log.SetOutput(ioutil.Discard)
	err := db.Update(func(t *bolt.Tx) error {
		_, e := t.CreateBucketIfNotExists(bucket)
		return e
	})

	return &Indexer{
		db:     db,
		bucket: bucket,
		pool: sync.Pool{
			New: func() interface{} {
				return &QueryResult{}
			},
		},
		entryPool: sync.Pool{
			New: func() interface{} {
				return &Entry{}
			},
		},
	}, err
}
