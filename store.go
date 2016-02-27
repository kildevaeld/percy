package percy

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/go-multierror"

	"github.com/kildevaeld/dict"
	"github.com/kildevaeld/percy/index"
	"github.com/kildevaeld/percy/serializer"
	"github.com/kildevaeld/percy/utils"
)
import (
	"github.com/google/cayley"
	"github.com/google/cayley/graph"
	_ "github.com/google/cayley/graph/bolt"
	"github.com/google/cayley/quad"
)

type Map dict.Map

type Q []index.Comparer

var metaBucket = []byte("$store_meta")

var storeVersion = 1

const (
	dataDB  = "objects"
	indexDB = "indexes"
	graphDB = "graphs"
)

type Item interface {
	ACLIdentifier() string
	ACLType() string
}

type StoreConfig struct {
	Path       string
	ReadOnly   bool
	Debug      bool
	Timeout    time.Duration
	NoGrowSync bool
	MmapFlags  int
	NoSync     bool
	Encoding   serializer.EncodingType
}

type BucketMeta struct {
	Name    string
	Items   int
	Indexes []index.Index
}

type StoreMeta struct {
	Version  uint32
	Encoding serializer.EncodingType
	Buckets  map[string]*BucketMeta
}

type Store struct {
	_log    *log.Logger
	datadb  *bolt.DB
	graphdb *cayley.Handle
	indexdb *bolt.DB
	lock    sync.RWMutex
	indexes map[string]*index.Indexer
	config  StoreConfig
	meta    StoreMeta
	s       Serializer
}

// Remove removes the database from the filesystem.
// This is VERY destructive and unreversible
//
// TODO: Rename or remove this method
func (self *Store) Remove() {
	os.Remove(filepath.Join(self.config.Path, dataDB))
	os.Remove(filepath.Join(self.config.Path, indexDB))
	os.Remove(filepath.Join(self.config.Path, graphDB))
}

// Create creates a new item in the spcified bucket.
//
// The key must be a byte array, utils.Sid or a string (which will be converted to a byte array)
func (self *Store) Create(bucket []byte, key interface{}, v interface{}) error {
	self.log("Creating new object for key %s", key)
	return self.datadb.Update(func(t *bolt.Tx) error {
		b := t.Bucket(bucket)

		if b == nil {
			return errors.New("bucket does not exists")
		}

		var by []byte
		self.log("Serializing object %s", v)
		err := self.s.Encode(&by, v)

		if err != nil {
			return err
		}

		var id []byte

		switch key.(type) {
		case string:
			if utils.IsSidHex(key.(string)) {
				key = utils.SidHex(key.(string))
			}
			id = []byte(key.(string))
		case []byte:
			id = key.([]byte)
		case utils.Sid:
			id = []byte(key.(utils.Sid))
		default:
			id = nil
		}

		if id == nil {
			return fmt.Errorf("invalid key %v", key)
		}

		err = b.Put(id, by)

		if err != nil {
			return err
		}

		if self.indexes[string(bucket)] != nil {
			self.log("Updating index for key %s", key)
			if err = self.indexes[string(bucket)].UpdateIndex(id, v); err != nil {
				return err
			}
		}

		q := cayley.Quad(string(id), "/store/model", string(bucket), "")
		self.log("Updating entry in quadstore %#v", q)
		if err = self.graphdb.AddQuad(q); err != nil {
			return err
		}

		self.lock.Lock()
		self.meta.Buckets[string(bucket)].Items++
		self.update_meta(t)
		self.lock.Unlock()

		return nil

	})

}

// List returns all items associated which spcified bucket
//
// It is a convience function for Query(nil).All(&result).
func (self *Store) List(bucket []byte, result interface{}) error {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	i := 0

	slicev := resultv.Elem()
	elemt := slicev.Type().Elem()

	err := self.datadb.View(func(t *bolt.Tx) error {

		b := t.Bucket(bucket)

		if b == nil {
			return nil
		}

		return b.ForEach(func(k, v []byte) error {

			if slicev.Len() == i {
				elemp := reflect.New(elemt)

				if err := self.s.Decode(v, elemp.Interface()); err != nil {
					return err
				}

				slicev = reflect.Append(slicev, elemp.Elem())

			} else {
				return nil
			}
			i++
			return nil
		})
	})

	resultv.Elem().Set(slicev.Slice(0, i))

	return err
}

// Delete deletes a key and value in specified bucket
func (self *Store) Delete(bucket, key []byte) error {
	return self.datadb.Update(func(t *bolt.Tx) error {

		b := t.Bucket(bucket)

		if b == nil {
			return errors.New("item does not exists")
		}

		p := self.Graph(string(key))

		it := p.BuildIterator()
		var w sync.WaitGroup

		// Remove relations
		for cayley.RawNext(it) {
			w.Add(1)

			go func(r graph.Value) {
				defer w.Done()
				var wg sync.WaitGroup
				wg.Add(2)

				go func() {
					defer wg.Done()
					ii := self.graphdb.QuadIterator(quad.Object, r)
					for cayley.RawNext(ii) {
						q := self.graphdb.Quad(ii.Result())

						self.graphdb.RemoveQuad(q)
					}
				}()

				go func() {
					defer wg.Done()
					ii := self.graphdb.QuadIterator(quad.Subject, r)
					for cayley.RawNext(ii) {
						q := self.graphdb.Quad(ii.Result())
						self.graphdb.RemoveQuad(q)
					}
				}()

				wg.Wait()
			}(it.Result())
			//r := it.Result()

		}
		var err error
		w.Wait()
		if _, ok := self.indexes[string(bucket)]; ok {
			//err = bi.RemoveIndex(t, key)
			if err != nil {
				return err
			}
		}

		err = b.Delete(key)

		if err != nil {
			return err
		}

		self.lock.Lock()
		self.meta.Buckets[string(bucket)].Items--
		self.lock.Unlock()
		self.update_meta(t)
		return nil

	})
}

// Update updates a keys value.
// An error will be returned, if the key does'nt exists
func (self *Store) Update(bucket []byte, key []byte, v interface{}) (err error) {
	return self.datadb.Update(func(t *bolt.Tx) error {

		b := t.Bucket(bucket)

		if b == nil {
			return errors.New("Bucket does not exists")
		}

		if b.Get(key) == nil {
			return fmt.Errorf("could not update item with key: %s: key not found", key)
		}

		var by []byte

		err := self.s.Encode(&by, v)

		if err != nil {
			return err
		}

		return b.Put(key, by)

	})
}

// Close closes the connection to the database
func (self *Store) Close() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	var result error
	err := self.datadb.Close()
	if err != nil {
		result = multierror.Append(result, err)
	}
	self.graphdb.Close()

	err = self.indexdb.Close()

	if err != nil {
		result = multierror.Append(result, err)
	}

	return result
}

func (self *Store) log(str string, args ...interface{}) {
	if self._log != nil {
		for i, v := range args {
			if f, ok := v.(func() []string); ok {
				args[i] = f()
			}
		}
		self._log.Printf(str, args...)
	}
}

// Debug turns debugging on or off.
func (self *Store) Debug(debug bool) {
	for _, i := range self.indexes {
		i.Debug(debug)
	}

	if debug && self._log == nil {
		self._log = log.New(os.Stderr, "[STORE] ", log.LstdFlags)
	} else if !debug && self._log != nil {
		self._log = nil
	}

}

// Open opens a new instanceof of a database from given path.
func Open(config StoreConfig) (*Store, error) {

	store, err := NewStore(config)

	if err != nil {
		return nil, err
	}

	store.Debug(config.Debug)

	store.log("using encoding %s", config.Encoding)

	err = store.Init()

	if err != nil {
		return nil, err
	}

	return store, nil
}

func initBoltDB(path string, config StoreConfig) (*bolt.DB, error) {
	idb, err := bolt.Open(path, 0600, &bolt.Options{
		ReadOnly:   config.ReadOnly,
		Timeout:    config.Timeout,
		NoGrowSync: config.NoGrowSync,
		MmapFlags:  config.MmapFlags,
	})

	idb.NoSync = config.NoSync

	if err != nil {
		return nil, err
	}

	return idb, err
}

// TODO: Turn this into a private method
func NewStore(config StoreConfig) (*Store, error) {

	dbPath := filepath.Join(config.Path, dataDB)
	qPath := filepath.Join(config.Path, graphDB)
	iPath := filepath.Join(config.Path, indexDB)

	var ddb, idb *bolt.DB
	var qdb *cayley.Handle
	var err error

	var stats os.FileInfo
	if stats, err = os.Stat(config.Path); err != nil {
		if err = os.MkdirAll(config.Path, 0600); err != nil {
			return nil, err
		}
	}

	if !stats.IsDir() {
		return nil, fmt.Errorf("path %s exists, but is not a directory", config.Path)
	}

	if ddb, err = initBoltDB(dbPath, config); err != nil {
		return nil, err
	}

	if idb, err = initBoltDB(iPath, config); err != nil {
		return nil, err
	}

	if qdb, err = initGraphDB(qPath, config); err != nil {
		return nil, err
	}

	return &Store{
		meta: StoreMeta{
			Version: 1,
			Buckets: make(map[string]*BucketMeta),
		},
		datadb:  ddb,
		graphdb: qdb,
		indexdb: idb,
		indexes: make(map[string]*index.Indexer),
		s:       serializer.NewSerializer(config.Encoding),
	}, nil

}
