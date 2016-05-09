package percy

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/google/cayley"
	"github.com/google/cayley/quad"
	"github.com/hashicorp/go-multierror"
	"github.com/kildevaeld/percy/index"
	"github.com/kildevaeld/percy/serializer"
	"github.com/kildevaeld/percy/utils"
)

// InitBucket initializes a new or existing bucket in the store.
// This should be called before using a bucket.
func (self *Store) InitBucket(buckets ...[]byte) error {

	self.log("Init buckets %s", buckets)

	return self.datadb.Update(func(t *bolt.Tx) error {
		self.lock.Lock()
		defer self.lock.Unlock()
		for _, bucket := range buckets {
			self.log("initializing bucket %s", bucket)
			_, err := t.CreateBucketIfNotExists(bucket)

			if _, ok := self.meta.Buckets[string(bucket)]; ok {
				continue
			}

			if err == nil {
				self.log("Adding new bucket to meta: %s", bucket)
				self.meta.Buckets[string(bucket)] = &BucketMeta{
					Name:  string(bucket),
					Items: 0,
				}

			} else {
				return err
			}
		}

		self.update_meta(t)

		return nil
	})
}

// Meta returns metadata associated with the datastore.
func (self *Store) Meta() StoreMeta {
	self.lock.RLock()
	defer self.lock.RUnlock()
	meta := StoreMeta{
		Version: self.meta.Version,
		Buckets: make(map[string]*BucketMeta),
	}

	for k, b := range self.meta.Buckets {

		bb := *b
		meta.Buckets[k] = &bb
	}

	return self.meta
}

func (self *Store) update_meta(t *bolt.Tx) error {
	bucket := t.Bucket(metaBucket)
	var b []byte
	e := self.s.Encode(&b, self.meta.Buckets)

	if e != nil {
		return e
	}
	return bucket.Put([]byte("buckets"), b)
}

// Init initializes the store.
// FIXME: This should be an unexported method, called only by Open
func (self *Store) init() error {

	return self.datadb.Update(func(t *bolt.Tx) error {
		self.lock.Lock()
		defer self.lock.Unlock()

		self.log("Initializing meta: %s", metaBucket)
		bucket, err := t.CreateBucketIfNotExists(metaBucket)

		if err != nil {
			return err
		}

		// VERSION
		vb := bucket.Get([]byte("version"))
		if vb == nil {
			var b [4]byte
			utils.PutUInt32(b[:], self.meta.Version)
			err := bucket.Put([]byte("version"), b[:])

			if err != nil {
				return err
			}
			self.log("  Setting store version to meta: %d", self.meta.Version)
			vb = b[:]
		} else {
			self.log("  Got version from store: %d", utils.GetUInt32(vb))
		}

		version := utils.GetUInt32(vb)
		if version != self.meta.Version {
			return errors.New("version mismatch")
		}

		// ENCODING
		// TODO: USE int8
		encKey := []byte("encoding")
		enc := bucket.Get(encKey)
		if enc == nil {
			var b [8]byte
			utils.PutInt64(b[:], int64(self.config.Encoding))

			if err := bucket.Put(encKey, b[:]); err != nil {
				return err
			}
			self.log("  Setting store encoding: %s", self.config.Encoding)
			enc = b[:]
		} else {
			self.log("  Store encoding is: %s", serializer.EncodingType(utils.GetInt64(enc)))
		}

		encoding := utils.GetInt64(enc)
		if encoding != int64(self.config.Encoding) {
			return fmt.Errorf("  Store encoded using %s, but config provided: %s", encoding, self.config.Encoding)
		}

		self.meta.Encoding = serializer.EncodingType(encoding)

		rb := bucket.Get([]byte("buckets"))

		if rb == nil {
			return nil
		}
		var buckets map[string]*BucketMeta
		err = self.s.Decode(rb, &buckets)

		if err != nil {
			return err
		}

		self.meta.Buckets = buckets

		for _, b := range buckets {
			self.log("  Got bucket %s from meta", b.Name)
			if len(b.Indexes) == 0 {
				continue
			}
			self.log("Updating index for %s", b.Name)
			err := self.ensureIndex(t, b.Name, b.Indexes, false)
			if err != nil {
				return err
			}
		}

		return nil
	})

}

// RemoveBucket remove a bucket entirely from the datastore.
// Warning: This cannot be undone.
func (self *Store) RemoveBucket(bucket []byte) error {
	var result error
	var wg sync.WaitGroup
	var lock sync.Mutex
	if i, ok := self.indexes[string(bucket)]; ok {
		wg.Add(1)
		go func(i *index.Indexer) {
			defer wg.Done()
			err := self.datadb.Update(func(t *bolt.Tx) error {
				//return i.Remove(t)
				return nil
			})
			if err != nil {
				lock.Lock()
				result = multierror.Append(result, err)
				lock.Unlock()
			}
		}(i)
		delete(self.indexes, string(bucket))
	}

	remove_q := func(key []byte, dir quad.Direction) {
		defer wg.Done()
		self.Quads(key, dir, func(q quad.Quad) error {
			self.graphdb.RemoveQuad(q)
			return nil
		})
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		p := self.Graph().Has("/store/model", string(bucket))
		it := p.BuildIterator()

		for cayley.RawNext(it) {
			id := self.graphdb.NameOf(it.Result())
			wg.Add(2)

			go remove_q([]byte(id), quad.Object)
			go remove_q([]byte(id), quad.Subject)

		}

	}()

	wg.Wait()

	err := self.datadb.Update(func(t *bolt.Tx) error {
		err := t.DeleteBucket(bucket)

		if err != nil {
			return err
		}

		self.lock.Lock()

		delete(self.meta.Buckets, string(bucket))
		self.update_meta(t)
		self.lock.Unlock()

		return nil
	})

	if err != nil {
		result = multierror.Append(err)
	}

	return result
}

func export(db *bolt.DB, z *zip.Writer, path string) (int64, error) {
	file, err := z.Create(path)

	if err != nil {
		return 0, err
	}

	var n int64
	err = db.View(func(t *bolt.Tx) error {
		n, err = t.WriteTo(file)

		return err
	})

	return n, err

}

func (self *Store) Export(path string) (int64, error) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	var written int64 = 0

	file, err := os.Create(path)
	defer file.Close()
	if err != nil {
		return written, err
	}

	zipfile := zip.NewWriter(file)
	var n int64

	// OBJECTS
	if n, err = export(self.datadb, zipfile, "objects"); err != nil {
		return 0, err
	}
	written += n

	// INDEXES
	if n, err = export(self.indexdb, zipfile, "indexes"); err != nil {
		return 0, err
	}
	written += n

	/*cf, ce := zipfile.Create("graphs")

	if ce != nil {
		return 0, ce
	}*/

	//ex := exporter.NewExporter(cf, self.graphdb.QuadStore)

	//ex.ExportQuad()

	// META
	meta := self.Meta()

	b, je := json.Marshal(meta)

	if je != nil {
		return 0, je
	}

	f, fe := zipfile.Create("meta")

	if fe != nil {
		return 0, fe
	}

	f.Write(b)

	return written, zipfile.Close()

}
