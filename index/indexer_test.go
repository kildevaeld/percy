package index

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/Pallinder/go-randomdata"
	"github.com/boltdb/bolt"
	"github.com/kildevaeld/dict"
	"github.com/kildevaeld/percy/utils"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func TestIndexer(t *testing.T) { TestingT(t) }

type IndexerTestSuite struct {
	store *bolt.DB
}

var _ = Suite(&IndexerTestSuite{})

var values = utils.Map{
	"id1": utils.Map{
		"name": "rasmus kildevaeld",
		"age":  31,
	},
	"id2": utils.Map{
		"name": "lone hansen rasmusen conrad",
		"age":  48,
	},
	"id3": utils.Map{
		"name": "alvilda kildevaeld",
		"age":  5,
	},
}

func (self *IndexerTestSuite) TestUpdateIndexSet(c *C) {

	db, err := bolt.Open("./index.boltdb", 0766, nil)

	if err != nil {
		c.Fatal(err)
	}

	defer db.Close()
	defer os.Remove("./index.boltdb")

	i, _ := NewIndexer(db, []byte("TestBucket"))

	indexes := []Index{
		Index{
			Name: "name",
			Path: "name",
		},
		Index{
			Name: "age",
			Path: "age",
		},
	}

	err = i.UpdateIndexSet(indexes, func(key []byte) dict.Map {
		if v, ok := values[string(key)].(dict.Map); ok {
			return v
		}
		return nil
	})

	if err != nil {
		c.Fatal(err)
	}

	for ii := 0; ii < 1000; ii++ {
		i.UpdateIndex([]byte(fmt.Sprintf("id%d", ii)), utils.Map{
			"name": randomdata.FirstName(randomdata.Male),
			"age":  rand.Intn(80),
		})
	}

	err = i.UpdateIndex([]byte("id1"), values["id1"].(utils.Map))
	i.UpdateIndex([]byte("id2"), values["id2"].(utils.Map))
	i.UpdateIndex([]byte("id3"), values["id3"].(utils.Map))
	if err != nil {
		c.Error(err)
	}

	/*db.View(func(t *bolt.Tx) error {
		buck := t.Bucket([]byte("TestBucket"))

		entry := &Entry{}
		buck.ForEach(func(k, v []byte) error {
			entry.Unmarshal(v)
			fmt.Printf("ID %s : %s - %s \n", k, entry.Id, entry.Entries)
			entry.Reset()
			return nil
		})

		return nil
	})*/

}

func (self *IndexerTestSuite) TestFind(c *C) {
	db, err := bolt.Open("./index.boltdb", 0766, nil)

	if err != nil {
		c.Fatal(err)
	}

	defer db.Close()
	defer os.Remove("./index.boltdb")

	i, _ := NewIndexer(db, []byte("TestBucket"))

	indexes := []Index{
		Index{
			Name: "name",
			Path: "name",
		},
		Index{
			Name: "age",
			Path: "age",
		},
	}

	err = i.UpdateIndexSet(indexes, func(key []byte) dict.Map {
		if v, ok := values[string(key)].(dict.Map); ok {
			return v
		}
		return nil
	})

	if err != nil {
		c.Fatal(err)
	}

	for k, v := range values {
		i.UpdateIndex([]byte(k), v)
	}

	err = i.Find(&Prefix{
		Path:  "name",
		Query: "rasmus",
	}, func(cmp Comparer, k, v []byte) error {
		if !bytes.Equal(v, []byte("id1")) {
			c.Errorf("byte")
		}
		return nil
	})

	c.Assert(err, IsNil)

	and := And(&Contains{
		Path:  "name",
		Query: "kildevaeld",
	}, Equal("age", 31))

	err = i.Find(and, func(cmp Comparer, k, v []byte) error {
		return nil
	})

	c.Assert(err, IsNil)

}
