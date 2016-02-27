package index

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/Pallinder/go-randomdata"
	"github.com/boltdb/bolt"
	"github.com/kildevaeld/percy/utils"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func TestBenchmarkIndex(t *testing.T) { TestingT(t) }

type IndexerBenchTestSuite struct {
	index *Indexer
	store *bolt.DB
}

var _ = Suite(&IndexerBenchTestSuite{})

func (self *IndexerBenchTestSuite) SetUpSuite(c *C) {
	db, err := bolt.Open("./index.boltdb", 0766, nil)

	if err != nil {
		c.Fatal(err)
	}

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

	err = i.UpdateIndexSet(indexes, func(key []byte) utils.Map {
		if v, ok := values[string(key)].(utils.Map); ok {
			return v
		}
		return nil
	})

	for ii := 0; ii < 10000; ii++ {
		i.UpdateIndex([]byte(fmt.Sprintf("id%d", ii)), utils.Map{
			"name": randomdata.FirstName(randomdata.Male),
			"age":  rand.Intn(80),
		})
	}

	i.UpdateIndex([]byte("id1"), values["id1"].(utils.Map))
	i.UpdateIndex([]byte("id2"), values["id2"].(utils.Map))
	i.UpdateIndex([]byte("id3"), values["id3"].(utils.Map))

	self.index = i
	self.store = db
}

func (self *IndexerBenchTestSuite) TearDownSuite(c *C) {
	self.store.Close()
	os.Remove("./index.boltdb")
}

func (self IndexerBenchTestSuite) BenchmarkQuery(c *C) {
	//c.ReportAllocs()
	c.ResetTimer()

	for ii := 0; ii < c.N; ii++ {
		self.index.Find(
			&Prefix{
				Path:  "name",
				Query: "rasmus",
			}, nil)
	}

}

func BenchmarkIndex(b *testing.B) {
	b.StopTimer()
	db, err := bolt.Open("./index.boltdb", 0766, nil)

	if err != nil {
		b.Fatal(err)
	}

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

	err = i.UpdateIndexSet(indexes, func(key []byte) utils.Map {
		if v, ok := values[string(key)].(utils.Map); ok {
			return v
		}
		return nil
	})

	b.ReportAllocs()
	b.StartTimer()
	b.ResetTimer()

	for ii := 0; ii < b.N; ii++ {
		i.UpdateIndex([]byte("id2"), values["id2"].(utils.Map))
	}

	db.View(func(t *bolt.Tx) error {

		b.SetBytes(t.Size())

		return nil
	})

	b.StopTimer()
	db.Close()
	os.Remove("./index.boltdb")

	b.StartTimer()
}

func BenchmarkQuery(b *testing.B) {
	b.StopTimer()
	db, err := bolt.Open("./index.boltdb", 0766, nil)

	if err != nil {
		b.Fatal(err)
	}

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

	err = i.UpdateIndexSet(indexes, func(key []byte) utils.Map {
		if v, ok := values[string(key)].(utils.Map); ok {
			return v
		}
		return nil
	})

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
		b.Error(err)
	}

	b.ReportAllocs()
	b.StartTimer()
	b.ResetTimer()

	for ii := 0; ii < b.N; ii++ {
		i.Find(&Prefix{
			Path:  "name",
			Query: "rasmus",
		}, nil)
	}

	b.StopTimer()
	db.Close()
	os.Remove("./index.boltdb")
	b.StartTimer()
}
