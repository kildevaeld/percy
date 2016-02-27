package serializer

import (
	"testing"

	"github.com/kildevaeld/blueprint/models"
	"github.com/ugorji/go/codec"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func TestSerializer(t *testing.T) { TestingT(t) }

type SerializerTestSuite struct {
	store *Store
}

var _ = Suite(&SerializerTestSuite{})

func (ts *SerializerTestSuite) TestSerialize(c *C) {

	s := NewSerializerWithHandle(&codec.MsgpackHandle{})

	var b []byte

	p := &models.Project{
		Name: "String",
	}

	err := s.Encode(&b, p)

	c.Assert(err, IsNil)

}

func BenchmarkSerialize(c *testing.B) {
	c.StopTimer()
	var b []byte
	p := &models.Project{
		Name: "String",
	}

	s := NewSerializer(&codec.MsgpackHandle{})
	c.ReportAllocs()
	c.StartTimer()
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		s.Encode(&b, p)
	}

}
