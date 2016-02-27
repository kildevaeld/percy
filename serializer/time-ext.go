package serializer

import (
	"fmt"
	"time"

	"github.com/kildevaeld/percy/utils"
)

type TimeExt struct{}

func (x TimeExt) WriteExt(v interface{}) []byte {
	var i int64

	switch v2 := v.(type) {
	case time.Time:
		i = v2.UTC().UnixNano()
	case *time.Time:
		i = v2.UTC().UnixNano()
	default:
		panic(fmt.Sprintln("Unsupported format for bid write"))
	}

	b := make([]byte, 8)
	utils.PutInt64(b, i)

	return b
}
func (x TimeExt) ReadExt(dest interface{}, v []byte) {
	i := utils.GetInt64(v)
	x.UpdateExt(dest, i)
}

func (x TimeExt) ConvertExt(v interface{}) interface{} {
	switch v2 := v.(type) {
	case time.Time:
		return v2.UTC().UnixNano()
	case *time.Time:
		return v2.UTC().UnixNano()
	default:
		panic(fmt.Sprintf("unsupported format for time conversion: expecting time.Time; got %T", v))
	}
}
func (x TimeExt) UpdateExt(dest interface{}, v interface{}) {
	tt := dest.(*time.Time)
	switch v2 := v.(type) {
	case int64:
		*tt = time.Unix(0, v2).UTC()
	case uint64:
		*tt = time.Unix(0, int64(v2)).UTC()
	default:
		panic(fmt.Sprintf("unsupported format for time conversion: expecting int64/uint64; got %T", v))
	}
}
