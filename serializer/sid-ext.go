package serializer

import (
	"fmt"

	"github.com/kildevaeld/percy/utils"
)

type SidExt struct{}

func (x SidExt) WriteExt(v interface{}) []byte {
	switch v2 := v.(type) {
	case utils.Sid:
		return []byte(v2)
	case *utils.Sid:
		return []byte(*v2)
	default:
		panic(fmt.Sprintln("Unsupported format for Sid write"))
	}

}

func (x SidExt) ReadExt(dest interface{}, v []byte) {
	tt := dest.(*utils.Sid)
	*tt = utils.Sid(string(v))
}

func (x SidExt) ConvertExt(v interface{}) interface{} {
	switch v2 := v.(type) {
	case utils.Sid:
		return []byte(v2)
	case *utils.Sid:
		return []byte(*v2)
	default:
		panic(fmt.Sprintf("unsupported format for time conversion: expecting utils.Sid; got %T", v))
	}
}
func (x SidExt) UpdateExt(dest interface{}, v interface{}) {
	tt := dest.(*utils.Sid)
	switch v2 := v.(type) {
	case []byte:
		*tt = utils.Sid(string(v2))
	case string:
		*tt = utils.Sid(v2)
	default:
		panic(fmt.Sprintf("unsupported format for time conversion: expecting int64/uint64; got %T", v))
	}
}
