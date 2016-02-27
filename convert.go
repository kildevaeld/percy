package percy

import (
	"fmt"

	"github.com/kildevaeld/percy/utils"
)

func ToString(i interface{}) string {

	switch i.(type) {
	case uint8, int8, uint16, int16, uint32, int32, uint64, int64, int, uint:
		return fmt.Sprintf("%d", i)
	case float32, float64, complex64:
		return fmt.Sprintf("%g", i)
	case bool:
		return fmt.Sprintf("%t", i)
	case string:
		return i.(string)
	case []byte:
		return string(i.([]byte))
	default:
		return ""
	}

}

func ToByte(v interface{}) (id []byte) {
	switch v.(type) {
	case string:
		if utils.IsSidHex(v.(string)) {
			v = utils.SidHex(v.(string))
		}
		id = []byte(v.(string))
	case utils.Sid:
		id = []byte(v.(utils.Sid))
	case []byte:
		id = v.([]byte)
	default:
		id = nil
	}
	return
}
