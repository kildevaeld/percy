// Code generated by "stringer -type EncodingType encoding.go"; DO NOT EDIT

package serializer

import "fmt"

const _EncodingType_name = "MsgPackJSONBincCbor"

var _EncodingType_index = [...]uint8{0, 7, 11, 15, 19}

func (i EncodingType) String() string {
	if i < 0 || i >= EncodingType(len(_EncodingType_index)-1) {
		return fmt.Sprintf("EncodingType(%d)", i)
	}
	return _EncodingType_name[_EncodingType_index[i]:_EncodingType_index[i+1]]
}
