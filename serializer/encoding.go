//go:generate stringer -type=EncodingType
package serializer

import "errors"

type EncodingType int

const (
	MsgPack EncodingType = iota
	JSON
	Binc
	Cbor
)

func (self EncodingType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + self.String() + "\""), nil
}

func (self *EncodingType) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case "MsgPack":
		*self = MsgPack
	case "JSON":
		*self = JSON
	case "Binc":
		*self = Binc
	case "Cbor":
		*self = Cbor
	}
	return errors.New(string(data) + " is not a EncodingType")
}
