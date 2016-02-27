package serializer

import (
	"reflect"
	"sync"
	"time"

	"github.com/kildevaeld/percy/utils"
	"github.com/ugorji/go/codec"
)

const (
	SidExtensionIdentifier  = 1
	TimeExtensionIdentifier = 2
)

type Serializer struct {
	encoder *codec.Encoder
	decoder *codec.Decoder
	eLock   sync.Mutex
	dLock   sync.Mutex
}

func (self *Serializer) Encode(dest *[]byte, v interface{}) error {
	self.eLock.Lock()
	defer self.eLock.Unlock()
	self.encoder.ResetBytes(dest)
	return self.encoder.Encode(v)
}

func (self *Serializer) Decode(source []byte, v interface{}) error {
	self.dLock.Lock()
	defer self.dLock.Unlock()
	self.decoder.ResetBytes(source)
	return self.decoder.Decode(v)
}

func NewSerializerWithHandle(handler codec.Handle) *Serializer {
	var b []byte
	return &Serializer{
		encoder: codec.NewEncoderBytes(&b, handler),
		decoder: codec.NewDecoderBytes(nil, handler),
	}
}

func NewSerializer(encoding EncodingType) *Serializer {

	var handler codec.Handle

	var sidExt SidExt
	var sid utils.Sid
	sidType := reflect.TypeOf(sid)

	timeType := reflect.TypeOf(time.Time{})
	var timeExt TimeExt

	switch encoding {
	case JSON:
		jh := &codec.JsonHandle{}
		jh.SetExt(sidType, SidExtensionIdentifier, sidExt)
		jh.SetExt(timeType, TimeExtensionIdentifier, timeExt)

		//jh.IntegerAsString = 'L'
		handler = jh

	case MsgPack:
		mh := &codec.MsgpackHandle{}
		mh.RawToString = true
		mh.MapType = reflect.TypeOf(utils.Map{})
		mh.WriteExt = true

		mh.SetExt(sidType, SidExtensionIdentifier, sidExt)
		mh.SetBytesExt(sidType, SidExtensionIdentifier, sidExt)

		mh.SetExt(timeType, TimeExtensionIdentifier, timeExt)
		mh.SetBytesExt(timeType, TimeExtensionIdentifier, timeExt)

		handler = mh

	case Binc:
		bh := &codec.BincHandle{}
		bh.SetExt(sidType, SidExtensionIdentifier, sidExt)
		bh.SetExt(timeType, TimeExtensionIdentifier, timeExt)
		handler = bh
	case Cbor:
		ch := &codec.CborHandle{}
		ch.SetExt(sidType, SidExtensionIdentifier, sidExt)
		ch.SetExt(timeType, TimeExtensionIdentifier, timeExt)
		handler = ch
	default:
		panic("encoder error")
	}

	return NewSerializerWithHandle(handler)

}
