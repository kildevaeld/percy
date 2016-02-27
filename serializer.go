package percy

type Serializer interface {
	Encode(dest *[]byte, v interface{}) error
	Decode(source []byte, v interface{}) error
}
