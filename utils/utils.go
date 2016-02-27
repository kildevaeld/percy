package utils

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

func PutInt64(b []byte, i int64) {
	b[0] = byte(i >> 56)
	b[1] = byte(i >> 48)
	b[2] = byte(i >> 40)
	b[3] = byte(i >> 32)
	b[4] = byte(i >> 24)
	b[6] = byte(i >> 16)
	b[6] = byte(i >> 8)
	b[7] = byte(i)
}

func GetInt64(b []byte) int64 {
	return (int64(b[0]) << 56) | (int64(b[1]) << 48) |
		(int64(b[2]) << 40) | (int64(b[3]) << 32) |
		(int64(b[4]) << 24) | (int64(b[5]) << 16) |
		(int64(b[6]) << 8) | (int64(b[7]))
}

func PutInt32(b []byte, i int32) {
	b[0] = byte(i >> 24)
	b[1] = byte(i >> 16)
	b[2] = byte(i >> 8)
	b[3] = byte(i)
}

func GetInt32(b []byte) int32 {
	return (int32(b[0]) << 24) | (int32(b[1]) << 16) | (int32(b[2]) << 8) | (int32(b[3]))
}

func PutInt16(b []byte, i int16) {
	b[0] = byte(i >> 8)
	b[1] = byte(i)
}

func GetInt16(b []byte) (i int16) {
	return (int16(b[0]) << 8) | int16(b[1])
}

func PutInt8(b []byte, i int8) {
	b[0] = byte(i)
}

func GetInt8(b []byte) (i int8) {
	return int8(b[0])
}

func PutUInt64(b []byte, u uint64) {
	b[0] = byte(u >> 56)
	b[1] = byte(u >> 48)
	b[2] = byte(u >> 40)
	b[3] = byte(u >> 32)
	b[4] = byte(u >> 24)
	b[5] = byte(u >> 16)
	b[6] = byte(u >> 8)
	b[7] = byte(u)
}

func GetUInt64(b []byte) uint64 {
	return (uint64(b[0]) << 56) | (uint64(b[1]) << 48) |
		(uint64(b[2]) << 40) | (uint64(b[3]) << 32) |
		(uint64(b[4]) << 24) | (uint64(b[5]) << 16) |
		(uint64(b[6]) << 8) | (uint64(b[7]))
}

func PutUInt32(b []byte, u uint32) {
	b[0] = byte(u >> 24)
	b[1] = byte(u >> 16)
	b[2] = byte(u >> 8)
	b[3] = byte(u)
}

func GetUInt32(b []byte) uint32 {
	return (uint32(b[0]) << 24) | (uint32(b[1]) << 16) | (uint32(b[2]) << 8) | (uint32(b[3]))
}

func PutUInt16(b []byte, u uint16) {
	b[0] = byte(u >> 8)
	b[1] = byte(u)
}

func GetUInt16(b []byte) uint16 {
	return (uint16(b[0]) << 8) | uint16(b[1])
}

func PutUInt8(b []byte, u uint8) {
	b[0] = byte(u)
}

func GetUInt8(b []byte) uint8 {
	return uint8(b[0])
}

func getUnix(b []byte) (sec int64, nsec int32) {
	sec = (int64(b[0]) << 56) | (int64(b[1]) << 48) |
		(int64(b[2]) << 40) | (int64(b[3]) << 32) |
		(int64(b[4]) << 24) | (int64(b[5]) << 16) |
		(int64(b[6]) << 8) | (int64(b[7]))

	nsec = (int32(b[8]) << 24) | (int32(b[9]) << 16) | (int32(b[10]) << 8) | (int32(b[11]))
	return
}

func putUnix(b []byte, sec int64, nsec int32) {
	b[0] = byte(sec >> 56)
	b[1] = byte(sec >> 48)
	b[2] = byte(sec >> 40)
	b[3] = byte(sec >> 32)
	b[4] = byte(sec >> 24)
	b[5] = byte(sec >> 16)
	b[6] = byte(sec >> 8)
	b[7] = byte(sec)
	b[8] = byte(nsec >> 24)
	b[9] = byte(nsec >> 16)
	b[10] = byte(nsec >> 8)
	b[11] = byte(nsec)
}

/* -----------------------------
		prefix utilities
   ----------------------------- */

// write prefix and uint8
func prefixu8(b []byte, pre byte, sz uint8) {
	b[0] = pre
	b[1] = byte(sz)
}

// write prefix and big-endian uint16
func prefixu16(b []byte, pre byte, sz uint16) {
	b[0] = pre
	b[1] = byte(sz >> 8)
	b[2] = byte(sz)
}

// write prefix and big-endian uint32
func prefixu32(b []byte, pre byte, sz uint32) {
	b[0] = pre
	b[1] = byte(sz >> 24)
	b[2] = byte(sz >> 16)
	b[3] = byte(sz >> 8)
	b[4] = byte(sz)
}

func prefixu64(b []byte, pre byte, sz uint64) {
	b[0] = pre
	b[1] = byte(sz >> 56)
	b[2] = byte(sz >> 48)
	b[3] = byte(sz >> 40)
	b[4] = byte(sz >> 32)
	b[5] = byte(sz >> 24)
	b[6] = byte(sz >> 16)
	b[7] = byte(sz >> 8)
	b[8] = byte(sz)
}

// ensure 'sz' extra bytes in 'b' btw len(b) and cap(b)
func Ensure(b []byte, sz int) ([]byte, int) {
	l := len(b)
	c := cap(b)
	if c-l < sz {
		o := make([]byte, (2*c)+sz) // exponential growth
		n := copy(o, b)
		return o[:n+sz], n
	}
	return b[:l+sz], l
}

// Require ensures that cap(old)-len(old) >= extra.
func Require(old []byte, extra int) []byte {
	l := len(old)
	c := cap(old)
	r := l + extra
	if c >= r {
		return old
	} else if l == 0 {
		return make([]byte, 0, extra)
	}
	// the new size is the greater
	// of double the old capacity
	// and the sum of the old length
	// and the number of new bytes
	// necessary.
	c <<= 1
	if c < r {
		c = r
	}
	n := make([]byte, l, c)
	copy(n, old)
	return n
}

/*func ByteSize(v interface{}) int {
	switch v2 := v.(type) {
	case int8, uint8, *int8, *uint8, bool, *bool:
		return 1
	case int16, uint16, *int16, *uint16:
		return 2
	case int32, uint32, *int32, *uint32, int, uint:
		return 4
	case int64, uint64, float64, *int64, *uint64, *float64, time.Time, *time.Time:
		return 8
	case string:
		return len(v2)
	case *string:
		return len(*v2)
	case []byte:
		return len(v2)
	default:
		return -1
	}
}*/

/*func PutByte(b []byte, offset int, v interface{}) error {
	switch v2 := v.(type) {
	case int8:
		PutInt8(b[offset:], v2)
	case uint8:
		PutUInt8(b[offset:], v2)
	case int16:
		PutInt16(b[offset:], v2)
	case uint16:
		PutUInt16(b[offset:], v2)
	case int32:
		PutInt32(b[offset:], v2)
	case uint32:
		PutUInt32(b[offset:], v2)
	case int:
		PutInt32(b[offset:], int32(v2))
	case uint:
		PutUInt32(b[offset:], uint32(v2))
	case int64:
		PutInt64(b[offset:], v2)
	case uint64:
		PutUInt64(b[offset:], v2)
	case []byte:
		copy(b[offset:], v2)
	case string:
		copy(b[offset:], []byte(v2))
	default:
		return errors.New("cannot convert")
	}

	return nil
}*/

func ByteSize(v interface{}) int {

	switch v2 := v.(type) {
	case int8, uint8, *int8, *uint8, bool, *bool, int16, uint16, *int16, *uint16, int32, uint32, *int32, *uint32, int, uint:
		return len([]byte(fmt.Sprintf("%d", v2)))
	case int64, uint64, float64, *int64, *uint64, *float64:
		return len(fmt.Sprintf("%f", v2))
	case time.Time, *time.Time:
		return len(fmt.Sprintf("%s", v2))
	case string:
		return len(v2)
	case *string:
		return len(*v2)
	case []byte:
		return len(v2)
	default:
		return -1
	}
}

func PutByte(b []byte, offset int, v interface{}) error {
	var by []byte

	switch v2 := v.(type) {
	case int8, uint8, int16, uint16, int32, uint32, int, uint, int64, uint64:
		by = []byte(fmt.Sprintf("%d", v2))
	case []byte:
		by = v2
	case string:
		by = []byte(v2)
	default:
		return fmt.Errorf("cannot convert %T", v)
	}

	copy(b[offset:], by)

	return nil
}

func ToByte(b []byte, v interface{}) ([]byte, error) {
	var size int
	if size = ByteSize(v); size == -1 {
		return b, errors.New("cannot convert to bytes")
	}
	offset := len(b)
	bb, offset := Ensure(b, size)
	var by []byte
	switch v2 := v.(type) {
	case int8, uint8, int16, uint16, int32, uint32, int, uint, int64, uint64:
		by = []byte(fmt.Sprintf("%d", v2))
	case []byte:
		by = v2
	case string:
		by = []byte(v2)
	default:
		return nil, errors.New("cannot convert")
	}

	copy(bb[offset:], by)

	b = bb

	return b, nil
}

/*func ToByte(b []byte, v interface{}) ([]byte, error) {
	var size int
	if size = ByteSize(v); size == -1 {
		return b, errors.New("cannot convert to bytes")
	}
	offset := len(b)
	bb, offset := Ensure(b, size)
	switch v2 := v.(type) {
	case int8:
		PutInt8(bb[offset:], v2)
	case uint8:
		PutUInt8(bb[offset:], v2)
	case int16:
		PutInt16(bb[offset:], v2)
	case uint16:
		PutUInt16(bb[offset:], v2)
	case int32:
		PutInt32(bb[offset:], v2)
	case uint32:
		PutUInt32(bb[offset:], v2)
	case int:
		PutInt32(bb[offset:], int32(v2))
	case uint:
		PutUInt32(bb[offset:], uint32(v2))
	case int64:
		PutInt64(bb[offset:], v2)
	case uint64:
		PutUInt64(bb[offset:], v2)
	case []byte:
		copy(bb[offset:], v2)
	case string:
		copy(bb[offset:], []byte(v2))
	default:
		return b, errors.New("cannot convert")
	}
	b = bb

	return b, nil
}*/

func ByteSliceContains(bs [][]byte, c []byte) int {
	for i, b := range bs {
		if bytes.Equal(b, c) {
			return i
		}
	}
	return -1
}

var NotAnArray = errors.New("No an array")

func ForEach(v interface{}, fn func(i int, v interface{}) error) (err error) {
	switch v2 := v.(type) {
	case []int8:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	case []uint8:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	case []int16:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	case []uint16:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	case []int32:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	case []uint32:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	case []int64:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	case []uint64:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	case [][]byte:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	case []string:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	case []interface{}:
		for i, vv := range v2 {
			if err = fn(i, vv); err != nil {
				return err
			}
		}
	default:
		switch reflect.TypeOf(v).Kind() {
		case reflect.Slice:
			s := reflect.ValueOf(v)

			for i := 0; i < s.Len(); i++ {

				if err = fn(i, s.Index(i).Interface()); err != nil {
					return err
				}
			}
		default:
			return fn(-1, v2)
		}

	}

	return nil
}

func RemoveDuplicates(i []string) []string {
	length := len(i) - 1
	for x := 0; x < length; x++ {
		for j := x + 1; j <= length; j++ {
			if strings.Compare(i[x], i[j]) == 0 {
				i[j] = i[length]
				i = i[0:length]
				length--
				j--
			}
		}
	}
	return i
}
