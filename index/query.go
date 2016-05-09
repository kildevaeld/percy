package index

import (
	"bytes"
	"fmt"
	"github.com/kildevaeld/dict"
	"github.com/kildevaeld/percy/utils"
	"strconv"
	"strings"
	"unicode"
)

func getNumber(name string) (new_s int) {
	// Need a key to sort the names by. In this case, it's the number contained in the name
	// Example: M23abcdeg ---> 23
	// Of course, this tends to break hideously once complicated stuff is involved:
	// For example, 'ülda123dmwak142.e2dööööwq,' what do you sort by here?
	// Could be 123, could be 142, or 2, could be 1231422 (this takes the last case)

	s := make([]string, 0)
	for _, element := range name {
		if unicode.IsNumber(element) {
			s = append(s, string(element))
		}
	}
	new_s, err := strconv.Atoi(strings.Join(s, ""))
	if err != nil {
		return -1
	}
	return new_s
}

func compile(p []byte, q interface{}) []byte {

	size := utils.ByteSize(q)

	if size == -1 {
		panic("intefacer cannot be serialized to bytes")
	}
	plen := len(p) + 1
	b := make([]byte, size+plen)

	copy(b, []byte(string(p)+":"))

	if err := utils.PutByte(b, plen, q); err != nil {
		panic(err.Error())
	}

	return b
}

type Comparer interface {
	Paths() []string
	Compare(path string, v, k []byte) bool
	WriteMap(dict.Map) error
}

type equal struct {
	Query  interface{}
	Path   string
	prefix []byte
}

func (self *equal) WriteMap(m dict.Map) error {
	m[self.Path] = self.Query
	return nil
}

func (self *equal) getPrefix() []byte {
	if self.prefix == nil {
		self.prefix = compile([]byte(self.Path), self.Query)
	}
	return self.prefix
}

func (self *equal) Paths() []string {
	return []string{self.Path}
}

func (self *equal) Compare(p string, v, s []byte) bool {
	return bytes.Equal(s, self.getPrefix())
}

func (self *equal) String() string {
	return fmt.Sprintf("%s = %v", self.Path, self.Query)
}

func Equal(path string, query interface{}) Comparer {
	return &equal{
		Query: query,
		Path:  path,
	}
}

type Greater struct {
	Query  interface{}
	Path   string
	prefix []byte
}

func (self *Greater) WriteMap(m dict.Map) error {
	m[self.Path] = self.Query
	return nil
}

func (self *Greater) getPrefix() []byte {
	if self.prefix == nil {
		self.prefix = compile([]byte(self.Path), self.Query)
	}
	return self.prefix
}

func (self *Greater) Paths() []string {
	return []string{self.Path}
}

func (self *Greater) Compare(p string, v, s []byte) bool {

	if !bytes.HasPrefix(s, []byte(self.Path)) {
		return false
	}

	pp, _ := utils.ToByte(nil, self.Query)

	var sn, qn int
	if sn = getNumber(string(s)); sn == -1 {
		return bytes.Compare(s, pp) > 0
	}

	if qn = getNumber(string(pp)); qn == -1 {
		return bytes.Compare(s, pp) > 0
	}

	return sn > qn
}

func (self *Greater) String() string {
	return fmt.Sprintf("%s > %v", self.Path, self.Query)
}

type Lesser struct {
	Query  interface{}
	Path   string
	prefix []byte
}

func (self *Lesser) WriteMap(m dict.Map) error {
	m[self.Path] = self.Query
	return nil
}

func (self *Lesser) getPrefix() []byte {
	if self.prefix == nil {
		self.prefix = compile([]byte(self.Path), self.Query)
	}
	return self.prefix
}

func (self *Lesser) Paths() []string {
	return []string{self.Path}
}

func (self *Lesser) Compare(p string, v, s []byte) bool {
	if !bytes.HasPrefix(s, []byte(self.Path)) {
		return false
	}

	pp, _ := utils.ToByte(nil, self.Query)

	var sn, qn int
	if sn = getNumber(string(s)); sn == -1 {
		return bytes.Compare(s, pp) < 0
	}

	if qn = getNumber(string(pp)); qn == -1 {
		return bytes.Compare(s, pp) < 0
	}

	return sn < qn
}

func (self *Lesser) String() string {
	return fmt.Sprintf("%s < %v", self.Path, self.Query)
}

type Prefix struct {
	Query  interface{}
	Path   string
	prefix []byte
}

func (self *Prefix) WriteMap(m dict.Map) error {
	m[self.Path] = self.Query
	return nil
}

func (self *Prefix) getPrefix() []byte {
	if self.prefix == nil {
		self.prefix = compile([]byte(self.Path), self.Query)
	}
	return self.prefix
}

func (self *Prefix) Paths() []string {
	return []string{self.Path}
}

func (self *Prefix) Compare(p string, v, s []byte) bool {

	return bytes.HasPrefix(s, self.getPrefix())
}

func (self *Prefix) String() string {
	return fmt.Sprintf("%s ~= %v*", self.Path, self.Query)
}

type Suffix struct {
	Query  interface{}
	Path   string
	prefix []byte
	q      []byte
}

func (self *Suffix) WriteMap(m dict.Map) error {
	m[self.Path] = self.Query
	return nil
}

func (self *Suffix) getPrefix() []byte {
	if self.prefix == nil {
		self.prefix = compile([]byte(self.Path), self.Query)
	}
	return self.prefix
}

func (self *Suffix) Paths() []string {
	return []string{self.Path}
}

func (self *Suffix) Compare(p string, v, s []byte) bool {
	if self.q == nil {
		var bb []byte
		b, e := utils.ToByte(bb, self.Query)
		if e != nil {
			panic(e)
		}
		self.q = b
	}
	return bytes.HasSuffix(s, self.q)
}

func (self *Suffix) String() string {
	return fmt.Sprintf("%s ~= *%v", self.Path, self.Query)
}

type Contains struct {
	Query  interface{}
	Path   string
	prefix []byte
	q      []byte
}

func (self *Contains) WriteMap(m dict.Map) error {
	m[self.Path] = self.Query
	return nil
}

func (self *Contains) getPrefix() []byte {
	if self.prefix == nil {
		self.prefix = compile([]byte(self.Path), self.Query)
	}
	return self.prefix
}

func (self *Contains) Paths() []string {
	return []string{self.Path}
}

func (self *Contains) Compare(p string, v, s []byte) bool {
	if self.q == nil {
		var bb []byte
		b, e := utils.ToByte(bb, self.Query)
		if e != nil {
			panic(e)
		}
		self.q = b
	}
	return bytes.Contains(s, self.q)
}

func (self *Contains) String() string {
	return fmt.Sprintf("%s ~= *%v*", self.Path, self.Query)
}

type and struct {
	queries []Comparer
	found   map[string]int
}

func (self *and) WriteMap(m dict.Map) error {
	out := dict.NewMap()

	for _, q := range self.queries {
		if e := q.WriteMap(out); e != nil {
			return e
		}
	}

	m["$and"] = out

	return nil
}

func (self *and) Compare(path string, v, key []byte) bool {
	ln := len(self.queries)
	for _, q := range self.queries {
		if q.Compare(path, v, key) {

			entry := NewEntry()

			entry.Unmarshal(v)

			for _, e := range entry.Entries {
				if _, ok := self.found[string(e)]; !ok {
					self.found[string(e)] = 1
				} else {
					self.found[string(e)]++
				}

				if self.found[string(e)] == ln {
					return true
				}
			}

		}
	}

	return false
}

func (self *and) Paths() []string {
	var s []string
	for _, q := range self.queries {
		s = append(s, q.Paths()...)
	}
	return utils.RemoveDuplicates(s)
}

func And(queries ...Comparer) Comparer {
	return &and{queries, make(map[string]int)}
}

type or struct {
	queries []Comparer
	//found map[]
}

func (self *or) WriteMap(m dict.Map) error {
	out := dict.NewMap()

	for _, q := range self.queries {
		if e := q.WriteMap(out); e != nil {
			return e
		}
	}

	m["$or"] = out

	return nil
}

func (self *or) Compare(p string, v, s []byte) bool {
	for _, q := range self.queries {
		if q.Compare(p, v, s) {
			return true
		}
	}
	return false
}

func (self *or) Paths() []string {
	var s []string
	for _, q := range self.queries {
		s = append(s, q.Paths()...)
	}
	return s
}

func Or(queries ...Comparer) Comparer {
	return &or{queries}
}
