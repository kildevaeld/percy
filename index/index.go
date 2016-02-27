package index

import "sync"

type Index struct {
	Name   string
	Path   string
	Unique bool
}

func (i Index) Equal(v Index) bool {
	return i.Name == v.Name && i.Path == v.Path && i.Unique == v.Unique
}

var entry_pool sync.Pool

func NewEntry() *Entry {
	v := entry_pool.Get().(*Entry)
	return v
}

func DisposeEntry(v *Entry) {
	v.Entries = nil
	v.Id = nil
	entry_pool.Put(v)
}

func init() {
	entry_pool = sync.Pool{
		New: func() interface{} {
			return &Entry{}
		},
	}
}
