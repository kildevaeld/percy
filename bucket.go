package percy

type Bucket struct {
	store  *Store
	bucket []byte
}

type InsertOptions struct {
	Overwrite bool
}

func (self *Bucket) BulkInsert() {

}

func (self *Bucket) Insert(key []byte, value interface{}) error {
	return nil
}

func (self *Bucket) Remove(key []byte, value interface{}) error {
	return nil
}

func (self *Bucket) Get(key []byte) error {
    return nil
}

func (slf *Bucket) Query() *Query {
	return nil
}

func (self *Bucket) Destroy() error {

	return nil
}
