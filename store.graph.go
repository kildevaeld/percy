package percy

import (
	"os"

	"github.com/google/cayley"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/path"
	"github.com/google/cayley/quad"
)

func (self *Store) AddLink(subject, predicate, object string) error {
	return self.graphdb.AddQuad(cayley.Quad(subject, predicate, object, ""))
}

/*func (self *Store) AddLink(subject Item, predicate string, object Item) error {
	return self.graphdb.AddQuad(cayley.Quad(subject.ACLIdentifier(), predicate, object.ACLIdentifier(), ""))
}*/

func (self *Store) RemoveLink(subject, predicate, object string) error {
	return self.graphdb.RemoveQuad(cayley.Quad(subject, predicate, object, ""))
}

func (self *Store) RemoveQuad(q quad.Quad) error {
	return self.graphdb.RemoveQuad(q)
}

func (self *Store) Graph(start ...string) *path.Path {
	return cayley.StartPath(self.graphdb, start...)
}

func (self *Store) Quads(key []byte, dir quad.Direction, fn func(q quad.Quad) error) error {
	p := self.Graph(string(key))

	it := p.BuildIterator()

	if fn == nil {
		fn = func(quad.Quad) error {
			return nil
		}
	}

	for cayley.RawNext(it) {
		ii := self.graphdb.QuadIterator(dir, it.Result())
		for cayley.RawNext(ii) {
			if err := fn(self.graphdb.Quad(ii.Result())); err != nil {
				return err
			}
		}
	}

	return nil
}

func (self *Store) Each(p *path.Path, dir quad.Direction, fn func(q quad.Quad) error) error {
	it := p.BuildIterator()

	if fn == nil {
		fn = func(quad.Quad) error {
			return nil
		}
	}

	for cayley.RawNext(it) {

		ii := self.graphdb.QuadIterator(dir, it.Result())

		for cayley.RawNext(ii) {
			if err := fn(self.graphdb.Quad(ii.Result())); err != nil {
				return err
			}
		}

	}

	return nil
}

func initGraphDB(path string, config StoreConfig) (*cayley.Handle, error) {

	if _, err := os.Stat(path); err != nil {
		err = graph.InitQuadStore("bolt", path, graph.Options{
			"nosync": config.NoSync,
		})
		if err != nil {
			return nil, err
		}
	}

	qdb, err := cayley.NewGraph("bolt", path, nil)

	if err != nil {
		return nil, err
	}

	return qdb, nil
}
