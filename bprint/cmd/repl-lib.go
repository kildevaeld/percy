package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/googollee/gocmd"
	"github.com/kildevaeld/percy"
	"github.com/kildevaeld/percy/index"
	"github.com/kildevaeld/percy/utils"
)

type TestServer struct {
	db *store.Store
}

func (s *TestServer) Ls(server gocmd.CmdServer, args []string) {

	if len(args) == 1 {
		var m []store.Map
		if err := s.db.List([]byte(args[0]), &m); err != nil {
			fmt.Printf("Error: %s\n", err)
			return
		}

		var b []byte
		var err error
		if b, err = json.MarshalIndent(m, "", "  "); err != nil {
			fmt.Printf("Error %v\n", err)
			return
		}

		fmt.Printf("%s\n", b)

	} else {
		m := s.db.Meta()
		fmt.Println("Buckets:")
		for _, b := range m.Buckets {
			fmt.Printf("  %s\n", b.Name)
		}
		fmt.Println("")
	}

}

func trim(q string) string {
	q = strings.TrimPrefix(q, "*")
	return strings.TrimSuffix(q, "*")
}

func (s *TestServer) Get(server gocmd.CmdServer, args []string) {
	defer fmt.Println()
	msg := "\nusage:\nget <bucket> [id]. Or \nget <bucket> where <attribute> =|>|<| (*)query(*)\n"
	if len(args) < 2 {
		fmt.Printf(msg)
		return
	}

	if len(args) == 2 {
		var m utils.Map
		if err := s.db.Get([]byte(args[0]), args[1], &m); err != nil {
			fmt.Printf("Error %v\n", err)
			return
		}

		var b []byte
		var err error

		if b, err = json.MarshalIndent(m, "", "  "); err != nil {
			fmt.Printf("Error %v\n", err)
			return
		}

		fmt.Printf("%s\n", b)

	} else if len(args) == 5 {
		attr := []byte(args[2])
		q := args[4]
		var comp index.Comparer
		switch args[3] {
		case "=":
			if strings.HasPrefix(q, "*") && strings.HasSuffix(q, "*") {
				q = trim(q)
				comp = &index.Contains{
					Path:  string(attr),
					Query: q,
				}
			} else if strings.HasPrefix(q, "*") {
				q = trim(q)
				comp = &index.Prefix{
					Path:  string(attr),
					Query: q,
				}
			} else if strings.HasSuffix(q, "*") {
				q = trim(q)
				comp = &index.Suffix{
					Path:  string(attr),
					Query: q,
				}
			} else {
				/*comp = &index.Equal{
					Path:  attr,
					Query: q,
				}*/
				comp = index.Equal(string(attr), q)
			}

		}

		if comp == nil {
			fmt.Printf(msg)
			return
		}

		var m []store.Map

		/*if err := s.db.Find([]byte(args[0]), comp, &m); err != nil {
			fmt.Printf("Error %v\n", err)
			return
		}*/
		if err := s.db.Query([]byte(args[0]), comp).All(&m); err != nil {
			fmt.Printf("Error %v\n", err)
			return
		}

		var b []byte
		var err error
		if b, err = json.MarshalIndent(m, "", "  "); err != nil {
			fmt.Printf("Error %v\n", err)
			return
		}

		fmt.Printf("%s\n", b)

	} else {
		fmt.Printf(msg)
	}

}

func (s *TestServer) Index(server gocmd.CmdServer, args []string) {
	msg := "usage: index <bucket> [add] [name] [path]\n"
	if len(args) == 0 {
		fmt.Print(msg)
		return
	}

	if len(args) == 1 {

		return
	}

	var err error
	switch args[1] {
	case "add":
		i := index.Index{
			Name: args[2],
			Path: args[2],
		}
		if len(args) == 4 {
			i.Path = args[3]
		}
		err = s.db.EnsureIndexes(utils.Map{
			args[0]: i,
		})
	}

	if err != nil {
		fmt.Printf("Got error: %s", err)
	}

	fmt.Println("")

}

func (s *TestServer) Create(server gocmd.CmdServer, args []string) {
	msg := "usage: create <bucket>\n"
	if len(args) == 0 {
		fmt.Print(msg)
		return
	}
	fmt.Printf("Creating bucket %s\n", args[0])
	if err := s.db.InitBucket([]byte(args[0])); err != nil {
		fmt.Printf("Error while creating bucket: %s\n  %s\n", err)
	} else {
		fmt.Printf("Bucket %s created\n", args[0])
	}

	fmt.Println("")
}

func (s *TestServer) Echo(server gocmd.CmdServer, args []string) {
	fmt.Println(strings.Join(args, " "))
}

func (s *TestServer) Quit(server gocmd.CmdServer, args []string) {
	server.Exit(0)
}

func NewServer(db *store.Store) *TestServer {
	return &TestServer{db}
}
