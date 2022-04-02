package kdb

import (
	"fmt"
	"github.com/bitrainforest/kdb/store"
	logging "github.com/ipfs/go-log"
	"strings"
)

var log = logging.Logger("kdb")

// NewStoreFunc is a function for opening a database.
type NewStoreFunc func(path string) (store.Store, error)

type Registration struct {
	Name        store.Name // unique name
	FactoryFunc NewStoreFunc
}

var registry = make(map[store.Name]*Registration)

func Register(reg *Registration) {
	if reg.Name == "" {
		log.Fatal("name cannot be blank")
	} else if _, ok := registry[reg.Name]; ok {
		log.Fatalw("already registered", "name", reg.Name)
	}
	registry[reg.Name] = reg
}

func New(dsn string, opts ...store.Option) (store.Store, error) {
	chunks := strings.Split(dsn, ":")
	reg, found := registry[store.Name(chunks[0])]
	if !found {
		return nil, fmt.Errorf("no such kv store registered %q", chunks[0])
	}
	st, err := reg.FactoryFunc(dsn)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt.Apply(st)
	}
	return st, nil
}

// ByName returns a registered store driver
func ByName(name string) *Registration {
	r, ok := registry[store.Name(name)]
	if !ok {
		return nil
	}
	return r
}
