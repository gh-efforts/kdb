package main

import (
	"context"
	"fmt"
	"github.com/bitrainforest/kdb"
	"github.com/bitrainforest/kdb/store"
	"github.com/bitrainforest/kdb/store/badger"
	"github.com/bitrainforest/kdb/store/etcd"
	"github.com/bitrainforest/kdb/store/redis"
)

func init() {
	kdb.Register(&kdb.Registration{
		Name:        store.Redis,
		FactoryFunc: redis.NewStore,
	})
	kdb.Register(&kdb.Registration{
		Name:        store.Etcd,
		FactoryFunc: etcd.NewStore,
	})
	kdb.Register(&kdb.Registration{
		Name:        store.Badger,
		FactoryFunc: badger.NewStore,
	})
}

func main() {

	//redisDSN := "redis://localhost:6379?compression=zstd&threshold=1024"
	//etcdDSN := "etcd://localhost:2379?compression=zstd&threshold=1024"
	badgerDsn := "badger:///tmp/badger-test-db.db?compression=zstd"

	st, err := kdb.New(badgerDsn)

	if err != nil {
		panic(err)
	}

	defer st.Close()
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	// Put writes to a transaction, call FlushPuts() to ensure all Put entries are properly written to the database.
	err = st.Put(ctx, []byte("key"), []byte("value value"))

	if err != nil {
		fmt.Println("Put error:", err)
		return
	}

	if err := st.FlushPuts(ctx); err != nil {
		fmt.Println("flush error:", err)
		return
	}

	val, err := st.Get(ctx, []byte("key"))

	if err != nil {
		fmt.Println("Get error:", err)
		return
	}

	fmt.Println("Get value:", string(val))
}
