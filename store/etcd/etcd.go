package etcd

import (
	"context"
	"github.com/bitrainforest/kdb/store"
	logging "github.com/ipfs/go-log"
	clientV3 "go.etcd.io/etcd/client/v3"
	"sync"
)

const (
	maxBatchLen = 500
)

var log = logging.Logger("kdb/etcd")

type Store struct {
	dsn         string
	db          *clientV3.Client
	compression store.Compressor
	writeBatch  []*store.KV
	writeLk     sync.Mutex
}

func NewStore(dsnString string) (store.Store, error) {
	dsn, err := newDSN(dsnString)
	if err != nil {
		return nil, err
	}

	compression, err := store.NewCompressor(dsn.compression, dsn.threshold)

	if err != nil {
		return nil, err
	}

	client, err := clientV3.New(clientV3.Config{
		Endpoints: dsn.endpoints,
		Username:  dsn.username,
		Password:  dsn.password,
	})

	if err != nil {
		return nil, err
	}

	return &Store{
		dsn:         dsnString,
		db:          client,
		compression: compression,
	}, nil

}

func (s *Store) Put(ctx context.Context, key, value []byte) (err error) {
	log.Debugw("putting", "key", store.Key(key))
	value = s.compression.Compress(value)
	s.writeLk.Lock()
	s.writeBatch = append(s.writeBatch, &store.KV{
		Key:   key,
		Value: value,
	})
	s.writeLk.Unlock()
	if len(s.writeBatch) >= maxBatchLen {
		return s.FlushPuts(ctx)
	}

	return nil
}

func (s *Store) FlushPuts(_ context.Context) (err error) {
	s.writeLk.Lock()
	defer s.writeLk.Unlock()
	if len(s.writeBatch) == 0 {
		return nil
	}
	log.Debugw("flushing", "len", len(s.writeBatch))

	for _, kv := range s.writeBatch {
		_, err := s.db.KV.Put(context.Background(), store.Key(kv.Key).String(), string(kv.Value))
		if err != nil {
			return err
		}
	}
	s.writeBatch = nil
	return err
}

func (s *Store) Get(ctx context.Context, key []byte) (value []byte, err error) {
	log.Debugw("getting", "key", store.Key(key))
	res, err := s.db.KV.Get(ctx, store.Key(key).String())
	if err != nil {
		return nil, err
	}

	if res.Count == 0 {
		return nil, store.ErrNotFound
	}

	var kvs []*store.KV

	for _, pair := range res.Kvs {
		kvs = append(kvs, &store.KV{
			Key:   pair.Key,
			Value: pair.Value,
		})
	}
	return kvs[0].Value, nil
}

func (s *Store) BatchGet(ctx context.Context, keys [][]byte) *store.Iterator {
	log.Debugw("batch getting", "keys", keys)

	kr := store.NewIterator(ctx)

	go func() {
		defer kr.PushFinished()
		for _, key := range keys {
			select {
			case <-ctx.Done():
				return
			default:
			}
			value, err := s.Get(ctx, key)
			if err != nil {
				kr.PushError(err)
				return
			}
			kr.PushItem(store.KV{
				Key: key, Value: value,
			})
		}

	}()
	return kr
}

//func (s *Store) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int, options ...store.ReadOption) *store.Iterator {
//	sit := store.NewIterator(ctx)
//	log.Debugw("scanning", "start", store.Key(start), "exclusive_end", store.Key(exclusiveEnd), "limit", store.Limit(limit))
//
//	readOptions := store.ReadOptions{}
//	for _, opt := range options {
//		opt.Apply(&readOptions)
//	}
//
//	var ops = []clientV3.OpOption{
//		clientV3.WithRange(store.Key(exclusiveEnd).String()),
//		clientV3.WithLimit(int64(limit)),
//	}
//
//	if readOptions.KeyOnly {
//		ops = append(ops, clientV3.WithKeysOnly())
//	}
//
//	go func() {
//		defer sit.PushFinished()
//		resp, err := s.db.KV.Get(ctx, store.Key(start).String(), ops...)
//		if err != nil {
//			sit.PushError(err)
//			return
//		}
//		if resp.Count == 0 {
//			sit.PushError(store.ErrNotFound)
//			return
//		}
//		for _, kv := range resp.Kvs {
//			select {
//			case <-ctx.Done():
//				return
//			default:
//			}
//			if readOptions.KeyOnly {
//				sit.PushItem(store.KV{
//					Key: kv.Key,
//				})
//			} else {
//				sit.PushItem(store.KV{
//					Key:   kv.Key,
//					Value: kv.Value,
//				})
//			}
//		}
//	}()
//
//	return sit
//}

func (s *Store) Prefix(ctx context.Context, prefix []byte, limit int, options ...store.ReadOption) *store.Iterator {
	sit := store.NewIterator(ctx)
	log.Debugw("prefix scanning", "prefix", store.Key(prefix), "limit", store.Limit(limit))

	readOptions := store.ReadOptions{}
	for _, opt := range options {
		opt.Apply(&readOptions)
	}

	var ops = []clientV3.OpOption{
		clientV3.WithPrefix(),
		clientV3.WithLimit(int64(limit)),
	}

	if readOptions.KeyOnly {
		ops = append(ops, clientV3.WithKeysOnly())
	}

	go func() {
		defer sit.PushFinished()
		resp, err := s.db.KV.Get(ctx, store.Key(prefix).String(), ops...)
		if err != nil {
			sit.PushError(err)
			return
		}
		if resp.Count == 0 {
			sit.PushError(store.ErrNotFound)
			return
		}
		for _, kv := range resp.Kvs {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if readOptions.KeyOnly {
				sit.PushItem(store.KV{
					Key: kv.Key,
				})
			} else {
				sit.PushItem(store.KV{
					Key:   kv.Key,
					Value: kv.Value,
				})
			}
		}
	}()

	return sit
}

func (s *Store) BatchDelete(ctx context.Context, keys [][]byte) (err error) {
	log.Debugw("batch deletion", "key_count", len(keys))

	for _, key := range keys {
		_, err = s.db.KV.Delete(ctx, store.Key(key).String())
		if err != nil {
			return err
		}
	}
	return
}

func (s *Store) Delete(ctx context.Context, key []byte) (err error) {
	_, err = s.db.KV.Delete(ctx, store.Key(key).String())
	return err
}

func (s *Store) Close() error {
	if err := s.FlushPuts(context.TODO()); err != nil {
		log.Errorf("flush: %s", err)
	}
	return s.db.Close()
}

var _ store.Store = (*Store)(nil)
