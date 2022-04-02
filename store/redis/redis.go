package redis

import (
	"context"
	"fmt"
	"github.com/bitrainforest/kdb/store"
	"github.com/go-redis/redis/v8"
	logging "github.com/ipfs/go-log"
	"sync"
)

const (
	maxBatchLen = 500
)

var log = logging.Logger("kdb/redis")

type Store struct {
	dsn         string
	db          *redis.Client
	compression store.Compressor
	writeBatch  redis.Pipeliner
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

	opt, err := redis.ParseURL(dsn.url)

	if err != nil {
		return nil, fmt.Errorf("cannot parse redis dsn %q: %w", dsnString, err)
	}

	client := redis.NewClient(opt)

	err = client.Ping(context.TODO()).Err()

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
	if s.writeBatch == nil {
		s.writeBatch = s.db.TxPipeline()
	}
	err = s.writeBatch.Set(ctx, store.Key(key).String(), value, 0).Err()

	if err != nil {
		return fmt.Errorf("set entry: %w", err)
	}

	if s.writeBatch.Len() >= maxBatchLen {
		_, err = s.writeBatch.Exec(ctx)
		if err != nil {
			return fmt.Errorf("batch exec: %w", err)
		}
		s.writeBatch = s.db.TxPipeline()
	}
	return nil
}

func (s *Store) FlushPuts(ctx context.Context) (err error) {
	if s.writeBatch == nil {
		return nil
	}
	_, err = s.writeBatch.Exec(ctx)
	if err != nil {
		return fmt.Errorf("batch exec: %w", err)
	}
	s.writeBatch = s.db.TxPipeline()
	return nil
}

func (s *Store) Get(ctx context.Context, key []byte) (value []byte, err error) {
	log.Debugw("getting", "key", store.Key(key))
	val, err := s.db.Get(ctx, store.Key(key).String()).Bytes()
	if err != nil {
		return nil, warpRedisError(err)
	}
	return val, nil
}

func (s *Store) BatchGet(ctx context.Context, keys [][]byte) *store.Iterator {
	log.Debugw("batch get", "key_count", len(keys))

	var strKeys []string
	for _, key := range keys {
		strKeys = append(strKeys, store.Key(key).String())
	}

	kr := store.NewIterator(ctx)

	go func() {
		defer kr.PushFinished()
		cmd := s.db.MGet(ctx, strKeys...)
		res, err := cmd.Result()
		if err != nil {
			kr.PushError(warpRedisError(err))
			return
		}
		for i, val := range res {
			if v, ok := val.([]byte); ok {
				kr.PushItem(store.KV{
					Key:   keys[i],
					Value: v,
				})
			} else {
				kr.PushError(fmt.Errorf("unexpected type: %T", val))
				return
			}
		}
	}()
	return kr
}

func (s *Store) Prefix(ctx context.Context, prefix []byte, limit int, options ...store.ReadOption) *store.Iterator {
	log.Debugw("prefix", "prefix", store.Key(prefix), "limit", limit)

	var opts store.ReadOptions
	for _, o := range options {
		o.Apply(&opts)
	}
	kr := store.NewIterator(ctx)
	go func() {
		defer kr.PushFinished()
		cmd := s.db.Scan(ctx, 0, store.Key(prefix).String(), int64(limit))
		sit := cmd.Iterator()
		for sit.Next(ctx) {
			if sit.Err() != nil {
				kr.PushError(warpRedisError(sit.Err()))
				return
			}
			if opts.KeyOnly {
				kr.PushItem(store.KV{
					Key: []byte(sit.Val()),
				})
			} else {
				val, err := s.db.Get(ctx, sit.Val()).Bytes()
				if err != nil {
					kr.PushError(warpRedisError(err))
					return
				}
				kr.PushItem(store.KV{
					Key:   []byte(sit.Val()),
					Value: val,
				})
			}
		}
	}()
	return kr
}

func (s *Store) Delete(ctx context.Context, key []byte) (err error) {
	log.Debugw("deleting", "key", store.Key(key))
	err = s.db.Del(ctx, store.Key(key).String()).Err()
	if err != nil {
		return fmt.Errorf("delete entry: %w", err)
	}
	return nil
}

func (s *Store) BatchDelete(ctx context.Context, keys [][]byte) (err error) {
	log.Debugw("batch delete", "key_count", len(keys))
	var strKeys []string
	for _, key := range keys {
		strKeys = append(strKeys, store.Key(key).String())
	}
	err = s.db.Del(ctx, strKeys...).Err()
	if err != nil {
		return fmt.Errorf("batch delete: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	if s.writeBatch != nil && s.writeBatch.Len() > 0 {
		if err := s.FlushPuts(context.TODO()); err != nil {
			log.Errorf("flush puts: %s", err)
		}
	}
	return s.db.Close()
}

var _ store.Store = (*Store)(nil)

func warpRedisError(err error) error {
	if err == redis.Nil {
		return store.ErrNotFound
	}
	return err
}
