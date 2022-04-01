package badger

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bitrainforest/kdb/store"
	"github.com/dgraph-io/badger/v3"
	logging "github.com/ipfs/go-log"
	"os"
	"path/filepath"
)

var log = logging.Logger("kdb/badger")

type Store struct {
	dsn        string
	db         *badger.DB
	writeBatch *badger.WriteBatch
}

var _ store.Store = (*Store)(nil)

func (s *Store) String() string {
	return fmt.Sprintf("badger kv store with dsn: %q", s.dsn)
}

func NewStore(dsnString string) (store.Store, error) {
	dsn, err := newDSN(dsnString)
	if err != nil {
		return nil, err
	}

	createPath := filepath.Dir(dsn.dbPath)
	if err := os.MkdirAll(createPath, 0755); err != nil {
		return nil, fmt.Errorf("creating path %q: %w", createPath, err)
	}

	db, err := badger.Open(dsnToOptions(dsn))
	if err != nil {
		return nil, fmt.Errorf("badger new: open badger db: %w", err)
	}

	s := &Store{
		dsn: dsnString,
		db:  db,
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Put(_ context.Context, key, value []byte) (err error) {
	log.Debugw("putting", "key", store.Key(key))
	if s.writeBatch == nil {
		s.writeBatch = s.db.NewWriteBatch()
	}

	err = s.writeBatch.SetEntry(badger.NewEntry(key, value))
	if err == badger.ErrTxnTooBig {
		log.Debug("txn too big pre-emptively pushing")
		if err := s.writeBatch.Flush(); err != nil {
			return err
		}

		s.writeBatch = s.db.NewWriteBatch()
		err := s.writeBatch.SetEntry(badger.NewEntry(key, value))
		if err != nil {
			return fmt.Errorf("set entry (after flush): %w", err)
		}
	}

	if err != nil {
		return fmt.Errorf("set entry: %w", err)
	}

	return nil
}

func (s *Store) FlushPuts(_ context.Context) error {
	if s.writeBatch == nil {
		return nil
	}
	err := s.writeBatch.Flush()
	if err != nil {
		return err
	}
	s.writeBatch = s.db.NewWriteBatch()
	return nil
}

func wrapNotFoundError(err error) error {
	if err == badger.ErrKeyNotFound {
		return store.ErrNotFound
	}
	return err
}

func (s *Store) Get(_ context.Context, key []byte) (value []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return wrapNotFoundError(err)
		}

		value, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (s *Store) BatchDelete(_ context.Context, keys [][]byte) (err error) {
	log.Debugw("batch deletion", "key_count", len(keys))

	deletionBatch := s.db.NewWriteBatch()
	for _, key := range keys {
		err = deletionBatch.Delete(key)
		if err == badger.ErrTxnTooBig {
			log.Debug("txn too big pre-emptively pushing")
			if err := deletionBatch.Flush(); err != nil {
				return err
			}

			// We start a new batch and add our key, the err is re-assigned, going to be check after the if
			deletionBatch = s.db.NewWriteBatch()
			err = deletionBatch.Delete(key)
			if err != nil {
				return fmt.Errorf("delete (after flush): %w", err)
			}
		}

		if err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}

	return deletionBatch.Flush()
}

func (s *Store) BatchGet(ctx context.Context, keys [][]byte) *store.Iterator {
	kr := store.NewIterator(ctx)

	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			for _, key := range keys {
				item, err := txn.Get(key)
				if err != nil {
					return wrapNotFoundError(err)
				}

				value, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}

				if !kr.PushItem(store.KV{Key: item.KeyCopy(nil), Value: value}) {
					break
				}

				// TODO: make sure this is conform and takes inspiration from `Scan`.. deals
				// with the `store.Iterator` properly
			}
			return nil
		})
		if err != nil {
			kr.PushError(wrapNotFoundError(err))
			return
		}
		kr.PushFinished()
	}()
	return kr
}

func (s *Store) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int, options ...store.ReadOption) *store.Iterator {
	sit := store.NewIterator(ctx)
	log.Debugw("scanning", "start", store.Key(start), "exclusive_end", store.Key(exclusiveEnd), "limit", store.Limit(limit))
	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			badgerOptions := badgerIteratorOptions(store.Limit(limit), options)
			bit := txn.NewIterator(badgerOptions)
			defer bit.Close()

			var err error
			count := uint64(0)
			for bit.Seek(start); bit.Valid() && bytes.Compare(bit.Item().Key(), exclusiveEnd) == -1; bit.Next() {
				count++

				// We require value only when `PrefetchValues` is true, otherwise, we are performing a key-only iteration and as such,
				// we should not fetch nor decompress actual value
				var value []byte
				if badgerOptions.PrefetchValues {
					value, err = bit.Item().ValueCopy(nil)
					if err != nil {
						return err
					}
				}

				if !sit.PushItem(store.KV{Key: bit.Item().KeyCopy(nil), Value: value}) {
					break
				}

				if store.Limit(limit).Reached(count) {
					break
				}
			}
			return nil
		})
		if err != nil {
			sit.PushError(err)
			return
		}

		sit.PushFinished()
	}()

	return sit
}

func (s *Store) Prefix(ctx context.Context, prefix []byte, limit int, options ...store.ReadOption) *store.Iterator {
	kr := store.NewIterator(ctx)
	log.Debugw("prefix scanning", "prefix", store.Key(prefix), "limit", store.Limit(limit))
	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			badgerOptions := badgerIteratorOptions(store.Limit(limit), options)
			badgerOptions.Prefix = prefix

			it := txn.NewIterator(badgerOptions)
			defer it.Close()

			var err error
			count := uint64(0)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				count++

				// We require value only when `PrefetchValues` is true, otherwise, we are performing a key-only iteration and as such,
				// we should not fetch nor decompress actual value
				var value []byte
				if badgerOptions.PrefetchValues {
					value, err = it.Item().ValueCopy(nil)
					if err != nil {
						return err
					}
				}

				if !kr.PushItem(store.KV{Key: it.Item().KeyCopy(nil), Value: value}) {
					break
				}

				if store.Limit(limit).Reached(count) {
					break
				}
			}
			return nil
		})
		if err != nil {
			kr.PushError(err)
			return
		}

		kr.PushFinished()
	}()

	return kr
}

func badgerIteratorOptions(limit store.Limit, options []store.ReadOption) badger.IteratorOptions {
	if limit.Unbounded() && len(options) == 0 {
		return badger.DefaultIteratorOptions
	}

	readOptions := store.ReadOptions{}
	for _, opt := range options {
		opt.Apply(&readOptions)
	}

	opts := badger.DefaultIteratorOptions
	if readOptions.KeyOnly {
		opts.PrefetchValues = false
	} else if limit.Bounded() && int(limit) < opts.PrefetchSize {
		opts.PrefetchSize = int(limit)
	}

	return opts
}

func (s *Store) Delete(_ context.Context, key []byte) (err error) {
	return s.db.Update(func(txn *badger.Txn) error {
		err = txn.Delete(key)
		return err
	})
}
