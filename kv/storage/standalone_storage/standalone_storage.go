package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	config *config.Config
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
		return nil
	}

	return &StandAloneStorage{
		config: conf,
		db:     db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.db.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneReader{
		inner:     s,
		iterCount: 0,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			err := s.db.Update(func(txn *badger.Txn) error {
				put := m.Data.(storage.Put)
				err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
				return err
			})
			if err != nil {
				return err
			}
		case storage.Delete:
			err := s.db.Update(func(txn *badger.Txn) error {
				del := m.Data.(storage.Delete)
				err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key))
				return err
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type StandAloneReader struct {
	inner     *StandAloneStorage
	iterCount int
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	var val []byte
	err := r.inner.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(engine_util.KeyWithCF(cf, key))
		if err != nil {
			return err
		}
		val, err = item.Value()
		if err != nil {
			return err
		}
		txn.Discard()

		return nil
	})

	if err != nil {
		//return nil, err
		return nil, nil
	}

	return val, nil
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.inner.db.NewTransaction(true))
}

func (r *StandAloneReader) Close() {
	r.inner.db.Close()
}
