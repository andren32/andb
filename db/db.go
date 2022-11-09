package db

import (
	"andb/core"
	"andb/memtable"
	"andb/wal"
	"errors"
	"fmt"
	"os"
	"sync"
)

var (
	EmptyDBNameError   = errors.New("Database name can not be empty string")
	NonExistingDBError = errors.New("Database does not exist")
)

type db struct {
	dbName       string
	options      *DBOptions
	mu           sync.Mutex
	memtable     memtable.MemTable
	lastSequence core.SequenceNumber
	fileNumber   uint64
	walWriter    *wal.WALWriter
}

type DBOptions struct {
	CreateIfMissing bool
	AlwaysSync      bool
}

type DB interface {
	Put(key core.Key, data []byte) error
	Get(key core.Key) (data []byte, err error)
	Delete(key core.Key) error
}

func newDB(dbName string, options *DBOptions) (*db, error) {
	if len(dbName) == 0 {
		return nil, EmptyDBNameError
	}
	return &db{dbName: dbName, options: options}, nil
}

func OpenDB(dbName string, options *DBOptions) (DB, error) {
	db, err := newDB(dbName, options)
	if err != nil {
		return nil, err
	}

	err = db.recover()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (d *db) Put(key core.Key, data []byte) error {
	return d.insert(key, data, false)
}

func (d *db) Delete(key core.Key) error {
	return d.insert(key, nil, true)
}

func (d *db) Get(key core.Key) (data []byte, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.memtable.Get(key)
}

func (d *db) insert(key core.Key, data []byte, isTombstone bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	sequenceNumber := d.lastSequence + 1

	err := d.walWriter.AddRecord(wal.NewWALRecord(key, data, sequenceNumber, isTombstone))
	d.walWriter.Flush()

	if d.options.AlwaysSync {
		d.walWriter.Sync()
	}

	if err != nil {
		return err
	}

	if isTombstone {
		d.memtable.Delete(key, sequenceNumber)
	} else {
		d.memtable.Insert(key, sequenceNumber, data)
	}

	d.lastSequence = sequenceNumber

	return nil
}

func (d *db) recover() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, err := os.Stat(d.dbName); os.IsNotExist(err) {
		if !d.options.CreateIfMissing {
			return NonExistingDBError
		}

		if err := os.MkdirAll(d.dbName, 0777); err != nil {
			return err
		}

		filename := WALFileName(d.dbName, d.fileNumber)
		walWriter, err := wal.NewWALWriter(filename)

		if err != nil {
			return err
		}

		d.walWriter = walWriter

		fmt.Println(walWriter)
		// fmt.Println(d.walWriter)

		d.memtable = memtable.NewSkiplistMemtable()

	} else {
		return fmt.Errorf("Handling existing db not implemented")
	}

	return nil
}
