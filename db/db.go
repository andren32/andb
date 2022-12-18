package db

import (
	"andb/core"
	"andb/memtable"
	"andb/wal"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"os"
	"sync"
)

var (
	EmptyDBNameError   = errors.New("Database name can not be empty string")
	NonExistingDBError = errors.New("Database does not exist")
)

type db struct {
	dbName    string
	options   *DBOptions
	mu        sync.Mutex
	memtable  memtable.MemTable
	walWriter *wal.WALWriter

	version dbVersion
}

type dbVersion struct {
	lastSequence       core.SequenceNumber
	manifestFileNumber uint64
	walNumber          uint64
}

type DBOptions struct {
	CreateIfMissing bool
	AlwaysSync      bool
}

type DB interface {
	Put(key core.Key, data []byte) error
	Get(key core.Key) (data []byte, err error)
	Delete(key core.Key) error
	Close() error
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

func (d *db) Close() error {
	d.walWriter.Close()
	return nil
}

func (d *db) insert(key core.Key, data []byte, isTombstone bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	sequenceNumber := d.version.lastSequence + 1

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

	d.version.lastSequence = sequenceNumber

	return nil
}

func (d *db) recover() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	os.MkdirAll(d.dbName, 0777)

	if _, err := os.Stat(CurrentFileName(d.dbName)); os.IsNotExist(err) {
		if !d.options.CreateIfMissing {
			return NonExistingDBError
		}

		err = d.bootstrapDB()
		if err != nil {
			return err
		}

		return nil
	}

	version := d.recoverVersion()

}

func (d *db) recoverVersion() dbVersion {
	// Read Current -> Read Manifest -> Get Version and return
	return dbVersion{}
}

func (d *db) bootstrapDB() error {
	version := dbVersion{lastSequence: 0, walNumber: 1, manifestFileNumber: 1}
	manifestFilename := ManifestFileName(d.dbName, 1)

	err := encodeToManifest(manifestFilename, version)
	if err != nil {
		return err
	}

	err = setCurrentFile(d.dbName, 1)
	if err != nil {
		return err
	}

	return nil
}

func (d *db) getDatabaseFilenames() ([]string, error) {
	files, err := ioutil.ReadDir(d.dbName)
	if err != nil {
		return nil, err
	}

	filenames := []string{}

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		filenames = append(filenames, f.Name())
	}

	return filenames, nil
}

func setCurrentFile(dbName string, number uint64) error {
	currentFilename := CurrentFileName(dbName)

	// TODO: fix issue. if we crash here we will have truncated the current file
	// and lost our latest reference to our current manifest
	f, err := os.Create(currentFilename)
	defer f.Close()

	if err != nil {
		return err
	}

	_, err = f.WriteString(ManifestFileNameWithoutDBPath(number))
	if err != nil {
		return err
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	return nil
}

func encodeToManifest(manifestFilename string, version dbVersion) error {
	encodedManifest := make([]byte, 24, 32)
	binary.LittleEndian.PutUint64(encodedManifest, uint64(version.lastSequence))
	binary.LittleEndian.PutUint64(encodedManifest[8:], uint64(version.manifestFileNumber))
	binary.LittleEndian.PutUint64(encodedManifest[16:], uint64(version.walNumber))

	crc := core.CRCForSlice(encodedManifest)
	encodedManifest = append(encodedManifest, crc...)

	f, err := os.Create(manifestFilename)
	defer f.Close()

	if err != nil {
		return err
	}

	_, err = f.Write(encodedManifest)
	if err != nil {
		return err
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	return nil
}
