package db

import (
	"andb/core"
	"andb/memtable"
)

type db struct {
	memtable memtable.MemTable
}

type DB interface {
	Put(key core.Key, data []byte) error
	Get(key core.Key) error
	Delete(key core.Key) error
	Close() error
}

func NewDB() DB {
	return &db{memtable: memtable.NewSkiplistMemtable()}
}

func (d *db) Put(key core.Key, data []byte) error {
	return nil
}

func (d *db) Get(key core.Key) error {
	return nil
}

func (d *db) Delete(key core.Key) error {
	return nil
}

func (d *db) Close() error {
	return nil
}
