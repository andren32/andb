package memtable

import (
	"andb/core"
	"errors"
	"unsafe"
)

type MemTable interface {
	Insert(key core.Key, timestamp core.Timestamp, data []byte)
	Get(key core.Key) (data []byte, err error)
	Delete(key core.Key, timestamp core.Timestamp)
	Size() uint64
}

type MemTableRecord struct {
	data        []byte
	key         core.Key
	timestamp   core.Timestamp
	isTombstone bool
}

const (
	MemTableRecordOverhead = unsafe.Sizeof(MemTableRecord{})
)

var (
	KeyNotFound = errors.New("Key not found")
)
