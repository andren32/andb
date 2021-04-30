package memtable

import "unsafe"

type Key string
type Timestamp int64

type MemTable interface {
	Insert(key Key, timestamp Timestamp, data []byte)
	Get(key Key) (data []byte, err error)
	Delete(key Key, timestamp Timestamp)
	Size() uint64
}

type MemTableRecord struct {
	data        []byte
	key         Key
	timestamp   Timestamp
	isTombstone bool
}

const (
	MemTableRecordOverhead = unsafe.Sizeof(MemTableRecord{})
)
