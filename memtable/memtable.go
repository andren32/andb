package memtable

import (
	"andb/core"
	"errors"
	"unsafe"
)

type MemTable interface {
	Insert(key core.Key, sequenceNumber core.SequenceNumber, data []byte)
	Get(key core.Key) (data []byte, err error)
	Delete(key core.Key, sequenceNumber core.SequenceNumber)
	Size() uint64
}

type MemTableRecord struct {
	data           []byte
	key            core.Key
	sequenceNumber core.SequenceNumber
	isTombstone    bool
}

const (
	MemTableRecordOverhead = unsafe.Sizeof(MemTableRecord{})
)

var (
	KeyNotFound = errors.New("Key not found")
)
