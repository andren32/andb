package wal

import (
	"andb/core"
)

// WAL Record
// CRC 4 bytes | Payload Length 8 bytes | Payload

// Payload
// Keylength 4 bytes | tombstone 1 byte | valuelength 4 bytes | key | value | sequenceNumber 8 bytes

type WALRecord struct {
	value          []byte
	key            core.Key
	sequenceNumber core.SequenceNumber
	isTombstone    bool
}

func NewWALRecord(key core.Key, value []byte, sequenceNumber core.SequenceNumber, isTombstone bool) *WALRecord {
	return &WALRecord{
		key: key, value: value, sequenceNumber: sequenceNumber, isTombstone: isTombstone,
	}
}
