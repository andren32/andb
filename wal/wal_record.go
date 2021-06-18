package wal

import "andb/core"

// WAL Record
// Keylength 8 bytes | tombstone 1 byte | valuelength 8 bytes | key | value | timestamp 8 bytes

type WALRecord struct {
	value       []byte
	key         core.Key
	timestamp   core.Timestamp
	isTombstone bool
}

func NewWALRecord(key core.Key, value []byte, timestamp core.Timestamp, isTombstone bool) *WALRecord {
	return &WALRecord{
		key: key, value: value, timestamp: timestamp, isTombstone: isTombstone,
	}
}
