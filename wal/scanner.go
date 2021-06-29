package wal

import (
	"andb/core"
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

type WALScanner struct {
	file   *os.File
	reader *bufio.Reader
}

var (
	ChecksumError = errors.New("Checksum did not match record payload")
)

func NewWALScanner(path string) (*WALScanner, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(f)

	return &WALScanner{f, reader}, nil
}

func deserializeRecord(rawPayload []byte) (*WALRecord, error) {
	keyLength := binary.LittleEndian.Uint32(rawPayload[0:4])
	tombstone := rawPayload[4]
	valueLength := binary.LittleEndian.Uint32(rawPayload[5:9])

	key := rawPayload[9 : 9+keyLength]
	value := rawPayload[9+keyLength : 9+keyLength+valueLength]
	timestamp := binary.LittleEndian.Uint64(rawPayload[9+keyLength+valueLength : 9+keyLength+valueLength+8])

	return &WALRecord{
		key:         core.Key(key),
		value:       value,
		timestamp:   core.Timestamp(timestamp),
		isTombstone: tombstone > 0,
	}, nil
}

func (s *WALScanner) ReadRecord() (*WALRecord, error) {
	rawCRC := make([]byte, 4)
	_, err := io.ReadFull(s.reader, rawCRC)
	if err != nil {
		return nil, err
	}

	rawRecordLength := make([]byte, 8)
	_, err = io.ReadFull(s.reader, rawRecordLength)
	if err != nil {
		return nil, err
	}
	recordLength := binary.LittleEndian.Uint64(rawRecordLength)

	rawPayload := make([]byte, recordLength)
	_, err = io.ReadFull(s.reader, rawPayload)
	if err != nil {
		return nil, err
	}

	crc := binary.LittleEndian.Uint32(rawCRC)
	if crc != crc32.ChecksumIEEE(rawPayload) {
		return nil, ChecksumError
	}

	return deserializeRecord(rawPayload)

}

func (s *WALScanner) HasNext() bool {
	_, err := s.reader.Peek(1)
	if err != nil {
		if err == io.EOF {
			return false
		}
	}
	return true
}

func (s *WALScanner) Close() {
	s.file.Close()
}
