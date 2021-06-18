package wal

import (
	"andb/core"
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type WALScanner struct {
	file   *os.File
	reader *bufio.Reader
}

func NewWALScanner(path string) (*WALScanner, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(f)

	return &WALScanner{f, reader}, nil
}

func (s *WALScanner) ReadRecord() (*WALRecord, error) {
	rawKeyLength := make([]byte, 8)
	_, err := io.ReadFull(s.reader, rawKeyLength)
	if err != nil {
		return nil, err
	}

	rawTombstone, err := s.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	rawValuelength := make([]byte, 8)
	_, err = io.ReadFull(s.reader, rawValuelength)
	if err != nil {
		return nil, err
	}

	keyLength := binary.LittleEndian.Uint64(rawKeyLength)
	valueLength := binary.LittleEndian.Uint64(rawValuelength)

	key := make([]byte, keyLength)
	value := make([]byte, valueLength)

	_, err = io.ReadFull(s.reader, key)
	if err != nil {
		return nil, err
	}

	_, err = io.ReadFull(s.reader, value)
	if err != nil {
		return nil, err
	}

	rawTimestamp := make([]byte, 8)
	_, err = io.ReadFull(s.reader, rawTimestamp)
	if err != nil {
		return nil, err
	}

	timestamp := binary.LittleEndian.Uint64(rawTimestamp)

	return &WALRecord{
		key:         core.Key(key),
		value:       value,
		timestamp:   core.Timestamp(timestamp),
		isTombstone: rawTombstone > 0,
	}, nil
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
