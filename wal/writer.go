package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"os"
)

type WALWriter struct {
	file   *os.File
	writer *bufio.Writer
}

func NewWALWriter(path string) (*WALWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(f)

	return &WALWriter{f, writer}, nil
}

func serializeRecord(record *WALRecord) []byte {
	keyLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLength, uint32(len(record.key)))
	valueLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(valueLength, uint32(len(record.value)))
	timestamp := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestamp, uint64(record.timestamp))

	tombstone := byte(0)
	if record.isTombstone {
		tombstone = byte(1)
	}

	recordByteSize := len(record.key) + len(record.value) + 25
	recordAsBytes := make([]byte, 0, recordByteSize)

	recordAsBytes = append(recordAsBytes, keyLength...)
	recordAsBytes = append(recordAsBytes, tombstone)
	recordAsBytes = append(recordAsBytes, valueLength...)
	recordAsBytes = append(recordAsBytes, []byte(record.key)...)
	recordAsBytes = append(recordAsBytes, []byte(record.value)...)
	recordAsBytes = append(recordAsBytes, timestamp...)

	return recordAsBytes
}

func (w *WALWriter) AddRecord(record *WALRecord) error {
	serializedRecord := serializeRecord(record)
	crc := crc32.ChecksumIEEE(serializedRecord)

	crcAsBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcAsBytes, crc)

	recordLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(recordLength, uint64(len(serializedRecord)))

	_, err := w.writer.Write(crcAsBytes)
	if err != nil {
		return err
	}

	_, err = w.writer.Write(recordLength)
	if err != nil {
		return err
	}

	_, err = w.writer.Write(serializedRecord)
	if err != nil {
		return err
	}

	return err
}

func (w *WALWriter) Flush() {
	w.writer.Flush()
}

func (w *WALWriter) Close() {
	w.writer.Flush()
	w.file.Close()
}
