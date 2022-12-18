package wal

import (
	"andb/core"
	"bufio"
	"encoding/binary"
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
	sequenceNumber := make([]byte, 8)
	binary.LittleEndian.PutUint64(sequenceNumber, uint64(record.sequenceNumber))

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
	recordAsBytes = append(recordAsBytes, sequenceNumber...)

	return recordAsBytes
}

func (w *WALWriter) AddRecord(record *WALRecord) error {
	serializedRecord := serializeRecord(record)

	crc := core.CRCForSlice(serializedRecord)

	recordLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(recordLength, uint64(len(serializedRecord)))

	_, err := w.writer.Write(crc)
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

func (w *WALWriter) Flush() error {
	return w.writer.Flush()
}

func (w *WALWriter) Sync() error {
	return w.file.Sync()
}

func (w *WALWriter) Close() {
	w.Flush()
	w.Sync()
	w.file.Close()
}
