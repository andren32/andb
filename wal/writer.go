package wal

import (
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

func (w *WALWriter) AddRecord(record *WALRecord) error {
	keyLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyLength, uint64(len(record.key)))
	valueLength := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueLength, uint64(len(record.value)))
	timestamp := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestamp, uint64(record.timestamp))

	tombstone := byte(0)
	if record.isTombstone {
		tombstone = byte(1)
	}

	_, err := w.writer.Write(keyLength)
	if err != nil {
		return err
	}

	err = w.writer.WriteByte(tombstone)
	if err != nil {
		return err
	}

	_, err = w.writer.Write(valueLength)
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(record.key))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(record.value)
	if err != nil {
		return err
	}

	_, err = w.writer.Write(timestamp)
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
