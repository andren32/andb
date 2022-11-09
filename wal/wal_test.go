package wal

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddRecordsAndReadRecords(t *testing.T) {
	testDir, err := ioutil.TempDir("", "wal_test")
	assert.NoError(t, err)
	defer os.RemoveAll(testDir)

	w, err := NewWALWriter(testDir + "file.wal")
	assert.NoError(t, err)

	records := []*WALRecord{
		{key: "A", value: []byte{1, 2}, sequenceNumber: 0, isTombstone: false},
		{key: "B", value: []byte{3, 4, 5, 6}, sequenceNumber: 1, isTombstone: false},
		{key: "C", value: []byte{7, 8, 9, 10, 11}, sequenceNumber: 2, isTombstone: false},
		{key: "B", value: []byte{}, sequenceNumber: 3, isTombstone: true},
	}

	for _, record := range records {
		w.AddRecord(record)
	}

	w.Flush()
	w.Close()

	s, err := NewWALScanner(testDir + "file.wal")

	r1, err := s.ReadRecord()
	assert.NoError(t, err)
	assert.True(t, s.HasNext())
	r2, err := s.ReadRecord()
	assert.NoError(t, err)
	assert.True(t, s.HasNext())
	r3, err := s.ReadRecord()
	assert.NoError(t, err)
	assert.True(t, s.HasNext())
	r4, err := s.ReadRecord()
	assert.NoError(t, err)

	assert.Equal(t, records[0], r1)
	assert.Equal(t, records[1], r2)
	assert.Equal(t, records[2], r3)
	assert.Equal(t, records[3], r4)

	assert.False(t, s.HasNext())
}

func TestChecksumDetectsCorruptData(t *testing.T) {
	testDir, err := ioutil.TempDir("", "wal_test")
	assert.NoError(t, err)
	defer os.RemoveAll(testDir)

	path := testDir + "file.wal"

	w, err := NewWALWriter(path)
	assert.NoError(t, err)

	record := &WALRecord{key: "A", value: []byte{1, 2, 3, 4, 5}, sequenceNumber: 0, isTombstone: false}

	w.AddRecord(record)

	w.Flush()
	w.Close()

	// write a corrupted wal entry
	data, err := os.ReadFile(path)
	assert.NoError(t, err)

	data = bytes.Replace(data, []byte{1, 2, 3, 4, 5}, []byte{1, 1, 3, 4, 5}, 1)

	err = os.WriteFile(path, data, 0666)
	assert.NoError(t, err)

	s, err := NewWALScanner(path)

	r, err := s.ReadRecord()
	assert.Nil(t, r)
	assert.ErrorIs(t, err, ChecksumError)
}
