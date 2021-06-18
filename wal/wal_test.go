package wal

import (
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
		{key: "A", value: []byte{1, 2}, timestamp: 0, isTombstone: false},
		{key: "B", value: []byte{3, 4, 5, 6}, timestamp: 1, isTombstone: false},
		{key: "C", value: []byte{7, 8, 9, 10, 11}, timestamp: 2, isTombstone: false},
		{key: "B", value: []byte{}, timestamp: 3, isTombstone: true},
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
