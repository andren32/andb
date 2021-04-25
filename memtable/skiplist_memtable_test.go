package memtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddAndGetData(t *testing.T) {
	keys := []Key{"key1", "key2", "key3"}
	expected := [][]byte{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}

	memtable := NewSkiplistMemtable()
	for i, key := range keys {
		memtable.Insert(key, 0, expected[i])
	}

	for i, key := range keys {
		got, err := memtable.Get(key)
		assert.NoError(t, err)

		assert.Equal(t, expected[i], got)
	}
}

func TestGetsTheLatestDataForSameKey(t *testing.T) {
	memtable := NewSkiplistMemtable()

	expected := []byte{1, 2, 3}

	memtable.Insert("key", 0, []byte{3, 2, 1})
	memtable.Insert("key", 1, []byte{1, 2, 3})

	got, err := memtable.Get("key")

	assert.NoError(t, err)
	assert.Equal(t, expected, got)
}
