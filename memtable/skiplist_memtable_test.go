package memtable

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}

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
	memtable.Insert("key", 2, []byte{1, 2, 3})
	memtable.Insert("key", 1, []byte{3, 2, 1})

	got, err := memtable.Get("key")

	assert.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestDataIsOrderedWhenWrittenConcurrently(t *testing.T) {
	memtable := NewSkiplistMemtable()
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(m MemTable, i int, wg *sync.WaitGroup) {
			memtable.Insert(Key(randString(5, 20)), Timestamp(i), []byte(randString(5, 20)))
			wg.Done()
		}(memtable, i, &wg)
	}

	wg.Wait()
	lastNode := memtable.head.next[0]
	cn := memtable.head.next[0].next[0]
	for cn != nil {
		assert.GreaterOrEqual(t, cn.record.key, lastNode.record.key)
		if cn.record.key == lastNode.record.key {
			assert.GreaterOrEqual(t, cn.record.timestamp, lastNode.record.timestamp)
		}

		cn = cn.next[0]
	}
}

func (m *skiplistMemTable) PrintLinkedList() {
	cn := m.head.next[0]
	for cn != nil {
		fmt.Println(cn.record.key, cn.record.timestamp, cn.record.data)
		cn = cn.next[0]
	}
}

func TestGetNonExistingKeyGivesError(t *testing.T) {
	memtable := NewSkiplistMemtable()

	_, err := memtable.Get("key")
	assert.ErrorIs(t, err, KeyNotFound)
}

func TestDeleteKey(t *testing.T) {
	memtable := NewSkiplistMemtable()

	memtable.Insert("key", 0, []byte{1, 2, 3})
	_, err := memtable.Get("key")

	assert.NoError(t, err)

	memtable.Delete("key", 1)

	_, err = memtable.Get("key")
	assert.ErrorIs(t, err, KeyNotFound)
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(minLength int, maxLength int) string {
	length := rand.Intn(maxLength-minLength) + minLength
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
