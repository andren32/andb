package memtable

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

const (
	maxHeight       = 12
	branchingFactor = 4
)

var (
	KeyNotFound = errors.New("Key not found")
)

type node struct {
	record MemTableRecord
	next   []*node
}

type skiplistMemTable struct {
	head    *node
	randGen *rand.Rand
	mu      sync.Mutex
	size    uint64
}

func NewSkiplistMemtable() *skiplistMemTable {
	return &skiplistMemTable{
		head: &node{
			record: MemTableRecord{},
			next:   make([]*node, maxHeight),
		},
		randGen: rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
		size:    0,
	}
}

func (m *skiplistMemTable) Insert(key Key, timestamp Timestamp, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	n := newNode(key, timestamp, data, false)
	m.insertNode(n)

	m.size += m.nodeMemoryUsage(n)
}

func (m *skiplistMemTable) Get(key Key) (data []byte, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	n := m.getLatestNode(key)
	if n == nil {
		return []byte{}, KeyNotFound
	}
	return n.record.data, nil
}

func (m *skiplistMemTable) Delete(key Key, timestamp Timestamp) {
	m.mu.Lock()
	defer m.mu.Unlock()

	n := newNode(key, timestamp, []byte{}, true)
	m.insertNode(n)

	m.size -= (m.nodeMemoryUsage(n) - uint64(MemTableRecordOverhead))
}

func (m *skiplistMemTable) Size() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.size
}

func (m *skiplistMemTable) nodeMemoryUsage(n *node) uint64 {
	return uint64(len(n.record.data)) + uint64(MemTableRecordOverhead)
}

func (m *skiplistMemTable) insertNode(newNode *node) {
	var updateList [maxHeight]*node
	currentNode := m.head

	// find insert points
	for currentLevel := maxHeight - 1; currentLevel >= 0; currentLevel-- {
		for {
			next := currentNode.next[currentLevel]
			if next == nil ||
				!(next.record.key < newNode.record.key ||
					(next.record.key == newNode.record.key &&
						next.record.timestamp < newNode.record.timestamp)) {
				break
			}

			currentNode = next
		}
		updateList[currentLevel] = currentNode
	}

	height := m.getNewHeight()

	for currentLevel := height - 1; currentLevel >= 0; currentLevel-- {
		prevNode := updateList[currentLevel]
		nextNode := prevNode.next[currentLevel]
		prevNode.next[currentLevel] = newNode
		newNode.next[currentLevel] = nextNode
	}
}

func (m *skiplistMemTable) getLatestNode(key Key) *node {
	currentNode := m.head
	for currentLevel := maxHeight - 1; currentLevel >= 0; currentLevel-- {
		for {
			next := currentNode.next[currentLevel]

			if next == nil || next.record.key > key {
				break
			}

			// make sure next is the latest for the searched key
			if next.record.key == key && (next.next[0] == nil || next.next[0].record.key != key) {
				if next.record.isTombstone {
					return nil
				}
				return next
			}

			currentNode = next
		}
	}
	return nil
}

func newNode(key Key, timestamp Timestamp, data []byte, isTombstone bool) *node {
	return &node{
		record: MemTableRecord{
			key:         key,
			timestamp:   timestamp,
			data:        data,
			isTombstone: isTombstone,
		},
		next: make([]*node, maxHeight),
	}
}

func (m *skiplistMemTable) getNewHeight() int {
	height := 1
	for m.randGen.Intn(branchingFactor) == 0 && height < maxHeight {
		height += 1
	}
	return height
}
