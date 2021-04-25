package memtable

import (
	"errors"
	"fmt"
	"math/rand"
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

type memTable struct {
	head    *node
	randGen *rand.Rand
	size    uint64
}

func NewSkiplistMemtable() MemTable {
	return &memTable{
		head: &node{
			record: MemTableRecord{},
			next:   make([]*node, maxHeight),
		},
		randGen: rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
		size:    0,
	}
}

func (m *memTable) PrintLinkedList() {
	cn := m.head
	for cn != nil {
		fmt.Println(cn.record.key)
		cn = cn.next[0]
	}
}

func (m *memTable) Insert(key Key, timestamp Timestamp, data []byte) {
	var updateList [maxHeight]*node
	currentNode := m.head

	for currentLevel := maxHeight - 1; currentLevel >= 0; currentLevel-- {
		for {
			next := currentNode.next[currentLevel]
			if next == nil || !(next.record.key < key || (next.record.key == key && next.record.timestamp < timestamp)) {
				break
			}

			currentNode = next
		}
		updateList[currentLevel] = currentNode
	}

	height := m.getNewHeight()

	newNode := &node{
		record: MemTableRecord{key: key, data: data, isTombstone: false},
		next:   make([]*node, maxHeight),
	}

	for currentLevel := height - 1; currentLevel >= 0; currentLevel-- {
		prevNode := updateList[currentLevel]
		nextNode := prevNode.next[currentLevel]
		prevNode.next[currentLevel] = newNode
		newNode.next[currentLevel] = nextNode
	}

	m.size += uint64(len(data)) + uint64(MemTableRecordOverhead)
}

func (m *memTable) Get(key Key) (data []byte, err error) {
	currentNode := m.head
	for currentLevel := maxHeight - 1; currentLevel >= 0; currentLevel-- {
		for {
			next := currentNode.next[currentLevel]

			if next == nil || next.record.key > key {
				break
			}

			// make sure next is the latest for the searched key
			if next.record.key == key && (next.next[0] == nil || next.next[0].record.key != key) {
				return next.record.data, nil
			}

			currentNode = next
		}
	}
	return []byte{}, KeyNotFound
}

func (m *memTable) Delete(key Key) {

}

func (m *memTable) Size() uint64 {
	return m.size
}

func (m *memTable) getNewHeight() int {
	height := 1
	for m.randGen.Intn(branchingFactor) == 0 && height < maxHeight {
		height += 1
	}
	return height
}
