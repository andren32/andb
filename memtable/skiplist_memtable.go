package memtable

import (
	"andb/core"
	"math/rand"
	"time"
)

const (
	maxHeight       = 12
	branchingFactor = 4
)

type node struct {
	record MemTableRecord
	next   []*node
}

type skiplistMemTable struct {
	head    *node
	randGen *rand.Rand
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

func (m *skiplistMemTable) Insert(key core.Key, sequenceNumber core.SequenceNumber, data []byte) {
	n := newNode(key, sequenceNumber, data, false)
	m.insertNode(n)

	m.size += m.nodeMemoryUsage(n)
}

func (m *skiplistMemTable) Get(key core.Key) (data []byte, err error) {
	n := m.getLatestNode(key)
	if n == nil {
		return []byte{}, KeyNotFound
	}
	return n.record.data, nil
}

func (m *skiplistMemTable) Delete(key core.Key, sequenceNumber core.SequenceNumber) {
	n := newNode(key, sequenceNumber, []byte{}, true)
	m.insertNode(n)

	m.size -= (m.nodeMemoryUsage(n) - uint64(MemTableRecordOverhead))
}

func (m *skiplistMemTable) Size() uint64 {
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
						next.record.sequenceNumber < newNode.record.sequenceNumber)) {
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

func (m *skiplistMemTable) getLatestNode(key core.Key) *node {
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

func newNode(key core.Key, sequenceNumber core.SequenceNumber, data []byte, isTombstone bool) *node {
	return &node{
		record: MemTableRecord{
			key:            key,
			sequenceNumber: sequenceNumber,
			data:           data,
			isTombstone:    isTombstone,
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
