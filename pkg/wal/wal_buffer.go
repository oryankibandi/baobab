package wal

import (
	"sync"

	"github.com/oryankibandi/baobab/pkg/logger"
)

// WAL BUffer is the in memory storage of WAL B-LOGS. It is in the
// form of a linked list
type WALBuffer struct {
	head   *node // head of the buffer
	tail   *node // tail of the buffer
	mu     sync.Mutex
	logger *logger.BaobabLogger
}

type node struct {
	v    nodeValue
	next *node
}

// Add a Log or Checkpoint to the end of the buffer linked list
func (walBuf *WALBuffer) Add(b nodeValue) {
	walBuf.mu.Lock()
	defer walBuf.mu.Unlock()

	n := node{
		v: b,
	}

	if walBuf.head == nil && walBuf.tail == nil {
		// Buffer is empty.
		walBuf.head = &n
		walBuf.tail = &n

		return
	}

	walBuf.tail.next = &n
	walBuf.tail = &n

	return
}

// Creates a copy of current wal, traverses linked list and
// constructs and returns data to be written
func (walBuf *WALBuffer) flushWal() ([]byte, error) {
	var head *node

	walBuf.mu.Lock()
	if walBuf.head == nil {
		// no items in buffer, return
		walBuf.mu.Unlock()
		return nil, nil
	}

	head = walBuf.head

	// Reset WAL and release lock
	walBuf.head = nil
	walBuf.tail = nil
	walBuf.mu.Unlock()

	// construct WAL data
	data := make([]byte, 0)
	var nodeByte []byte
	nextNode := head

	for nextNode != nil {
		nodeByte = nextNode.v.toBytes()
		data = append(data, nodeByte...)
		nextNode = nextNode.next
	}

	return data, nil
}

func NewWalBuff(l *logger.BaobabLogger) *WALBuffer {
	wBuff := WALBuffer{
		logger: l,
	}

	return &wBuff
}
