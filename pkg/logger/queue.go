package logger

import (
	"log/slog"
	"sync"
	"sync/atomic"
)

// Log requests are stored in a doubly linked list
// and ordered from oldest(head) to newest(tail).
// New logs are added to tail while maintaining exclusive lock on the queue

type LogReq struct {
	log  LogItem
	next *LogReq
	mu   sync.Mutex
}

type LogItem struct {
	pkg      string
	fn       string
	logLevel slog.Level
	msg      string
}

type LogQueue struct {
	head      *LogReq
	tail      *LogReq
	itemCount atomic.Uint64
	mu        sync.Mutex
}

// Set n as the next pointer
func (lr *LogReq) setNext(n *LogReq) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	lr.next = n
}

// Adds a new log request, r, to the LogQueue
func (q *LogQueue) addItem(r *LogReq) *LogReq {
	if r == nil {
		panic("(logger) Log request cannot be nil.")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// add item to tail
	if q.itemCount.Load() <= 0 {
		// set new item as tail and head
		q.head = r
		q.tail = r

		q.itemCount.Add(1)

		return r
	}

	// Since count is greater than 0, head and tail should not be nil
	if q.tail == nil {
		panic("(logger) No tail in the log queue.")
	}

	if q.head == nil {
		panic("(logger) No head in the log queue.")
	}

	q.tail.setNext(r)
	q.tail = r
	q.itemCount.Add(1)

	return r
}

// Removes and returns the oldest log request (head of linked list)
func (q *LogQueue) getOldest() *LogReq {
	q.mu.Lock()
	defer q.mu.Unlock()

	currCount := q.itemCount.Load()
	if currCount <= 0 {
		return nil
	}

	// Since count is greater than 0, head and tail should not be nil
	if q.head == nil {
		panic("(queue) - getOldest - No head in the log queue")
	}

	if q.tail == nil {
		panic("(queue) - getOldest - No tail in the log queue")
	}

	lr := q.head
	q.head = lr.next

	newCount := currCount - 1
	q.itemCount.Swap(newCount)

	// if no item left, set tail as nil
	if newCount == 0 {
		q.tail = nil
	}

	return lr
}

func newLogQueue() *LogQueue {
	return &LogQueue{}
}
