package buffermanager

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type ILRU interface {
	Add(f *Frame) error
	getHead() *Frame
	getCount() uint64
	getPinnedFrameCount() uint64
	Pop() *Frame
	deductCount() bool
	ReAddFrame(f *Frame) error
	RemoveFrame(f *Frame) error
	// Delete(f *Frame) error
	// borrow()
	// returnBorrowedFrames()
	// SetMostRecent(f *Frame) error
	// incrementPinCount()
	decrementPinCount() bool
}

// A doubly linked list with Least Recently Used(LRU) eviction policy.
// The LRU maintains the order of items according to access time
// and tracks pinned items to calculate the number of items available
// in the linked list.
// available items = total count - pinned items
type LRU struct {
	head           *Frame
	tail           *Frame
	count          atomic.Uint64
	capacity       uint64
	pinnedFrames   atomic.Uint64
	borrowedFrames atomic.Uint64
	isFull         atomic.Bool
	segName        string
	mu             sync.RWMutex
}

// Adds an item to head of the lru
func (l *LRU) Add(f *Frame) error {
	if f == nil {
		return LRUError{Message: "Frame is nil"}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// state check
	if (l.head == nil && l.tail != nil) || (l.head != nil && l.tail == nil) {
		panic(fmt.Errorf("(Add) Invalid state:\nHead -> %v\nTail -> %v\n", l.head, l.tail))
	}

	// Add item to head and update head pointers
	if l.head != nil {
		l.head.setPrevPtr(f)
		f.setNextPtr(l.head)
		l.head = f

		l.count.Add(1)

		return nil
	}

	// head is nil, set new item as both head and tail
	l.head = f
	l.tail = f

	l.count.Add(1)

	return nil
}

// Removes and returns tail item from LRU
func (l *LRU) Pop() *Frame {
	l.mu.Lock()
	defer l.mu.Unlock()
	// state check
	if (l.head == nil && l.tail != nil) || (l.head != nil && l.tail == nil) {
		panic(fmt.Errorf("(Add) Invalid state:\nHead -> %v\nTail -> %v\n", l.head, l.tail))
	}

	if l.tail == nil {
		// no tail
		return nil
	}

	t := l.tail
	// check available items
	available := l.count.Load() - l.pinnedFrames.Load()

	if available <= 0 {
		panic("0 or less available frames")
	} else if available == 1 {
		// only one frame is in the lru. Set both head and tail to nil
		l.head = nil
		l.tail = nil
	} else {
		if t.prev == nil {
			// If there's more than 1 available frame, there should
			// be a previous pointer
			panic("No previous pointer for tail item")
		}

		// set tail to prev frame
		l.tail = t.prev

		t.prev.setNextPtr(nil)
	}

	// decrement count
	deducted := l.deductCount()

	fmt.Println("deducted: ", deducted)

	return t
}

func (l *LRU) ReAddFrame(f *Frame) error {
	if f == nil {
		return LRUError{Message: "Frame is nil"}
	}

	f.mu.Lock()
	// Frame should have nil pointers
	if f.prev != nil || f.next != nil {
		return LRUError{Message: "Frame to readd already has pointers"}
	}
	f.mu.Unlock()

	l.mu.Lock()
	defer l.mu.Unlock()

	availableCount := l.count.Load() - l.pinnedFrames.Load()

	// if there are frames available(unpinned), head and tail shouldn't be nil
	if availableCount > 0 && (l.head == nil || l.tail == nil) {
		panic(fmt.Errorf("Invalid state:\navailable count: %d\nhead -> %v\ntail -> %v\n", availableCount, l.head, l.tail))
	}

	// both head and tail have to have a value or be nil. One cannot be nil
	if (l.head == nil && l.tail != nil) || (l.head != nil && l.tail == nil) {
		panic(fmt.Errorf("Invalid lru state:\nhead -> %v\ntail -> %v\n", l.head, l.tail))
	}

	if availableCount <= 0 {
		// set frame as head and tail
		l.head = f
		l.tail = f
	} else {
		l.head.setPrevPtr(f)
		f.setNextPtr(l.head)
		l.head = f
	}

	// decrement pin count
	decremented := l.decrementPinCount()
	log.Println("decremented pin count: ", decremented)

	// decrement frame pin count
	unpinned, err := f.UnpinFrame()

	if err != nil {
		return err
	}

	log.Println("Unpinned frame: ", unpinned)

	return nil
}

// removes and pins a frame from the lru
func (l *LRU) RemoveFrame(f *Frame) error {
	if f == nil {
		return LRUError{Message: "No frame provided"}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// check frame pin count. If pin count > 0 it has aready
	// been pinned, increment frame pin count and return
	pinCount := f.GetPinCount()

	if pinCount > 0 {
		f.pinFrame()
		return nil
	}

	availableCount := l.count.Load() - l.pinnedFrames.Load()

	// if there are frames available(unpinned), head and tail shouldn't be nil
	if availableCount > 0 && (l.head == nil || l.tail == nil) {
		panic(fmt.Errorf("Invalid state:\navailable count: %d\nhead -> %v\ntail -> %v\n", availableCount, l.head, l.tail))
	}

	// both head and tail have to have a value or be nil. One cannot be nil
	if (l.head == nil && l.tail != nil) || (l.head != nil && l.tail == nil) {
		panic(fmt.Errorf("Invalid lru state:\nhead -> %v\ntail -> %v\n", l.head, l.tail))
	}

	switch availableCount {
	case 0:
		return LRUError{Message: "No frame available for removal"}
	case 1:
		if f != l.head || f != l.tail {
			panic(fmt.Errorf("Invalid state: only one item available but current frame is not head and tail:\nframe -> %v\nhead -> %v\ntail -> %v\n", f, l.head, l.tail))
		}

		// current frame is both head and tail, set both to nil
		l.head = nil
		l.tail = nil
	default:
		// available count > 1
		// update adjascent frames
		if f.prev != nil {
			f.prev.setNextPtr(f.next)
		}

		if f.next != nil {
			f.next.setPrevPtr(f.prev)
		}
	}

	// if frame is head or tail, update head and tail ptrs
	if f == l.head {
		l.head = f.next
	}

	if f == l.tail {
		l.tail = f.prev
	}

	// increment frame's pin count
	f.mu.Lock()
	err := f.pinFrame()
	f.mu.Unlock()

	if err != nil {
		return err
	}

	// increment pinned frame count
	l.pinnedFrames.Add(1)

	return nil
}

func (l *LRU) decrementPinCount() bool {
	for {
		c := l.pinnedFrames.Load()

		if c <= 0 {
			return false
		}

		if l.pinnedFrames.CompareAndSwap(c, c-1) {
			return true
		}
	}
}

func (l *LRU) getHead() *Frame {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.head
}

func (l *LRU) getCount() uint64 {
	c := l.count.Load()

	return c
}

func (l *LRU) getPinnedFrameCount() uint64 {
	pinned := l.pinnedFrames.Load()

	return pinned
}

// performs a CAS operation to reduct count by 1
func (l *LRU) deductCount() bool {
	for {
		c := l.count.Load()

		if c <= 0 {
			return false
		}

		if l.count.CompareAndSwap(c, c-1) {
			return true
		}
	}
}

func NewLru(capacity uint64, segName string) *LRU {
	return &LRU{
		head:     nil,
		tail:     nil,
		capacity: capacity,
		segName:  segName,
	}
}
