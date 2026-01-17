package buffermanager

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const (
	Window LruType = iota
	Probation
	Protected
)

type LruType int

type ILRU interface {
	Add(f *Frame) error
	getHead() *Frame
	getTail() *Frame
	getCount() uint64
	getCapacity() uint64
	getPinnedFrameCount() uint64
	Pop() *Frame
	deductCount() bool
	ReAddFrame(f *Frame) error
	RemoveFrame(f *Frame) error
	Delete(f *Frame) error
	borrow()
	returnBorrowedFrame() error
	decrementBorrowedFrames() bool
	hasBorrowedFrames() bool
	SetMostRecent(f *Frame) error
	// incrementPinCount()
	decrementPinCount() bool
	lruIsFull() bool
}

// A doubly linked list with Least Recently Used(LRU) eviction policy.
// The LRU maintains the order of items according to access time
// and tracks pinned items to calculate the number of items available
// in the linked list.
// available items = total count - pinned items
type LRU struct {
	head  *Frame
	tail  *Frame
	count atomic.Uint64

	// capacity does not change after initialization
	capacity       atomic.Uint64
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
	f.mu.Lock()
	defer f.mu.Unlock()
	fmt.Printf("lru.add %s Adding frame -> %v\n", l.segName, f)

	// state check
	if (l.head == nil && l.tail != nil) || (l.head != nil && l.tail == nil) {
		panic(fmt.Errorf("(Add) Invalid state:\nHead -> %v\nTail -> %v\n", l.head, l.tail))
	}

	// update LRUType
	switch l.segName {
	case "probation":
		f.CacheType = Probation
	case "protected":
		f.CacheType = Protected
	default:
		f.CacheType = Window
	}

	// Add item to head and update head pointers
	if l.head != nil {
		l.head.setPrevPtr(f)
		f.next = l.head
		l.head = f

		l.count.Add(1)

		fmt.Printf("lru.add %s tail after adding -> %v\n", l.segName, l.tail)
		fmt.Printf("lru.add %s head after adding -> %v\n", l.segName, l.head)

		l.checkState()

		return nil
	}

	// head is nil, set new item as both head and tail
	l.head = f
	l.tail = f

	l.count.Add(1)

	availableCount := l.getCount() - l.getPinnedFrameCount()
	if l.tail != nil && l.tail.prev == nil && availableCount > 0 && availableCount-1 > 1 {
		panic(fmt.Errorf("New tail prev is nil -> %v", l.tail))
	}

	fmt.Printf("lru.add %s tail after adding -> %v --> %d\n", l.segName, l.tail, f.CacheType)
	fmt.Printf("lru.add %s head after adding -> %v\n", l.segName, l.head)

	l.checkState()
	return nil
}

// Removes and returns tail item from LRU
func (l *LRU) Pop() *Frame {
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Printf("(lru.pop()) (%s) TAIL -> %v\n", l.segName, l.tail)
	log.Printf("(lru.pop()) (%s) HEAD -> %v\n", l.segName, l.head)
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
		log.Printf("(%s) available frames --> %d\n", l.segName, available)
		if t.prev == nil {
			// If there's more than 1 available frame, there should
			// be a previous pointer
			panic(fmt.Errorf("No previous pointer for tail item -> %v", t))
		}

		// set tail to prev frame
		l.tail = t.prev

		t.prev.setNextPtr(nil)
	}

	// zero out pointers
	t.setPtrs(nil, nil)

	// decrement count
	deducted := l.deductCount()

	fmt.Println("deducted: ", deducted)

	if l.tail != nil && l.tail.prev == nil && available > 0 && available-1 > 1 {
		panic(fmt.Errorf("New tail prev is nil -> %v", l.tail))
	}

	l.checkState()

	return t
}

func (l *LRU) checkState() {
	totFrames := l.count.Load()
	pinned := l.pinnedFrames.Load()
	availableCount := totFrames - pinned

	if availableCount == 1 && l.tail != l.head {
		panic(fmt.Errorf("(%s) Invalid state, count is 1 but head is not equal to tail\nhead -> %v\ntail -> %v\ntotFrames -> %d\npinnedFrames -> %d\navailable -> %d\n", l.segName, l.head, l.tail, totFrames, pinned, availableCount))
	}
}

// ReAdds a frame to lru.
func (l *LRU) ReAddFrame(f *Frame) error {
	if f == nil {
		return LRUError{Message: "Frame is nil"}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	// Frame should have nil pointers
	if f.prev != nil || f.next != nil {
		panic(fmt.Errorf("Frame to readd already has pointers -> %v\n", f))
	}

	reAdd, err := f.UnpinFrame()

	if err != nil {
		return err
	}

	if !reAdd {
		// pin count > 0, do not readd
		l.mu.Lock()
		l.checkState()
		l.mu.Unlock()
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	pinCount := l.pinnedFrames.Load()
	availableCount := l.count.Load() - pinCount
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
		f.next = l.head
		l.head = f
	}

	// decrement pin count
	decremented := l.decrementPinCount()
	log.Println("decremented pin count: ", decremented)
	log.Println("prev pin count -> ", pinCount)
	log.Println("curr pin count -> ", l.pinnedFrames.Load())
	fPinC := f.GetPinCount()
	log.Println("frame pin count -> ", fPinC)

	// log.Println("Unpinned frame: ", unpinned)
	if l.tail != nil && l.tail.prev == nil && availableCount > 0 && availableCount-1 > 1 {
		panic(fmt.Errorf("(%s) New tail prev is nil -> %v\navailableCount -> %d\npinnedFrameCount -> %d\navailableCount -> %d\n", l.segName, l.tail, l.getCount(), l.getPinnedFrameCount(), availableCount))
	}

	l.checkState()

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
	f.mu.Lock()
	defer f.mu.Unlock()
	pinCount := f.GetPinCount()

	if pinCount > 0 {
		if f.prev != nil || f.next != nil {
			panic(fmt.Errorf("pinned(%d) frame has prev and next pointers.\nframe -> %v\nprev -> %v\nnext-> %v\n", pinCount, f, f.prev, f.next))
		}
		fmt.Printf("(RemoveFrame) pinCount -> %d\n", pinCount)

		f.pinFrame()

		l.checkState()
		return nil
	}

	frameCount := l.count.Load()
	pinnedCount := l.pinnedFrames.Load()
	availableCount := frameCount - pinnedCount

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
			panic(fmt.Errorf("(%s) Invalid state: only one item available but current frame is not head or tail:\nframe -> %v\nhead -> %v\ntail -> %v\nCount -> %d\nTotal Frames -> %d\npinned Frames -> %d\n", l.segName, f, l.head, l.tail, availableCount, frameCount, pinnedCount))
		}

		// current frame is both head and tail, set both to nil
		l.head = nil
		l.tail = nil
	default:
		// available count > 1
		log.Println("available count --> ", availableCount)
		log.Println("count -> ", frameCount)
		log.Println("pinnedCount -> ", pinnedCount)
		// update adjascent frames
		if f.prev != nil {
			log.Printf("curr frame %v\nprev -> %v\next -> %v\n", f, f.prev, f.next)
			log.Println("FRAME PIN COUNT -> ", f.GetPinCount())
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
	// f.mu.Lock()
	err := f.pinFrame()
	// f.mu.Unlock()

	if err != nil {
		return err
	}

	// increment pinned frame count
	l.pinnedFrames.Add(1)

	if l.tail != nil && l.tail.prev == nil && availableCount > 0 && availableCount-1 > 1 {
		panic(fmt.Errorf("New tail prev is nil -> %v", l.tail))
	}

	fmt.Printf("(RemoveFrame) (%s) NewTail after pinning -> %v\n", l.segName, l.tail)
	fmt.Printf("(RemoveFrame) %s Frame after removing -> %v\n", l.segName, f)

	l.checkState()

	return nil
}

// Remove a frame form the LRU
func (l *LRU) Delete(f *Frame) error {
	if f == nil {
		return LRUError{Message: "No frame provided"}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Printf("(%s) lru.Delete -> %d\n", l.segName, f.Key)

	availableFrames := l.count.Load() - l.pinnedFrames.Load()

	// if frame has no pointers and is not head or tail, consider it invalid
	if f.prev == nil && f.next == nil && l.head != f && l.tail != f {
		// frame already removed
		return nil
	}

	// frame has pins
	if f.GetPinCount() > 0 {
		return LRUError{Message: "Frame is currently referenced."}
	}

	// availableFrames := l.count.Load() - l.pinnedFrames.Load()

	if availableFrames <= 0 {
		return LRUError{Message: "No frames available for deletion. avaialble frames <= 0"}
	}

	if availableFrames == 1 {
		l.head = nil
		l.tail = nil
	} else if availableFrames > 1 {
		if l.head == f {
			// update head
			l.head = f.next
		}

		if l.tail == f {
			// update tail
			l.tail = f.prev
		}

		if f.next != nil {
			// update next frame prev pointer
			f.next.setPrevPtr(f.prev)
		}

		if f.prev != nil {
			// update prev frame next pointer
			f.prev.setNextPtr(f.next)
		}
	}

	f.next = nil
	f.prev = nil

	// reduce count
	reduced := l.deductCount()

	log.Println("Reduced count -> ", reduced)

	if l.tail != nil && l.tail.prev == nil && availableFrames > 0 && availableFrames-1 > 1 {
		panic(fmt.Errorf("New tail prev is nil -> %v", l.tail))
	}

	l.checkState()

	return nil
}

// sends the frame to head of the lru
func (l *LRU) SetMostRecent(f *Frame) error {
	if f == nil {
		return LRUError{Message: "No frame provided"}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// if frame is currently referenced, return. Frame will be added to head
	// during unpinning
	if f.GetPinCount() > 0 {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// if frame has no pointers and is not head or tail, consider it invalid
	// TODO: This could be an issue if frame is currently pinned. Add check
	// for ref/pin count
	if f.prev == nil && f.next == nil && l.head != f && l.tail != f {
		return LRUError{Message: fmt.Sprintf("(lru.setmostrecent) Invalid frame. Nil pointers while not being head or tail -> %v\n", f)}
	}

	// frame has pins(already removed from LRU)
	if f.GetPinCount() > 0 {
		return LRUError{Message: "Frame is already pinned."}
	}

	if l.head == f {
		// frame is already head of lru
		return nil
	}

	availableFrames := l.count.Load() - l.pinnedFrames.Load()

	if availableFrames <= 0 {
		return LRUError{Message: "No frames available for deletion. avaialble frames <= 0"}
	}

	if availableFrames == 1 {
		// frame already head of lru
		return nil
	} else {
		// available frames > 1
		if f.next != nil {
			f.next.setPrevPtr(f.prev)
		}

		if f.prev != nil {
			f.prev.setNextPtr(f.next)
		}

		if l.tail == f {
			// update tail
			l.tail = f.prev
		}

		// add to front of lru
		l.head.setPrevPtr(f)
		f.next = l.head
		l.head = f

		if l.tail != nil && l.tail.prev == nil && availableFrames > 0 && availableFrames-1 > 1 {
			panic(fmt.Errorf("New tail prev is nil -> %v", l.tail))
		}

		l.checkState()
		return nil
	}
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

func (l *LRU) getTail() *Frame {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.tail
}

// returns the number of items in the LRU
func (l *LRU) getCount() uint64 {
	c := l.count.Load()

	return c
}

// Returns that capacity of the lru
func (l *LRU) getCapacity() uint64 {
	c := l.capacity.Load()

	return c
}

func (l *LRU) getPinnedFrameCount() uint64 {
	pinned := l.pinnedFrames.Load()

	return pinned
}

// Increments the number of borrowed frames
func (l *LRU) borrow() {
	l.borrowedFrames.Add(1)
}

func (l *LRU) lruIsFull() bool {
	count := l.getCount()
	capacity := l.getCapacity()

	return count >= capacity
}

func (l *LRU) hasBorrowedFrames() bool {
	borrowed := l.borrowedFrames.Load()

	return borrowed > 0
}

// Decrements number of borrowed frames
func (l *LRU) decrementBorrowedFrames() bool {
	for {
		c := l.borrowedFrames.Load()

		if c <= 0 {
			return false
		}

		if l.borrowedFrames.CompareAndSwap(c, c-1) {
			return true
		}
	}
}

// checks if there are borrowed frames and decrements the count
func (l *LRU) returnBorrowedFrame() error {
	borrowedF := l.borrowedFrames.Load()

	if borrowedF <= 0 {
		return LRUError{Message: "No borrowed frames"}
	}

	l.decrementBorrowedFrames()

	return nil
}

// performs a CAS operation to reduct count by 1
func (l *LRU) deductCount() bool {
	for {
		c := l.count.Load()

		if c <= 0 {
			return false
		}

		if l.count.CompareAndSwap(c, c-1) {
			fmt.Printf("(deductCount) prevCount -> %d  Count after -> %d\n", c, l.count.Load())
			return true
		}
	}
}

func NewLru(capacity uint64, segName string) *LRU {
	lru := &LRU{
		head:    nil,
		tail:    nil,
		segName: segName,
	}

	lru.capacity.Store(capacity)

	return lru
}
