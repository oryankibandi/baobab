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

// Lru represents the doubly-linked list(DLL) structure that maintains items with
// a Least Recently Used(LRU) eviction policy. Recently accessed items
// are moved to head of the DLL so that least recentyl used items are
// always at the tail
type Lru struct {
	Head     *Frame
	Tail     *Frame
	Count    uint64
	Capacity uint64

	// number of borrowed frames
	borrowedFrames atomic.Uint64

	// Keep track of current items in LRU. If some of the frames have
	// been pinned, they have been removed from the Doubly Linked List
	// but still part of the LRU segment. To get the # of items in the
	// LRU that are not pinned -> Count - pinnedCount
	// pinnedCount is never less than Count
	pinnedCount atomic.Uint64
	Full        bool
	segName     string // name of the segment.Window, probation or protected
	mu          sync.RWMutex
}

// Remove least recently used
func (l *Lru) Pop() *Frame {
	fmt.Println("lru.Pop()")
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Count <= 0 {
		fmt.Println("(pop) list is empty")
		return nil
	}

	pCount := l.pinnedCount.Load()
	if (l.Count-pCount) == 1 || (l.Tail != nil && l.Head != nil && l.Tail == l.Head) {
		// curr item is set as head and tail.
		// Pop() tail and empty list
		fmt.Printf("(lru.Pop() TAIL == HEAD -> %t\nCURR COUNT -> %d\nCURR PINS -> %d\n", l.Tail == l.Head, l.Count, l.pinnedCount.Load())
		fmt.Printf("(lru.Pop()) TAIL -> %v\n(lru.Pop()) HEAD -> %v\n", l.Tail, l.Head)
		t := l.Tail

		l.Count--
		l.Head = nil
		l.Tail = nil

		// assert pin count
		if l.Count < pCount {
			panic(fmt.Errorf("Frame Count(%d) is lower than pinned frames count(%d)", l.Count, l.pinnedCount.Load()))
		}

		// reset pointers
		t.setPtrs(nil, nil)

		l.Full = l.Count >= l.Capacity

		return t
	}

	if l.Tail == nil {
		fmt.Println("(pop) No tail")
		fmt.Printf("(pop) COUNT: %d\t CAPACITY: %d\t PINNED: %d\nHEAD: %v\nTAIL: %v\n", l.Count, l.Capacity, l.pinnedCount.Load(), l.Head, l.Tail)
		return nil
	}

	oldTail := l.Tail

	oldTail.mu.RLock()

	l.Tail = oldTail.prev

	if oldTail.prev != nil {
		oldTail.prev.mu.Lock()
		oldTail.prev.next = nil
		oldTail.prev.mu.Unlock()
	}

	l.Count -= 1

	if l.Count < l.pinnedCount.Load() {
		panic(fmt.Errorf("(%s) Frame Count(%d) is lower than pinned frames count(%d)\nPOPED FRAME PIN COUNT -> %d\nBorrowed frames: %d", l.segName, l.Count, l.pinnedCount.Load(), oldTail.pins.Load(), l.borrowedFrames.Load()))
	}

	l.Full = l.Count >= l.Capacity

	oldTail.mu.RUnlock()

	// reset prev and next pointers
	oldTail.setPtrs(nil, nil)

	return oldTail
}

// Add item to the head of the doubly linked list
func (l *Lru) Add(f *Frame) *Frame {
	fmt.Println("lru.Add()")
	if f == nil {
		panic("(Add) No Frame provided")
	}

	log.Println("ADDINT ITEM TO CACHE^^^^^^^>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	log.Println("ADDINT ITEM TO CACHE^^^^^^^>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	log.Println("ADDINT ITEM TO CACHE^^^^^^^>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	log.Printf("(lru.Add()) (%s) ITEM >> %v", l.segName, f)
	l.mu.Lock()
	log.Println("OBTAINED LRU LOCK >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	//  f.mu.Lock()
	// log.Println("OBTAINED FRAME LOCK >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	defer l.mu.Unlock()
	// defer f.mu.Unlock()
	log.Println("(lru.Add) ITEM >> ", f)
	log.Println("(lru.Add) ITEM COUNT B4:>>>>>>>>> ", l.Count)

	if f.prev != nil {
		panic(fmt.Errorf("(%s) frame has a prev pointer -> %v", l.segName, f.prev))
	}

	if f.next != nil {
		panic(fmt.Errorf("(%s) frame has a next pointer -> %v", l.segName, f.next))
	}

	if l.Head == f {
		panic(fmt.Errorf("%s (Add) Frame is already head of lru list", l.segName))
	}

	if l.Tail == f {
		log.Println("TAIL => ", l.Tail)
		log.Println("FRAME => ", f)
		log.Printf("%s Count: %d\n", l.segName, l.Count)
		log.Printf("%s Capacity: %d\n", l.segName, l.Capacity)
		log.Printf("%s Full: %t\n", l.segName, l.Full)
		panic(fmt.Errorf("%s (Add) Frame is already tail of lru list", l.segName))
	}

	// update lru type
	switch l.segName {
	case "window":
		f.UpdateCacheType(Window)
	case "protected":
		f.UpdateCacheType(Protected)
	case "probation":
		f.UpdateCacheType(Probation)
	default:
		// invalid segName
		panic(fmt.Errorf("(lru) Invalid segName: %s", l.segName))
	}

	log.Println("(lru.Add) Added item to lru")

	if l.Count <= 0 {
		// list is empty
		l.Head = f
		l.Tail = f
		// f.setNextPtr(nil)
		// f.setPrevPtr(nil)
		f.setPtrs(nil, nil)
		l.Count += 1

		l.Full = l.Count >= l.Capacity

		log.Println("FINAL COUNT AFTER ADD ==> ", l.Count)
		return f
	}

	// Add item before head, and make it the new head
	if l.Head != nil {
		log.Println("Head is not nil, updating new head...")
		// l.Head.mu.Lock()
		l.Head.setPrevPtr(f)
		// l.Head.prev = f
		f.setNextPtr(l.Head)
		// l.Head.mu.Unlock()
		log.Println("Set new head...")
	}

	// if only one item existed in the DLL, ensure the old head is set as the tail
	if l.Count == 1 {
		l.Tail = l.Head
	}

	// set new head
	l.Head = f

	// If there are no unpinned frames available, set frame as tail
	if l.Count-l.pinnedCount.Load() == 0 {
		l.Tail = f
	}

	l.Count += 1
	l.Full = l.Count >= l.Capacity

	log.Println("FINAL COUNT AFTER ADD ==> ", l.Count)

	if l.Tail == nil && l.Count-l.pinnedCount.Load() > 0 {
		l.findTail()
	}

	return f
}

// Deletes item from linked list and adjusts count
func (l *Lru) Delete(n *Frame) *Frame {
	fmt.Println("lru.Delete()")
	l.mu.Lock()
	n.mu.Lock()
	defer l.mu.Unlock()
	defer n.mu.Unlock()

	fmt.Println("(Delete) FRAME TO DELETE PIN COUNT -> ", n.pins.Load())

	// If tail or head, update the values
	if l.Tail == n {
		l.Tail = n.prev
	}

	if l.Head == n {
		if n.next == l.Head {
			panic("Cannot set next pointer as itself.")
		}
		l.Head = n.next
	}

	if n.prev != nil {
		n.prev.mu.Lock()
		if n.next != nil {
			if n.prev == n.next {
				panic("Cannot set next pointer as itself.")
			}
			n.prev.next = n.next
		} else {
			n.prev.next = nil
		}
		n.prev.mu.Unlock()
	}

	if n.next != nil {
		n.next.mu.Lock()
		if n.prev != nil {
			log.Println("n => ", n)
			log.Println("n.next => ", n.next)
			log.Println("n.prev => ", n.prev)
			n.next.prev = n.prev // TODO: Check nil ptr dereference err
		} else {
			n.next.prev = nil
		}
		n.next.mu.Unlock()
	} else {
		// set new tail
		l.Tail = n.prev
	}

	l.Count--
	if l.Count < l.pinnedCount.Load() {
		panic(fmt.Errorf("(lru.Delete) (%s) Frame count (%d) cannot be less than pinned frames(%d)", l.segName, l.Count, l.pinnedCount.Load()))
	}

	l.Full = l.Count >= l.Capacity

	return n
}

// Sets frame f as the most recent
func (l *Lru) SetMostRecent(f *Frame) {
	log.Println("lru.SetMostRecent()")
	l.mu.Lock()
	log.Println("(SETMOSTRECENT) Obtained LRU lock...")
	f.mu.Lock()
	log.Println("(SETMOSTRECENT) Obtained FRAME lock...")

	defer l.mu.Unlock()
	defer f.mu.Unlock()

	fmt.Println("SETTING MOST RECENT......................................................")
	log.Println("ITEMS IN LRU ==> ", l.Count)
	log.Println("(SETMOSTRECENT) HEAD => ", l.Head)
	log.Println("(SETMOSTRECENT) TAIL => ", l.Tail)

	if l.Count <= 0 || l.Head == nil {
		log.Println("NO ITEM IN LRU...")
		return
	}

	if l.Count == 1 {
		log.Println("OnLY ONE ITEM IN LRU")
		// set only frame as head and tail
		l.Head = f
		l.Tail = f
		f.prev = nil
		f.next = nil

		return

	}

	if f.prev == nil {
		// is already the most recent
		log.Println("ALREADY AT MOST RECENT")
		return
	}

	log.Println("(SETMOSTRECENT) PREV => ", f.prev)
	log.Println("(SETMOSTRECENT) NEXT => ", f.next)

	if f.prev != nil {
		f.prev.mu.Lock()
		if f.next != nil {
			// FIX: Remove this block
			if f.prev == f.next {
				panic("(setmostrecent) Found it! f.prev == f.next")
			}
			f.prev.next = f.next
		} else {
			f.prev.next = nil
		}
		f.prev.mu.Unlock()
	}

	if f.next != nil {
		f.next.mu.Lock()
		if f.prev != nil {
			f.next.prev = f.prev
		} else {
			f.next.prev = nil
		}
		f.next.mu.Unlock()
	} else {
		// set new tail
		l.Tail = f.prev
	}

	// Add to head ot DLL
	if f.next == l.Head {
		panic("Cannot set next pointer as itself.")
	}
	f.next = l.Head

	log.Println("(SETMOSTRECENT) GETTING LOCK FOR HEAD => l.Head")
	l.Head.mu.Lock()
	log.Println("(SETMOSTRECENT) OBTAINED LOCK FOR HEAD => ")
	l.Head.prev = f
	l.Head.mu.Unlock()

	f.prev = nil
	l.Head = f

	log.Println("(SETMOSTRECENT) PREV => ", f.prev)
	log.Println("(SETMOSTRECENT) NEXT => ", f.next)

	return
}

// Remove frame from LRU. Called when pinning a frame.
func (l *Lru) RemoveFrame(f *Frame) error {
	if f == nil {
		return BufferManagerError{Message: "(RemoveFrame) Frame is required"}
	}

	// FIX: REMOVE DEBUG BLOCK BELOW
	fmt.Println("(REMOVEFRAME) HEAD -> ", l.Head)
	fmt.Println("(REMOVEFRAME) TAIL -> ", l.Tail)

	log.Println("(REMOVEFRAME) HEAD -> ", l.Head)
	log.Println("(REMOVEFRAME) TAIL -> ", l.Tail)
	fmt.Println("(REMOVEFRAME) FRAME TO REMOVE -> ", f)

	if l.Head != nil && l.Head.prev != nil {
		log.Println("(REMOVEFRAME) HEAD PREV -> ", l.Head.prev)
	}

	if l.Head != nil && l.Head.next != nil {
		log.Println("(REMOVEFRAME) HEAD NEXT -> ", l.Head.next)

		if l.Head.next.next != nil {
			log.Println("(REMOVEFRAME) HEAD NEXT NEXT -> ", l.Head.next.next)
		}
	} else {
		log.Println("(REMOVEFRAME) HEAD NEXT IS NIL. COUNT IS -> ", l.Count)
	}

	// f.mu.Lock()
	// defer f.mu.Unlock()

	log.Println("(REMOVEFRAME) FRAME TO REMOVE -> ", f)
	// if already pinned, return
	pinCount := f.GetPinCount()
	if pinCount > 0 {
		// increment pin count
		log.Println("FRAME ALREADY PINNED...")

		l.mu.Lock()
		// Increment pin count
		f.mu.Lock()
		err := f.pinFrame()
		fmt.Printf("(RemoveFrame) Frame after PinFrame => %v\n", f)

		// FIX: REmove debug code below
		if l.Head != nil && l.Head.prev != nil {
			panic(fmt.Errorf("\nHEAD has a previous pointer --> %v\n TAIL -> %v\nCOUNT: %d\nPINCOUNT: %d\n", l.Head, l.Tail, l.Count, l.pinnedCount.Load()))
		}

		l.mu.Unlock()
		f.mu.Unlock()

		if err != nil {
			return err
		}

		fmt.Println("(REMOVEFRAME) HEAD AFTER (PIN > 0) -> ", l.Head)
		fmt.Println("(REMOVEFRAME) TAIL AFTER (PIN > 0) -> ", l.Tail)
		fmt.Println("(REMOVEFRAME) FRAME TO REMOVE AFTER (PIN > 0) -> ", f)
		return nil
	}

	log.Println("lru.RemoveFrame()")
	l.mu.Lock()
	log.Println("(lru.RemoveFrame): obtained lock for lru...")

	log.Println("(lru.RemoveFrame): obtained lock for frame...")
	defer l.mu.Unlock()

	if l.Count <= 0 {
		// No item in LRU. If current frame has pins, it should be in lru
		// hence invalid state
		panic(fmt.Errorf("(%s) No item in LRU despite current frame having no pins, Current pins --> %d \t LRU Count: %d", l.segName, pinCount, l.Count))
	}

	f.mu.Lock()
	// FIX: Remove block below
	if f == f.next {
		fmt.Println("FRAME => ", f)
		fmt.Println("NEXT FRAME => ", f.next)
		fmt.Println("IS HEAD? ", f == l.Head)
		fmt.Println("IS TAIL? ", f == l.Tail)
		fmt.Println("")
		fmt.Println("LRU COUNT => ", l.Count)
		panic(fmt.Sprintf("(%s) Frame pointing to itself as next ptr..", l.segName))
	}

	// FIX: Remove block below
	if f == f.prev {
		panic(fmt.Sprintf("(%s) Frame pointing to itself as prev ptr..", l.segName))
	}

	if l.Head == f {
		fmt.Println("(REMOVEFRAME) Frame Is Head")
		// set new head
		// if l.Count <= 1 {
		// 	fmt.Println("(REMOVEFRAME) Setting head to nil...")
		// 	l.Head = nil
		// } else {
		// 	l.Head = f.next
		// }
		l.Head = f.next
	}

	if l.Tail == f {
		fmt.Println("(REMOVEFRAME) Frame Is tail")
		// set new tail
		// if l.Count <= 1 {
		// 	fmt.Println("(REMOVEFRAME) Setting tail to nil...")
		// 	l.Tail = nil
		// } else {
		// 	l.Tail = f.prev
		// }

		l.Tail = f.prev
	}

	// decrement count
	// if l.Count > 0 {
	// 	l.Count -= 1
	// }

	// f.mu.RUnlock()
	fmt.Printf("(RemoveFrame) (%s) CURR LRU COUNT  B4 PinFrame => %d\n", l.segName, l.Count)
	err := f.pinFrame()
	fmt.Printf("(RemoveFrame) Frame after PinFrame => %v\n", f)

	f.mu.Unlock()
	if err != nil {
		return err
	}

	// FIX: Remove debug code below
	if l.Head != nil && l.Head.prev != nil {
		panic(fmt.Errorf("(%s) HEAD has a previous pointer --> %v\n TAIL -> %v\nCOUNT: %d\n PINCOUNT: %d\n", l.segName, l.Head, l.Tail, l.Count, l.pinnedCount.Load()))
	}

	// Increment pin count for first time pin
	l.pinnedCount.Add(1)
	fmt.Println("(REMOVEFRAME) INCREMENTED PINNED COUNT -> ", l.pinnedCount.Load())

	fmt.Println("(REMOVEFRAME) TAIL AFTER -> ", l.Tail)
	return nil
}

func (l *Lru) GetCount() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	c := l.Count

	return c
}

// traverses the lru and sets the last item as tail
func (l *Lru) findTail() error {
	fmt.Println("lru.findTail ---> ")
	if l.Tail != nil {
		return nil
	}

	curr := l.Head

	for curr != nil {
		if curr.next == nil {
			// found tail
			l.Tail = curr
		}

		curr = curr.next
	}

	return nil
}

func (l *Lru) GetCapacity() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	cp := l.Capacity

	return cp
}

// Returns the key of current tail
func (l *Lru) GetTailKey() uint32 {
	log.Println("lru.GetTailKey()")
	l.mu.RLock()
	l.Tail.mu.RLock()
	defer l.mu.RUnlock()
	if l.Tail == nil {
		return 0
	}

	tKey := l.Tail.Key

	l.Tail.mu.RUnlock()

	return tKey
}

// Readds a frame as head during unpinning
func (l *Lru) ReAddFrame(f *Frame) error {
	log.Println("lru.ReAddFrame()")
	l.mu.Lock()
	log.Println("(lru.ReAddFrame): obtained lock for LRU")
	f.mu.Lock()
	log.Println("(lru.ReAddFrame): obtained lock for Frame")
	defer l.mu.Unlock()
	defer f.mu.Unlock()

	if f.next != nil {
		panic(fmt.Errorf("(reAddFrame) Frame has next pointers -> %v", f.next))
	}

	if f.prev != nil {
		panic(fmt.Errorf("(reAddFrame) Frame has prev pointers -> %v", f.prev))
	}

	if l.Head == f {
		panic("(lru.ReAddFrame) Frame already head of lru")
	}

	if l.Tail == f {
		panic("(lru.ReAddFrame) Frame already tail of lru")
	}

	if l.Count <= 0 {
		fmt.Println("(lru.ReAddFrame): DONE(no item)")
		return BufferManagerError{Message: "No item in LRU"}
	}

	currPinCount := l.pinnedCount.Load()

	if l.Head != nil {
		if f == l.Head {
			panic("Cannot set next pointer as itself.")
		}

		//  FIX: Remove code block below
		if l.Head == f.prev {
			panic(fmt.Errorf("Possible cyclic dependancy. \nf.prev => %v\nl.Head => %v", f.prev, l.Head))
		}

		if f == l.Head.next {
			panic(fmt.Errorf("Possible cyclic dependancy. \nf => %v\nl.Head.next => %v", f, l.Head.next))
		}

		f.next = l.Head
		//log.Println("(lru.ReAddFrame) l.Head ==> ", l.Head)
		log.Println("(lru.ReAddFrame) f ==> ", f)
		log.Println("(lru.ReAddFrame) Getting lock for l.Head")
		l.Head.mu.Lock()
		log.Println("(lru.ReAddFrame) Obtained lock for l.Head: ", l.Head)
		l.Head.prev = f
		log.Println("(lru.ReAddFrame) Updated l.Head: ", l.Head)
		l.Head.mu.Unlock()

		// if all frames are pinned, set head as tail as well
		if l.Count-currPinCount <= 0 && l.Tail == nil {
			l.Tail = l.Head
		}
	}

	// Set curr frame as head
	l.Head = f

	// If tail and head is nil even when the count > 1 due to pinning,
	// set frame as tail and head
	// if l.Tail == nil {
	// 	fmt.Printf("(lru.ReAddFrame) tail is nil with count ->%d\nsetting frame to tail", l.Count)
	// 	l.Tail = f
	// }

	// if l.Count == 1 {
	// 	fmt.Println("(lru.ReAddFrame) Count is 1, setting f as tail")
	// 	l.Tail = f
	// }

	fmt.Println("(lru.ReAddFrame) Unpinned Frame Count -> ", l.Count-currPinCount)
	fmt.Println("(lru.ReAddFrame) pin Count -> ", currPinCount)
	fmt.Println("(lru.ReAddFrame) Frame Count -> ", l.Count)

	if l.Count-currPinCount <= 0 && l.Head == nil {
		// No unpinned items prior, set item is tail
		fmt.Println("(lru.ReAddFrame) No pinned items -> ", l.Count-currPinCount)
		fmt.Printf("(lru.ReAddFrame) SETTING TAIL TO -> %v\n", f)
		fmt.Printf("(lru.ReAddFrame) PREV TAIL -> %v\n", l.Tail)
		l.Tail = f
	}

	l.decrementPinnedFrames()
	currPinCount = l.pinnedCount.Load()

	if currPinCount > l.Count {
		panic(fmt.Errorf("(%s) Invalid pin count.\nPin Count: %d <-> Frame Count: %d", l.segName, currPinCount, l.Count))
	}

	log.Println("(lru.ReAddFrame): DONE")

	if l.Tail == nil && l.Count-l.pinnedCount.Load() > 0 {
		l.findTail()
	}

	return nil
}

func (l *Lru) IsFull() bool {
	log.Println("lru.IsFull()")
	l.mu.RLock()
	defer l.mu.RUnlock()
	log.Printf("(IsFull)LRU: %s\tCAPACITY: %d\t COUNT: %d\n", l.segName, l.Capacity, l.Count)
	return l.Full
}

// Increments the number of borrowed frames
func (l *Lru) borrow() error {
	fmt.Println("(lru.borrow()) borrowing frame")
	l.borrowedFrames.Add(1)

	return nil
}

// Increments the number of borrowed frames
func (l *Lru) returnBorrowedFrame() error {
	b := l.borrowedFrames.Load()

	if b <= 0 {
		return LRUError{Message: "(lru) No borrowed frames"}
	}

	if l.borrowedFrames.CompareAndSwap(b, b-1) {
		return nil
	}

	panic("Unable to return borrowed frames..")
	// return nil
}

// Returns true if LRU list has borrowed frames
func (l *Lru) hasBorrowedFrames() bool {
	b := l.borrowedFrames.Load()

	return b > 0
}

// Decrement pinned count
func (l *Lru) decrementPinnedFrames() {
	fmt.Println("lru.decrementPinnedFrames()")
	k := l.pinnedCount.Load()

	fmt.Println("(lru.decrementPinnedFrames) pinnedCount -> ", k)
	if k > 0 {
		if l.pinnedCount.CompareAndSwap(k, k-1) {
			fmt.Println("(decrementPinnedFrames) decremented pinned frames -> ", l.pinnedCount.Load())
			return
		} else {
			panic("Unable to decrement pinned frames....")
		}
	}

	return
}

// Create new instance of LRU Linked List
func NewLRU(capacity uint64, name string) *Lru {
	return &Lru{
		Count:    0,
		Capacity: capacity,
		segName:  name,
		Full:     false,
	}
}
