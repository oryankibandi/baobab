package buffermanager

import (
	"log"
	"sync"
)

const (
	Window LruType = iota
	Probation
	Protected
)

type LruType int

type Lru struct {
	Head     *Frame
	Tail     *Frame
	Count    uint64
	Capacity uint64
	Full     bool
	mu       sync.RWMutex
}

// Remove least recently used
func (l *Lru) Pop() *Frame {
	log.Println("lru.Pop()")
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.Count <= 0 {
		log.Println("(pop) list is empty")
		return nil
	}

	if l.Count == 1 {
		// remove head and empty list
		h := l.Head

		l.Count--
		l.Head = nil
		l.Tail = nil

		l.Full = l.Count >= l.Capacity

		return h
	}

	if l.Tail == nil {
		log.Println("(pop) No tail")
		return nil
	}

	oldTail := l.Tail

	l.Tail = oldTail.prev

	if oldTail.prev != nil {
		oldTail.mu.RLock()
		oldTail.prev.mu.Lock()
		oldTail.prev.next = nil
		oldTail.mu.RUnlock()
		oldTail.prev.mu.Unlock()
	}

	l.Count -= 1

	l.Full = l.Count >= l.Capacity

	return oldTail
}

// Add item to the doubly linked list
func (l *Lru) Add(f *Frame) *Frame {
	if f == nil {
		panic("(Add) No Frame provided")
	}

	log.Println("ADDINT ITEM TO CACHE^^^^^^^>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	log.Println("ADDINT ITEM TO CACHE^^^^^^^>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	log.Println("ADDINT ITEM TO CACHE^^^^^^^>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	// log.Println("ITEM >> ", f)
	l.mu.Lock()
	log.Println("OBTAINED LRU LOCK >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	f.mu.Lock()
	log.Println("OBTAINED FRAME LOCK >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	defer l.mu.Unlock()
	defer f.mu.Unlock()
	log.Println("ITEM >> ", f)
	log.Println("ITEM COUNT B4:>>>>>>>>> ", l.Count)

	if l.Head == f {
		panic("(Add) Frame is already head of lru list")
	}

	if l.Tail == f {
		panic("(Add) Frame is already tail of lru list")
	}

	if l.Count <= 0 {
		l.Head = f
		l.Tail = f
		f.next = nil
		f.prev = nil
		l.Count++

		l.Full = l.Count >= l.Capacity

		log.Println("FINAL COUNT AFTER ADD ==> ", l.Count)
		return f
	}

	// Add item before head, and make it the new head
	if l.Head != nil {
		l.Head.mu.Lock()
		l.Head.prev = f
		f.next = l.Head
		l.Head.mu.Unlock()
	}

	// if only one item existed in the DLL, ensure the old head is set as the tail
	if l.Count == 1 {
		l.Tail = l.Head
	}

	// set new head
	l.Head = f

	l.Count++
	l.Full = l.Count >= l.Capacity

	log.Println("FINAL COUNT AFTER ADD ==> ", l.Count)

	return f
}

// Deletes item from linked list and adjusts count
func (l *Lru) Delete(n *Frame) *Frame {
	log.Println("lru.Delete()")
	l.mu.Lock()
	n.mu.Lock()
	defer l.mu.Unlock()
	defer n.mu.Unlock()

	// If tail or head, update the values
	if n.next == nil && n.prev != nil {
		l.Tail = n.prev
	}

	if n.prev == nil && n.next != nil {
		l.Head = n.next
	}

	if n.prev != nil {
		n.prev.mu.Lock()
		if n.next != nil {
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
	l.Full = l.Count >= l.Capacity

	return n
}

// Sets node n as the most recent
func (l *Lru) SetMostRecent(f *Frame) {
	log.Println("lru.SetMostRecent()")
	l.mu.Lock()
	log.Println("(SETMOSTRECENT) Obtained LRU lock...")
	f.mu.Lock()
	log.Println("(SETMOSTRECENT) Obtained FRAME lock...")

	defer l.mu.Unlock()
	defer f.mu.Unlock()

	log.Println("SETTING MOST RECENT......................................................")
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

// Remove frame from LRU. Called when pinning/deleting a frame.
func (l *Lru) RemoveFrame(f *Frame) error {
	if f == nil {
		return BufferManagerError{Message: "(RemoveFrame) Frame is required"}
	}

	log.Println("lru.RemoveFrame()")
	l.mu.Lock()
	f.mu.Lock()
	defer l.mu.Unlock()
	defer f.mu.Unlock()

	if l.Head == f {
		// set new head
		if l.Count <= 1 {
			l.Head = nil
		} else {
			l.Head = f.next
		}
	}

	if l.Tail == f {
		// set new tail
		if l.Count <= 1 {
			l.Tail = nil
		} else {
			l.Tail = f.prev
		}
	}

	if l.Count > 0 {
		l.Count -= 1
	}

	// f.mu.RUnlock()
	log.Println("(RemoveFrame) CURR LRU COUNT  B4 PinFrame => ", l.Count)
	err := f.pinFrame()

	if err != nil {
		return err
	}

	return nil
}

func (l *Lru) GetCount() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	c := l.Count

	return c
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
	f.mu.Lock()
	defer l.mu.Unlock()
	defer f.mu.Unlock()

	if l.Count <= 0 {
		return BufferManagerError{Message: "No item in LRU"}
	}

	if l.Head != nil {
		f.next = l.Head
		l.Head.mu.Lock()
		l.Head.prev = f
		l.Head.mu.Unlock()
	} else {
		l.Head = f
	}

	if l.Count == 1 {
		l.Tail = f
	}

	return nil
}

func (l *Lru) IsFull() bool {
	log.Println("lru.IsFull()")
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.Full
}

// Create new instance of LRU Linked List
func NewLRU(capacity uint64) *Lru {
	return &Lru{
		Count:    0,
		Capacity: capacity,
		Full:     false,
	}
}
