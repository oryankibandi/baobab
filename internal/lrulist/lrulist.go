package lrulist

import (
	"log"
	"sync"

	diskio "github.com/oryankibandi/baobab/pkg/disk_io"
)

const (
	Window LruType = iota
	Probation
	Protected
)

type LruType int

type LruList[T any] struct {
	Head     *Frame
	Tail     *Frame
	Count    uint64
	Capacity uint64
	Full     bool
	mu       sync.RWMutex
}

//	type LRUNode[T any] struct {
//		Item *T
//		Prev *LRUNode[T]
//		Next *LRUNode[T]
//	}
type Frame struct {
	Pins      uint32
	Prev      *Frame
	Next      *Frame
	Key       uint32
	CacheType LruType
	Page      *diskio.Page
	mu        sync.RWMutex
}

// Remove least recently used
func (l *LruList[T]) Pop() *Frame {
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

	l.Tail = oldTail.Prev

	if oldTail.Prev != nil {
		oldTail.mu.RLock()
		oldTail.Prev.mu.Lock()
		oldTail.Prev.Next = nil
		oldTail.mu.RUnlock()
		oldTail.Prev.mu.Unlock()
	}

	l.Count -= 1

	l.Full = l.Count >= l.Capacity

	return oldTail
}

// Add item to the doubly linked list
func (l *LruList[T]) Add(f *Frame) *Frame {
	if f == nil {
		panic("(Add) frame is nil")
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
		f.Next = nil
		f.Prev = nil
		l.Count++

		l.Full = l.Count >= l.Capacity

		log.Println("FINAL COUNT AFTER ADD ==> ", l.Count)
		return f
	}

	// Add item before head, and make it the new head
	if l.Head != nil {
		l.Head.mu.Lock()
		l.Head.Prev = f
		f.Next = l.Head
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
func (l *LruList[T]) Delete(n *Frame) *Frame {
	log.Println("lru.Delete()")
	l.mu.Lock()
	n.mu.Lock()
	defer l.mu.Unlock()
	defer n.mu.Unlock()

	// If tail or head, update the values
	if n.Next == nil && n.Prev != nil {
		l.Tail = n.Prev
	}

	if n.Prev == nil && n.Next != nil {
		l.Head = n.Next
	}

	if n.Prev != nil {
		n.Prev.mu.Lock()
		if n.Next != nil {
			n.Prev.Next = n.Next
		} else {
			n.Prev.Next = nil
		}
		n.Prev.mu.Unlock()
	}

	if n.Next != nil {
		n.Next.mu.Lock()
		if n.Prev != nil {
			log.Println("n => ", n)
			log.Println("n.Next => ", n.Next)
			log.Println("n.Prev => ", n.Prev)
			n.Next.Prev = n.Prev // TODO: Check nil ptr dereference err
		} else {
			n.Next.Prev = nil
		}
		n.Next.mu.Unlock()
	} else {
		// set new tail
		l.Tail = n.Prev
	}

	l.Count--
	l.Full = l.Count >= l.Capacity

	return n
}

// Prepares page for eviction by flushing page to disk
func (f *Frame) PrepareForEviction() error {
	log.Println("lru.PrepareForEviction()")
	f.mu.Lock()
	f.mu.Unlock()
	c := make(chan int32)
	err := diskio.DiskBTree.WriteReq(f.Page, &c)

	if err != nil {
		panic(err.Error())
	}

	n := <-c

	log.Printf("(PrepareForEviction) Flushed %d page\n", n)

	return nil
}

// Sets node n as the most recent
func (l *LruList[T]) SetMostRecent(f *Frame) {
	log.Println("lru.SetMostRecent()")
	l.mu.Lock()
	log.Println("(SETMOSTRECENT) Obtained LRU lock...")
	f.mu.Lock()
	log.Println("(SETMOSTRECENT) Obtained FRAME lock...")

	defer f.mu.Unlock()
	defer l.mu.Unlock()

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
		f.Prev = nil
		f.Next = nil

		return

	}

	if f.Prev == nil {
		// is already the most recent
		log.Println("ALREADY AT MOST RECENT")
		return
	}

	log.Println("(SETMOSTRECENT) PREV => ", f.Prev)
	log.Println("(SETMOSTRECENT) NEXT => ", f.Next)

	if f.Prev != nil {
		f.Prev.mu.Lock()
		if f.Next != nil {
			f.Prev.Next = f.Next
		} else {
			f.Prev.Next = nil
		}
		f.Prev.mu.Unlock()
	}

	if f.Next != nil {
		f.Next.mu.Lock()
		if f.Prev != nil {
			f.Next.Prev = f.Prev
		} else {
			f.Next.Prev = nil
		}
		f.Next.mu.Unlock()
	} else {
		// set new tail
		l.Tail = f.Prev
	}

	// Add to head ot DLL
	f.Next = l.Head

	log.Println("(SETMOSTRECENT) GETTING LOCK FOR HEAD => l.Head")
	l.Head.mu.Lock()
	log.Println("(SETMOSTRECENT) OBTAINED LOCK FOR HEAD => ")
	l.Head.Prev = f
	l.Head.mu.Unlock()

	f.Prev = nil
	l.Head = f

	log.Println("(SETMOSTRECENT) PREV => ", f.Prev)
	log.Println("(SETMOSTRECENT) NEXT => ", f.Next)

	return
}

// Remove frame from LRU. Called when pinning/deleting a frame.
func (l *LruList[T]) RemoveFrame(f *Frame) error {
	if f == nil {
		return LruError{Message: "(RemoveFrame) Frame is required"}
	}

	log.Println("lru.RemoveFrame()")
	l.mu.Lock()
	f.mu.Lock()
	defer l.mu.Unlock()
	defer f.mu.Unlock()

	if l.Head == f {
		// set new head
		l.Head = f.Next
	}

	if l.Tail == f {
		// set new tail
		l.Tail = f.Prev
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

func (l *LruList[T]) GetCount() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	c := l.Count

	return c
}

func (l *LruList[T]) GetCapacity() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	cp := l.Capacity

	return cp
}

// Returns the key of current tail
func (l *LruList[T]) GetTailKey() uint32 {
	log.Println("lru.GetTailKey()")
	l.mu.RLock()
	l.Tail.mu.RLock()
	defer l.mu.RUnlock()
	tKey := l.Tail.Key

	l.Tail.mu.RUnlock()

	return tKey
}

// Readds a frame as head during unpinning
func (l *LruList[T]) ReAddFrame(f *Frame) error {
	log.Println("lru.ReAddFrame()")
	l.mu.Lock()
	f.mu.Lock()
	defer l.mu.Unlock()
	defer f.mu.Unlock()

	if l.Count <= 0 {
		return LruError{Message: "No item in LRU"}
	}

	if l.Head != nil {
		f.Next = l.Head
		l.Head.mu.Lock()
		l.Head.Prev = f
		l.Head.mu.Unlock()
	} else {
		l.Head = f
	}

	if l.Count == 1 {
		l.Tail = f
	}

	return nil
}

func (l *LruList[T]) IsFull() bool {
	log.Println("lru.IsFull()")
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.Full
}

// Pins frame
func (f *Frame) pinFrame() error {
	if f.Pins < 0 {
		panic("(PinFrame) Pin count is less than zero.")
	}

	log.Println("(PinFrame) Obtaining Frame Lock...")
	//	f.mu.Lock()
	//	defer f.mu.Unlock()
	log.Println("(PinFrame) Obtained Frame Lock...")
	log.Println("CURR FRAME => ", f)
	if f.Pins == 0 {
		// first time pin, remove from LRU
		if f.Prev != nil {
			f.Prev.mu.Lock()
			log.Println("(pinFrame) Obtained lock for prev frame...")
			f.Prev.Next = f.Next
			f.Prev.mu.Unlock()
		}

		if f.Next != nil {
			log.Println("(pinFrame) Geting lock for next frame...->", f.Next)
			f.Next.mu.Lock()
			log.Println("(pinFrame) Obtained lock for next frame...")
			f.Next.Prev = f.Prev
			f.Next.mu.Unlock()
		}
	}

	f.Pins += 1
	f.Next = nil
	f.Prev = nil

	return nil
}

func (f *Frame) GetKey() uint32 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	k := f.Key

	return k
}

func (f *Frame) GetPage() *diskio.Page {
	f.mu.RLock()
	defer f.mu.RUnlock()

	p := f.Page

	return p
}

// Unpins a frame and if no other pins exists,
// returns if it should be added back to LRU
func (f *Frame) UnpinFrame() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.Pins <= 0 {
		// already unpinned
		return false, nil
	}

	f.Pins -= 1

	if f.Pins <= 0 {
		return true, nil
	}

	return false, nil
}

func (f *Frame) GetCacheType() LruType {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.CacheType
}

func (f *Frame) UpdatePage(p *diskio.Page) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Page = p

	return nil
}

func (f *Frame) UpdateCacheType(t LruType) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.CacheType = t
}

func (f *Frame) PageIsDirty() (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	d, err := f.Page.IsDirty()

	return d, err
}

// Check if a frame's page is marked for deletion.
func (f *Frame) PageIsDead() (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.Page == nil {
		return false, LruError{Message: "No page associated with frame."}
	}
	d := f.Page.IsDeleted()

	return d, nil
}

// Create new instance of LRU Linked List
func NewLRUList[T any](capacity uint64) *LruList[T] {
	return &LruList[T]{
		Count:    0,
		Capacity: capacity,
		Full:     false,
	}
}
