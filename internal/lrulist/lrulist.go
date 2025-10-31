package lrulist

import (
	"log"
	"sync"

	diskio "github.com/oryankibandi/on_disk_btree/pkg/disk_io"
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
	l.mu.RLock()
	defer l.mu.RUnlock()

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

	l.Tail = l.Tail.Prev
	oldTail.Prev.Next = nil

	l.Count -= 1

	l.Full = l.Count >= l.Capacity

	return oldTail
}

// Add item to linked list
func (l *LruList[T]) Add(f *Frame) *Frame {
	log.Println("ADDINT ITEM TO CACHE^^^^^^^>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	log.Println("ADDINT ITEM TO CACHE^^^^^^^>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	log.Println("ADDINT ITEM TO CACHE^^^^^^^>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	// log.Println("ITEM >> ", f)
	l.mu.RLock()
	defer l.mu.RUnlock()
	log.Println("ITEM >> ", f)
	log.Println("ITEM COUNT B4:>>>>>>>>> ", l.Count)

	if l.Count <= 0 {
		l.Head = f
		l.Tail = f
		l.Count++

		l.Full = l.Count >= l.Capacity

		log.Println("FINAL COUNT AFTER ADD ==> ", l.Count)
		return f
	}

	// Add item before head, and make it the new head
	if l.Head != nil {
		l.Head.Prev = f
		f.Next = l.Head
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
	l.mu.RLock()
	defer l.mu.RUnlock()

	// If tail or head, update the values
	if n.Next == nil && n.Prev != nil {
		l.Tail = n.Prev
	}

	if n.Prev == nil && n.Next != nil {
		l.Head = n.Next
	}

	if n.Prev != nil {
		if n.Next != nil {
			n.Prev.Next = n.Next
		} else {
			n.Prev.Next = nil
		}
	}

	if n.Next != nil {
		if n.Prev != nil {
			n.Next.Prev = n.Prev
		} else {
			n.Next.Prev = nil
		}
	} else {
		// set new tail
		l.Tail = n.Prev
	}

	l.Count--
	l.Full = l.Count >= l.Capacity

	return n
}

// Prepares page for eviction by flusing page to disk
func (f *Frame) PrepareForEviction() error {
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
func (l *LruList[T]) SetMostRecent(n *Frame) {
	l.mu.Lock()
	n.mu.Lock()

	defer n.mu.Unlock()

	defer l.mu.Unlock()
	log.Println("SETTING MOST RECENT......................................................")
	log.Println("ITEMS IN LRU ==> ", l.Count)

	if l.Count <= 0 || l.Head == nil {
		log.Println("NO ITEM IN LRU...")
		return
	}

	if l.Count == 1 {
		log.Println("OnLY ONE ITEM IN LRU")
		// set only frame as head and tail
		l.Head = n
		l.Tail = n
		n.Prev = nil
		n.Next = nil

		return

	}

	if n.Prev == nil {
		// is already the most recent
		log.Println("ALREADY AT MOST RECENT")
		return
	}

	log.Println("PREV => ", n.Prev)
	log.Println("NEXT => ", n.Next)

	if n.Prev != nil {
		if n.Next != nil {
			n.Prev.Next = n.Next
		} else {
			n.Prev.Next = nil
		}
	}

	if n.Next != nil {
		if n.Prev != nil {
			n.Next.Prev = n.Prev
		} else {
			n.Next.Prev = nil
		}
	} else {
		// set new tail
		l.Tail = n.Prev
	}

	// Add to head ot DLL
	n.Next = l.Head
	l.Head.Prev = n
	n.Prev = nil
	l.Head = n

	return
}

// Remove frame from LRU. Called when pinning/deleting a frame
func (l *LruList[T]) RemoveFrame(f *Frame) error {
	l.mu.Lock()
	f.mu.RLock()
	defer f.mu.RUnlock()
	defer l.mu.Unlock()

	if l.Head == f {
		// set new head
		l.Head = f.Next
	}

	if l.Tail == f {
		// set new tail
		l.Tail = f.Prev
	}

	if l.Count > 0 {
		l.Count--
	}

	return nil
}

// Pins frame
func (f *Frame) PinFrame() error {
	log.Println("Obtaining Frame Pin...")
	f.mu.Lock()
	defer f.mu.Unlock()
	log.Println("Obtained Frame Pin...")
	log.Println("CURR FRAME => ", f)
	if f.Pins == 0 {
		// first time pin, remove from LRU
		if f.Prev != nil {
			log.Println("(pinFrame) Geting look for prev frame...->", f.Prev)
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

// Unpins a frame and if no other pins exists, returns if it should be added back to LRU
func (f *Frame) UnpinFrame() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.Pins <= 0 {
		// already unpinned
		return false, nil
	}

	f.Pins--

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

// Create new instance of LRU Linked List
func NewLRUList[T any](capacity uint64) *LruList[T] {
	return &LruList[T]{
		Count:    0,
		Capacity: capacity,
		Full:     false,
	}
}
