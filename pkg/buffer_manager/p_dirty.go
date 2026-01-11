package buffermanager

import (
	"fmt"
	"log"
	"sync"
)

// Dirty page structure
// All dirty pages are kept in a doubly linked-list and
// order is maintained as in a Least Recently Used(LRU) pattern
// A hash map of pageID and pDirty stores dirty pages for O(1) lookup
type dPages struct {
	dPageLru *dirtyPageLRU
	dPageMap map[uint32]*pDirty
	mu       sync.RWMutex
}

// Dirty pages linked list
type dirtyPageLRU struct {
	head *pDirty
	tail *pDirty
	mu   sync.RWMutex
}

type pDirty struct {
	frame *Frame
	next  *pDirty
	prev  *pDirty
	mu    sync.Mutex
}

// remove and returns the tail item from the list, nil if list is empty
func (dl *dirtyPageLRU) popDirty() *pDirty {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if dl.tail == nil || dl.head == nil {
		return nil
	}

	t := dl.tail

	if dl.head == dl.tail {
		// only one item in linked list
		dl.head = nil
		dl.tail = nil
	} else {
		dl.tail = t.prev
	}

	return t
}

// checks if there are items in dirty page list
func (dl *dirtyPageLRU) isEmpty() bool {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	if dl.head == nil || dl.tail == nil {
		return true
	}

	return false
}

// Adds a dirty frame to dirty list
func (dl *dirtyPageLRU) addToDirtyList(p *pDirty) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	//	pD := pDirty{
	//		frame: f,
	//	}

	if dl.head != nil {
		dl.head.mu.Lock()
		defer dl.head.mu.Unlock()
	}

	if dl.head == nil || dl.tail == nil {
		// first entry
		dl.head = p
		dl.tail = p

		return nil
	}

	p.next = dl.head
	dl.head.prev = p
	dl.head = p

	return nil
}

// moves a dirty page in the linkedlist and moves it to the head.
func (dl *dirtyPageLRU) moveToHead(pD *pDirty) error {
	dl.mu.Lock()
	pD.mu.Lock()
	log.Println("Obtained lock for dirtyLRU and pDirty....")
	defer dl.mu.Unlock()
	defer pD.mu.Unlock()

	if dl.head == pD {
		// already head of list
		if pD.prev != nil {
			panic(fmt.Errorf("(moveToHead) head has a prev pointer. pD.prev -> %v", pD.prev))
		}
		log.Println("(moveToHead) ALready head  of lru...")
		return nil
	}

	// acquire adjascent nodes locks
	if pD.prev != nil {
		log.Println("(moveTOHead) Obtaining lock for prev node....")
		pD.prev.mu.Lock()
		// defer pD.prev.mu.Unlock()
	}

	if pD.next != nil {
		log.Println("(moveTOHead) Obtaining lock for next node....")
		pD.next.mu.Lock()
		// defer pD.next.mu.Unlock()
	}

	// update next node prev ptr
	if pD.next != nil {
		pD.next.prev = pD.prev
		pD.next.mu.Unlock()
	}

	// update prev node next ptr
	if pD.prev != nil {
		pD.prev.next = pD.next
		pD.prev.mu.Unlock()
	}

	pD.next = dl.head
	// set prev ptr for old head
	log.Println("(moveTOHead) Obtaining lock for head of LRU list....")
	dl.head.mu.Lock()
	log.Println("(moveTOHead) Obtained lock for head of LRU list....")
	dl.head.prev = pD
	dl.head.mu.Unlock()

	log.Println("(moveTOHead) Setting new head of LRU list....")
	dl.head = pD

	if dl.tail == pD {
		log.Println("(moveTOHead) Setting new tail of LRU list....")
		// if was tail, set new tail
		dl.tail = pD.prev
	}

	// update it's  prev ptr. Set to nil since it is head of list
	log.Println("(moveTOHead) Resetting tail of pDirty....")
	pD.prev = nil

	return nil
}

// removes a dirty page from dirty page linked list.
// This is called when a page is flushed.
func (dl *dirtyPageLRU) removeFromLru(d *pDirty) error {
	dl.mu.Lock()
	d.mu.Lock()
	defer dl.mu.Unlock()
	defer d.mu.Unlock()

	if dl.head == d && dl.tail == d {
		// the only item clear
		dl.head = nil
		dl.tail = nil

		return nil
	}

	// acquire locks for adjascent nodes
	if d.prev != nil {
		d.prev.mu.Lock()
		defer d.prev.mu.Unlock()
	}

	if d.next != nil {
		d.next.mu.Lock()
		defer d.next.mu.Unlock()
	}

	// update adjascent nodes
	if d.prev != nil {
		d.prev.next = d.next
	}

	if d.next != nil {
		d.next.prev = d.prev
	}

	// Update tail or head ptrs if necessary
	if dl.head == d {
		dl.head = d.next
	}

	if dl.tail == d {
		dl.tail = d.prev
	}

	// clear remaining ptrs
	d.next = nil
	d.prev = nil

	return nil
}

// Adds a frame to dirty pages list
func (dp *dPages) addDirtyFrame(f *Frame) {
	log.Println("Adding frame to dirty list....")
	dp.mu.RLock()
	// check hash map
	fKey := f.GetKey()
	log.Printf("Checking if frame (KEY: %d) is in dirty map...\n", fKey)
	pD, ok := dp.dPageMap[fKey]
	dp.mu.RUnlock()

	if ok {
		// dirty page is already part of dirty page list.
		// move to head of LRU list
		log.Println("Frame is in dirty map...")
		err := dp.dPageLru.moveToHead(pD)
		log.Printf("FRAME (KEY: %d) MOVED TO HEAD...\n", fKey)

		if err != nil {
			log.Panic(err)
		}
	} else {
		// Add new frame to dirty page list
		log.Println("Frame not in dirty map...")
		p := pDirty{
			frame: f,
		}

		dp.mu.Lock()
		dp.dPageMap[f.GetKey()] = &p
		dp.mu.Unlock()

		// Add to LRU
		log.Println("Adding to dirty LRU...")
		err := dp.dPageLru.addToDirtyList(&p)

		if err != nil {
			log.Panic(err)
		}
	}
}

// Pop LRU pDirty from dirty page linked list and removes it from hashmap
func (dp *dPages) popDirtyPage() *pDirty {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	p := dp.dPageLru.popDirty()

	if p == nil {
		return p
	}

	pgeId := p.frame.GetKey()

	if pgeId == 0 {
		// is nil. return nil page
		return nil
	}

	delete(dp.dPageMap, pgeId)

	return p
}

func NewDirtyPageList() *dPages {
	dP := dPages{
		dPageLru: &dirtyPageLRU{},
		dPageMap: make(map[uint32]*pDirty),
	}

	return &dP
}
