package buffermanager

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	diskmanager "github.com/oryankibandi/baobab/pkg/disk_io"
)

// A frame represents a location in the cache/buffer.
// It holds the 8K page in memory to reduce time taken
// to retrieve it from disk. It also buffers changes to page before
// they are persisted to disk by the background writer
type Frame struct {
	pins       atomic.Uint32
	prev       *Frame
	next       *Frame
	Key        uint32 // Key is the page ID
	CacheType  LruType
	page       *diskmanager.Page
	isDirty    bool
	isDeleted  atomic.Bool // If the frame and associated page is marked for  deletion
	IsInternal bool        // true if is an internal node
	lsn        []byte      // LSN of last operation. Added when marking as dirty
	mu         sync.RWMutex
}

// This is a minimal page struct with items required to materialize a node
// in the B+ Tree Index
type PgeMin struct {
	PageId       uint32
	Keys         [][]byte
	Vals         [][]byte
	Children     []int32
	RightSibling int32
	LeftSibling  int32
}

// Pins frame. Frame lock should be acquired before calling this method.
// Updates pointers of adjascent frames as well as the provided frame
func (f *Frame) pinFrame() error {
	log.Println("(PinFrame) Obtaining Frame Lock...")
	//	f.mu.Lock()
	//	defer f.mu.Unlock()
	log.Println("(PinFrame) Obtained Frame Lock...")
	log.Println("CURR FRAME => ", f)
	if f.pins.Load() == 0 {
		// first time pin, remove from LRU
		if f.prev != nil {
			f.prev.mu.Lock()
			log.Println("(pinFrame) Obtained lock for prev frame...")
			// FIX: Remove debug block below
			if f.prev == f.next {
				panic(fmt.Errorf("(pinFrame) invalid pointers. f.prev == f.next\nf.prev.prev -> %v \n f.prev -> %v\nf -> %v\nf.next -> %v\nf.next.next -> %v", f.prev.prev, f.prev, f, f.next, f.next.next))
			}

			f.prev.next = f.next
			f.prev.mu.Unlock()
		}

		// FIX:: Remove f.next != f condition after bug fix
		if f.next != nil && f.next == f {
			panic("(pinFrame) FRAME POINTING TO ITSELF AS NEXT Ptr")
		}

		if f.next != nil {
			log.Println("(pinFrame) Geting lock for next frame...->", f.next)
			f.next.mu.Lock()
			log.Println("(pinFrame) Obtained lock for next frame...")
			// FIX:Remove block cde below
			if f.next == f.prev {
				panic(fmt.Errorf("(pinFrame) invalid pointers. f.next == f.prev\n f.next -> %v\nf.prev -> %v\nf -> %v", f.next, f.prev, f))
			}

			f.next.prev = f.prev
			f.next.mu.Unlock()
		}
	}

	f.pins.Add(1)
	f.next = nil
	f.prev = nil
	fmt.Println("(pinframe) frame after pin -> ", f)
	log.Println("(pinFrame) DONE.")

	return nil
}

func (f *Frame) GetKey() uint32 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	k := f.Key

	return k
}

func (f *Frame) GetPage() *diskmanager.Page {
	f.mu.RLock()
	defer f.mu.RUnlock()

	p := f.page

	return p
}

// Decrements pin count and if no other pins exists,
// returns true if it should be added back to LRU
func (f *Frame) UnpinFrame() (bool, error) {
	//	pinCount := f.pins.Load()
	//	if pinCount <= 0 {
	//		// already unpinned
	//		return false, nil
	//	}
	//
	//	f.pins.Store(pinCount - 1)
	//
	//	pinCount = f.pins.Load()
	//
	//	if pinCount <= 0 {
	//		return true, nil
	//	}
	//
	//	return false, nil

	fmt.Println("UNPINNING FRAME WITH KEY -> ", f)
	for {
		c := f.pins.Load()

		if c <= 0 {
			log.Println("Pin count <= 0")
			return false, nil
		}

		if f.pins.CompareAndSwap(c, c-1) {
			// if updated pin count is zero, return true
			return c-1 <= 0, nil
		}
	}
}

func (f *Frame) GetCacheType() LruType {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.CacheType
}

func (f *Frame) UpdatePage(p *diskmanager.Page) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.page = p

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
	d, err := f.page.IsDirty()

	return d, err
}

// returns true if the frame is dirty
func (f *Frame) IsDirty() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	d := f.isDirty

	return d
}

// Set the frame as dirty. This means it  has unflushed changes
func (f *Frame) markDirty() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.isDirty = true
	log.Println("(markDirty) Set frame to dirty.....")
}

// Mark the frame as clean. This means all updates have been flushed.
func (f *Frame) markClean() {
	fmt.Println("(markClean) Marking frame as clean...")
	f.mu.Lock()
	defer f.mu.Unlock()

	f.isDirty = false
}

// Check if a frame's page is marked for deletion.
func (f *Frame) PageIsDead() (bool, error) {
	if f.page == nil {
		return false, BufferManagerError{Message: "No page associated with frame."}
	}

	// d := f.page.IsDeleted()
	d := f.isDeleted.Load()

	return d, nil
}

// marks a frame and associated page as dead(to be deleted)
func (f *Frame) MarkAsDead() error {
	f.isDeleted.Store(true)

	err := f.page.MarkAsDead()

	if err != nil {
		return err
	}

	return nil
}

// Gets the minimal items from a page to materialize a node
func (f *Frame) GetMinPage() (PgeMin, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.page == nil {
		return PgeMin{}, BufferManagerError{Message: "No page associated with page"}
	}

	k, v, children, rightPtr, err := f.page.GetCellData()

	if err != nil {
		return PgeMin{}, err
	}

	if rightPtr != 0 {
		children = append(children, rightPtr)
	}

	// Get Siblings
	rSib, lSib := f.page.Header.GetSiblngs()

	p := PgeMin{
		Keys:         k,
		Vals:         v,
		PageId:       f.Key,
		Children:     children,
		RightSibling: rSib,
		LeftSibling:  lSib,
	}

	return p, nil
}

// Returns true if associated page is an internal node, else false
func (f *Frame) Internal() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	i := f.IsInternal

	return i
}

// Updates LSN of a frame's attached page
func (f *Frame) UpdatePageLSN(lsn []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.page == nil {
		return BufferManagerError{Message: "Unable to update page LSN: No page associated with frame"}
	}

	err := f.page.UpdateLSN(lsn)

	if err != nil {
		return err
	}

	f.lsn = lsn

	return nil
}

// Returns Log Sequence Number of the frame. This is the same LSN as the page.
func (f *Frame) GetLSN() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.page == nil {
		return nil, BufferManagerError{Message: "No page associated with frame"}
	}

	if f.lsn == nil {
		return nil, BufferManagerError{Message: "Invalid lsn on frame."}
	}

	log.Println("(GetLSN) Frame ==> ", f)
	pLsn := f.page.GetLSN()
	log.Println("(GetLSN) PAGE LSN ==> ", pLsn)

	return f.lsn, nil
}

func (f *Frame) setNextPtr(fr *Frame) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f == fr {
		panic(fmt.Errorf("(setPtrs) Cannot set curr frame as its own next pointer. \nFrame -> %v\nProvided next -> %v\n", f, fr))
	}

	if f.prev == fr && (f.prev != nil && fr != nil) {
		panic(fmt.Errorf("(setNextPtr) Frame has prev pointer same as next ptr. \nprev -> %v\nnext -> %v\n", f.prev, fr))
	}

	f.next = fr
}

func (f *Frame) setPrevPtr(fr *Frame) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f == fr {
		panic(fmt.Errorf("(setPtrs) Cannot set curr frame as its own prev pointer. \nFrame -> %v\nProvided prev -> %v\n", f, fr))
	}

	if f.next == fr && (f.next != nil && fr != nil) {
		panic(fmt.Errorf("(setPrevPtr) frame has prev pointer equal to next pointer. \nprev -> %v\next -> %v\n", fr, f.next))
	}

	f.prev = fr
}

func (f *Frame) setPtrs(prev *Frame, next *Frame) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f == prev {
		panic(fmt.Errorf("(setPtrs) Cannot set curr frame as its own prev pointer. \nFrame -> %v\nProvided prev -> %v\n", f, prev))
	}

	if f == next {
		panic(fmt.Errorf("(setPtrs) Cannot set curr frame as its own next pointer. \nFrame -> %v\nProvided next -> %v\n", f, next))
	}

	f.prev = prev
	f.next = next
}

func (f *Frame) GetPinCount() uint32 {
	if f == nil {
		panic("(GetPinCount) No frame provided")
	}
	p := f.pins.Load()

	return p
}
