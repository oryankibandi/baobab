package buffermanager

import (
	"log"
	"sync"

	diskmanager "github.com/oryankibandi/baobab/pkg/disk_io"
)

// A frame represents a location in the cache/buffer.
// It holds the 8K page in memory to reduce time taken
// to retrieve it from disk. It also buffers changes to page before
// they are persisted to disk by the background writer
type Frame struct {
	pins       uint32
	prev       *Frame
	next       *Frame
	Key        uint32 // Key is the page ID
	CacheType  LruType
	page       *diskmanager.Page
	isDirty    bool
	isDeleted  bool   // If the frame and associated page is marked for  deletion
	IsInternal bool   // true if is an internal node
	lsn        []byte // LSN of last operation. Added when marking as dirty
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

// Pins frame
func (f *Frame) pinFrame() error {
	if f.pins < 0 {
		panic("(PinFrame) Pin count is less than zero.")
	}

	log.Println("(PinFrame) Obtaining Frame Lock...")
	//	f.mu.Lock()
	//	defer f.mu.Unlock()
	log.Println("(PinFrame) Obtained Frame Lock...")
	log.Println("CURR FRAME => ", f)
	if f.pins == 0 {
		// first time pin, remove from LRU
		if f.prev != nil {
			f.prev.mu.Lock()
			log.Println("(pinFrame) Obtained lock for prev frame...")
			f.prev.next = f.next
			f.prev.mu.Unlock()
		}

		if f.next != nil {
			log.Println("(pinFrame) Geting lock for next frame...->", f.next)
			f.next.mu.Lock()
			log.Println("(pinFrame) Obtained lock for next frame...")
			f.next.prev = f.prev
			f.next.mu.Unlock()
		}
	}

	f.pins += 1
	f.next = nil
	f.prev = nil

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

// Unpins a frame and if no other pins exists,
// returns if it should be added back to LRU
func (f *Frame) UnpinFrame() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.pins <= 0 {
		// already unpinned
		return false, nil
	}

	f.pins -= 1

	if f.pins <= 0 {
		return true, nil
	}

	return false, nil
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
	f.mu.Lock()
	defer f.mu.Unlock()

	f.isDirty = false
}

// Check if a frame's page is marked for deletion.
func (f *Frame) PageIsDead() (bool, error) {
	log.Println("(PageIsDead) Acquiring frame lock...")
	f.mu.RLock()
	log.Println("(PageIsDead) Acquired frame lock...")
	defer f.mu.RUnlock()
	if f.page == nil {
		return false, BufferManagerError{Message: "No page associated with frame."}
	}

	// d := f.page.IsDeleted()
	d := f.isDeleted

	return d, nil
}

// marks a frame and associated page as dead(to be deleted)
func (f *Frame) MarkAsDead() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.isDeleted = true

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
	pLsn := f.page.Header.GetLSN()
	log.Println("(GetLSN) PAGE LSN ==> ", pLsn)

	return f.lsn, nil
}
