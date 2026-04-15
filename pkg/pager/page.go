package pager

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/oryankibandi/baobab/pkg/helpers"
)

type PageHeaderFlagPos int

// Page struct with byte data
//
//	pgeData - raw byte data
//	rmu - reader writer mutex
//	PageId - Id of page
//
//	8220 bytes, alignment 4
type Page struct {
	// raw byte data 8192 bytes
	pgeData [PAGE_SIZE_BYTES]byte
	// mutex - 24 bytes, alignment 4
	rmu sync.RWMutex
	// PageId/BlockId
	PageId uint32
	_      [4]byte // 4 byte padding for better alignment in Frame{}
}

func (p *Page) UpdateRightPtr(pageId int32) error {
	if pageId == 0 {
		return nil
	}

	// check if is internal node
	if !helpers.BitIsSet(&p.pgeData[0], IsInternal) {
		return PagerError{Message: "Invalid node: Only internal nodes can have right pointer in header."}
	}

	binary.LittleEndian.PutUint32(p.pgeData[39:43], uint32(pageId))

	return nil
}

// marks a page as dead, prepares it for deletion
func (p *Page) MarkAsDead() error {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	helpers.SetFlag(&p.pgeData[0], Dead)

	return nil
}

func (p *Page) MarkDirty() {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	helpers.SetFlag(&p.pgeData[0], Dirty)
}

func (p *Page) MarkClean() {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	helpers.UnsetFlag(&p.pgeData[0], Dirty)
}

// Check if page is marked for deletion
func (p *Page) IsDeleted() bool {
	fmt.Println("(IsDeleted) Acquiring page latch...")
	p.rmu.Lock()
	fmt.Println("(IsDeleted) Acquired page latch...")
	defer p.rmu.Unlock()

	d := helpers.BitIsSet(&p.pgeData[0], Dead)

	return d
}

func (p *Page) UpdateLSN(lsn []byte) error {
	p.rmu.RLock()
	defer p.rmu.RUnlock()
	if lsn == nil {
		return PagerError{Message: "Invalid LSN provided"}
	}

	if len(lsn) != LSN_SIZE_BYTE {
		return PagerError{Message: "LSN size is invalid."}
	}

	copy(p.pgeData[5:5+LSN_SIZE_BYTE], lsn)

	return nil
}

// Retrieves the Log Sequence Number of the page block
func (p *Page) GetLSN() [LSN_SIZE_BYTE]byte {
	p.rmu.RLock()
	defer p.rmu.RUnlock()

	var lsn [LSN_SIZE_BYTE]byte

	copy(lsn[:], p.pgeData[5:5+LSN_SIZE_BYTE])

	return lsn
}

func (p *Page) GetPageByteData() (data *[PAGE_SIZE_BYTES]byte, err error) {
	if p == nil {
		return nil, PagerError{Message: "Page is not set"}
	}

	p.rmu.RLock()
	defer p.rmu.RUnlock()

	return &(p.pgeData), nil
}

// initializePage - sets pageId and internal node flag on a new
// page(8k byte location)
func (p *Page) initializePage(pageId uint32, internal bool) error {
	// PageId 0 reserved for metadata page
	if pageId == 0 {
		return PagerError{Message: "Invalid page id provided"}
	}

	p.rmu.Lock()
	defer p.rmu.Unlock()

	// set page id
	p.PageId = pageId
	binary.LittleEndian.PutUint32(p.pgeData[1:5], pageId)

	// set is internal
	if internal {
		helpers.SetFlag(&p.pgeData[0], IsInternal)
	}

	return nil
}

// resets the  Page details
func (p *Page) Clear() error {
	if p == nil {
		return PagerError{Message: "Page is not set"}
	}

	p.rmu.Lock()
	defer p.rmu.Unlock()

	for i := range PAGE_SIZE_BYTES {
		p.pgeData[i] = 0x00
	}

	return nil
}

// Check whether the page represents an internal node
func (p *Page) IsInternal() (bool, error) {
	p.rmu.RLock()
	defer p.rmu.RUnlock()

	return helpers.BitIsSet(&p.pgeData[0], IsInternal), nil
}

func (p *Page) UpdateUpperOffset(off uint32) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	binary.LittleEndian.PutUint32(p.pgeData[25:29], off)
}

func (p *Page) UpdateLowerOffset(off uint32) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	binary.LittleEndian.PutUint32(p.pgeData[29:33], off)
}

func (p *Page) UpdateItemCount(count int32) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	binary.LittleEndian.PutUint32(p.pgeData[17:21], uint32(count))
}

func (p *Page) UpdateFreeSpace(free int32) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	binary.LittleEndian.PutUint32(p.pgeData[21:25], uint32(free))
}

func (p *Page) UpdateRightSibling(pageId uint32) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	binary.LittleEndian.PutUint32(p.pgeData[43:47], pageId)
}

func (p *Page) UpdateLeftSibling(pageId uint32) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	binary.LittleEndian.PutUint32(p.pgeData[47:51], pageId)
}

func (p *Page) SetLSN(lsn []byte) {
	p.rmu.Lock()
	defer p.rmu.Unlock()

	copy(p.pgeData[5:17], lsn)
}

// swaps page data with provided page p data. Currently only used in tests.
func (p *Page) SwapData(buff *[PAGE_SIZE_BYTES]byte) error {
	if buff == nil {
		return PagerError{Message: "No byte data provided"}
	}
	p.rmu.Lock()
	defer p.rmu.Unlock()

	copy(p.pgeData[:], (buff[:]))

	return nil
}
