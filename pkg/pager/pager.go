/*
The pager is responsible for managing page layout, page header and
page metadata.

If provides an interface to modify page byte content and control concurrent
access.

It manages metadata such as root page Id, number of flushed pages, max page Id.
It maps page IDs to offsets in data file and issues read and write requests
to the disk manager.

It also maintains a free list of available pageIds that may have been deleted
and need recycling.
*/

package pager

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
)

const (
	DEGREE                   = 2
	ORDER                    = DEGREE * 2
	PAGE_SIZE_BYTES          = 8192
	HEADER_SIZE_BYTES        = 51
	METADATA_PAGE_SIZE_BYTES = 8192 //20
	LOWER_PADDING_BYTES      = 16
	CELL_POINTER_SIZE_BYTE   = 5
	CELL_KEY_SIZE_BYTES      = 4
	CELL_VAL_SIZE_BYTES      = 4
	CELL_CHILD_PAGEID_SIZE   = 4
	LSN_SIZE_BYTE            = 12
)

// Page Header Flag bits
const (
	Dead int = iota + 4
	Dirty
	Written
	IsInternal
)

type Pager struct {
	// free list
	freeList *FreeList
	// disk manager
	dManager *diskmanager.DiskManager
	// id of the root page
	rootPageId uint32
	// total number of pages
	pageCount uint32
	// max no of pageId issued. pageId increases monotonically. This is
	// used to issue new page Ids.
	maxPageId uint32
	mu        sync.RWMutex
}

// NewPage - initializes a blank page by assigning a pageId and setting a flag
func (pgr *Pager) NewPage(isInternal bool, pge *Page) error {
	var newPgeId uint32
	if n := pgr.freeList.pop(); n < 0 {
		pgr.mu.Lock()
		newPgeId = pgr.maxPageId + 1
		pgr.maxPageId++
		pgr.mu.Unlock()
	} else {
		newPgeId = uint32(n)
	}

	err := pge.initializePage(newPgeId, isInternal)
	if err != nil {
		return err
	}

	return nil
}

// readMetadata get metadata page and reads content into buff
func (pgr *Pager) readMetadata(buff *[]byte) error {
	if len(*buff) != PAGE_SIZE_BYTES {
		return PagerError{Message: fmt.Sprintf("Invalid buffer size. Expected buffer size of %d bytes", PAGE_SIZE_BYTES)}
	}

	errChan := make(chan error)
	err := pgr.dManager.ReadReq(buff, 0, &errChan)
	if err != nil {
		return err
	}

	testTimeout := 200
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

	select {
	case e := <-errChan:
		cancel()
		if e != nil {
			return e
		}
	case <-ctx.Done():
		return PagerError{Message: "timeout reading metadata page."}
	}
	return nil
}

// ReadPage submits a read request to diskmanager which reads page
// content into buff. Access to buff should be controlled to prevent data races.
func (pgr *Pager) ReadPage(pageId uint32, buff *[]byte) error {
	if len(*buff) != PAGE_SIZE_BYTES {
		return PagerError{Message: fmt.Sprintf("Invalid buffer size. Expected buffer size of %d bytes", PAGE_SIZE_BYTES)}
	}

	errChan := make(chan error)
	err := pgr.dManager.ReadReq(buff, pageId, &errChan)
	if err != nil {
		return err
	}

	testTimeout := 200
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

	select {
	case e := <-errChan:
		cancel()
		if e != nil {
			return e
		}
	case <-ctx.Done():
		return PagerError{Message: "timeout reading metadata page."}
	}
	return nil
}

// WritePage submits a write request to diskmanager and writes content of buff
// to disk. Access to buff should be controlled to prevent data races.
// Parameters:
//
//	pageId id of page to flush
//	buff buffer containing page content to write
func (pgr *Pager) WritePage(pageId uint32, buff *[]byte) error {
	if len(*buff) != PAGE_SIZE_BYTES {
		return PagerError{Message: fmt.Sprintf("Invalid buffer size. Expected buffer size of %d bytes", PAGE_SIZE_BYTES)}
	}

	isDead := helpers.BitIsSet(&((*buff)[0]), Dead)

	errChan := make(chan error)
	err := pgr.dManager.WriteReq(pageId*PAGE_SIZE_BYTES, buff, PAGE_SIZE_BYTES, isDead, &errChan)
	if err != nil {
		return err
	}

	testTimeout := 200
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

	select {
	case e := <-errChan:
		cancel()
		if e != nil {
			return e
		}
	case <-ctx.Done():
		return PagerError{Message: "timeout writing metadata page."}
	}

	// if page was marked as dead, add pageid to free list
	if isDead {
		pgr.freeList.add(pageId)
	}
	return nil
}

// FlushMetadata flushes metadata page in buff to disk.
// Parameters:
//
//	buff buffer containing metadata page content
func (pgr *Pager) FlushMetadata(buff *[]byte) error {
	if len(*buff) != PAGE_SIZE_BYTES {
		return PagerError{Message: fmt.Sprintf("Invalid buffer size. Expected buffer size of %d bytes", PAGE_SIZE_BYTES)}
	}

	// update data
	pgr.mu.RLock()
	binary.LittleEndian.PutUint32((*buff)[0:4], pgr.rootPageId)
	binary.LittleEndian.PutUint32((*buff)[12:16], pgr.pageCount)
	binary.LittleEndian.PutUint32((*buff)[16:20], pgr.maxPageId)
	pgr.mu.RUnlock()

	errChan := make(chan error)
	err := pgr.dManager.WriteReq(0, buff, PAGE_SIZE_BYTES, false, &errChan)
	if err != nil {
		return err
	}

	testTimeout := 200
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

	select {
	case e := <-errChan:
		cancel()
		if e != nil {
			return e
		}
	case <-ctx.Done():
		return PagerError{Message: "timeout writing metadata page."}
	}
	return nil
}

// UpdateRootPage updates root page
func (pgr *Pager) UpdateRootPage(pageId uint32) {
	pgr.mu.Lock()
	defer pgr.mu.Unlock()

	pgr.rootPageId = pageId
}

// NewPager creates and returns an instance of pager, or error if any
// Parameters:
//
//	buff	buffer to store metadata page
//
// Returns:
//
//	pager a pointer to a pager
//	e	error if any
func NewPager(buff *[]byte, diskmanagerConfig diskmanager.DiskManagerConfig) (pager *Pager, e error) {
	fl := NewFreeList()
	dm, err := diskmanager.NewDiskManager(diskmanagerConfig)
	if err != nil {
		return nil, err
	}

	// read metadata page
	pgr := &Pager{
		freeList: fl,
		dManager: dm,
	}

	err = pgr.readMetadata(buff)
	if err != nil {
		return nil, err
	}

	pgr.rootPageId = binary.LittleEndian.Uint32((*buff)[0:4])
	pgr.pageCount = binary.LittleEndian.Uint32((*buff)[12:16])
	pgr.maxPageId = binary.LittleEndian.Uint32((*buff)[16:20])

	return pgr, nil
}
