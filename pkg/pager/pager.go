/*
The pager is responsible for managing page layout, page headser and
page metadata.

If provides an interface to modify page byte content and control concurrent
access.

It manages metadata such as root page Id, number of  flushed pages, max page Id.
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

// Create a new Page. Requires at least two keys and values/pointers.
// Keys should be sorted in a lexicographical order.
//
// Parameters:
//
//	lsn LSN number of new page
//	keys keys for the page keys
//	values a
//
// Returns:
//
//	pageid pageId of newly created page
//	pagenewly created page
//	error error if any
func (pgr *Pager) NewPage(lsn []byte, keys [][]byte, values [][]byte, childPageIds []int32, setAsRoot bool) (int32, *Page, error) {
	fmt.Println("(NEW) KEYS ==> ", keys)
	// internal node
	if ((len(values) <= 0) && (len(keys) < DEGREE-1 || len(keys) > ORDER-1)) && !setAsRoot {
		return 0, nil, DiskManagerError{Message: fmt.Sprintf("Atleast %d keys are required.\n", DEGREE-1)}
	}

	// leaf node
	if ((len(childPageIds) <= 0) && (len(keys) < DEGREE || len(keys) > ORDER)) && !setAsRoot {
		return 0, nil, DiskManagerError{Message: fmt.Sprintf("Atleast %d keys are required.\n", DEGREE)}
	}

	// check if both page IDs(internal node) and values(leaf nodes) have been provided
	if values == nil && childPageIds == nil {
		return 0, nil, DiskManagerError{Message: "Insufficient input parameters: Either page IDs or values are required to create a new page."}
	}

	log.Printf("VALUES ==> %v\n", values)
	log.Printf("CHILD PAGE IDS ==> %v\n", childPageIds)

	if (values != nil && len(values) <= 0) && len(childPageIds) <= 0 {
		return 0, nil, DiskManagerError{Message: fmt.Sprintf("Atleast %d values or pageIds are required.\n", ORDER)}
	}

	fmt.Println("GETTING PAGE..")

	var rightPtr int32
	var newPageId int32

	d.mu.Lock()
	fmt.Println("(NEW) ROOT NODE ==> ", d.RootPage)

	if d.RootPage != 0 {
		fmt.Println("(NEW) ROOT NODE PAGE ID ==> ", d.RootPage)
	}

	newPageId = d.freeList.pop()
	fmt.Println("(NEW) Returned page id from free list --> ", newPageId)

	if newPageId <= 0 {
		newPageId = d.MaxPageId + 1
		d.MaxPageId = newPageId
	}

	pageByteData := make([]byte, PAGE_SIZE_BYTES)

	// Fill Page Data
	pageByteData[0] = byte(32)
	// page Id
	binary.LittleEndian.PutUint32(pageByteData[1:5], uint32(newPageId))
	// LSN
	if len(lsn) == LSN_SIZE_BYTE {
		copy(pageByteData[5:17], lsn)
	}
	// item count
	binary.LittleEndian.PutUint32(pageByteData[17:21], uint32(len(keys)))
	// lower offset
	binary.LittleEndian.PutUint32(pageByteData[29:33], uint32(HEADER_SIZE_BYTES))
	// Right pointer
	binary.LittleEndian.PutUint32(pageByteData[39:43], uint32(rightPtr))

	// If internal node, set flag
	isInternal := false
	if len(values) == 0 {
		helpers.SetFlag(&pageByteData[0], IsInternal)
		isInternal = true
	}

	// add keys and values to page data
	cData := make([]byte, 13)
	lastOff := PAGE_SIZE_BYTES - LOWER_PADDING_BYTES
	for i, k := range keys {
		// create cell layout then copy cell to page
		binary.LittleEndian.PutUint32(cData[1:5], uint32(len(k)))

		if isInternal {
			binary.LittleEndian.PutUint32(cData[9:13], uint32((childPageIds)[i]))
		} else {
			binary.LittleEndian.PutUint32(cData[5:9], uint32(len((values)[i])))
		}

		cData = append(cData, k...)
		cData = append(cData, (values)[i]...)

		// calculate offset to start writing. cells are written from end of page upwards to lower spots
		lastOff -= len(cData)
		copy(pageByteData[lastOff:(lastOff+len(cData))], cData)
		cData = make([]byte, 13)
	}

	// upper offset
	binary.LittleEndian.PutUint32(pageByteData[25:29], uint32(lastOff))
	// free space
	binary.LittleEndian.PutUint32(pageByteData[21:25], (uint32(lastOff) - HEADER_SIZE_BYTES))

	// new page
	p := Page{
		PageId:  uint32(newPageId),
		pgeData: [PAGE_SIZE_BYTES]byte{},
	}

	// LSN and Flags
	p.Flags = pageByteData[0]
	copy(p.LSN[:], lsn)

	// Add page count & offset
	if d.RootPage == 0 && d.PageCount <= 0 {
		d.RootPage = int32(p.PageId)
	}

	d.PageCount += 1
	d.mu.Unlock()

	return newPageId, &p, nil
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
