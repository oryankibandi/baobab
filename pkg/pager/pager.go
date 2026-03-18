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
	"encoding/binary"
	"sync"

	"github.com/oryankibandi/baobab/pkg/diskmanager"
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
	// id of the root page
	rootPageId uint32
	// total number of pages
	pageCount uint32
	// Number of flushed pages
	flushedPages uint32
	// max no of pageId issued. pageId increases monotonically. This is
	// used to issue new page Ids.
	maxPageId uint32

	// free list
	freeList *FreeList

	// disk manager
	dManager *diskmanager.DiskManager

	mu sync.RWMutex
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
func (d *DiskManager) NewPage(lsn []byte, keys [][]byte, values [][]byte, childPageIds []int32, setAsRoot bool) (int32, *Page, error) {
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

// Generates new page for test use only
func NewTestPage(pageId int32) *Page {
	p := Page{
		LSN:     [LSN_SIZE_BYTE]byte{},
		pgeData: [PAGE_SIZE_BYTES]byte{},
		PageId:  uint32(pageId),
		Flags:   0x32,
	}

	binary.LittleEndian.PutUint32(p.pgeData[1:5], p.PageId)
	return &p
}
