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
