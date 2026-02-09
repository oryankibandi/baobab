package diskio

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

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

var (
	maxPageOffset = 0
)

// var d *DiskManager
// var maxPageID uint32     // Use this to assign a pageID to new Pages.
// var PgFreeList *FreeList

type PageHeaderFlagPos int

// type Cell struct {
// 	Flags     []byte // 1 Byte
// 	KeySize   int32  // 4 Bytes
// 	ValueSize int32  // 4 Bytes
// 	PageId    int32  // 4 Bytes PageId of child Node
// 	Key       []byte // key_size Bytes
// 	Value     []byte // value_size Bytes
// }

// // Header 31B
// type PageHeader struct {
// 	Flags        byte   // 1 Byte
// 	PageId       int32  // ID of page. Aligns with number of block/page number on disk
// 	LSN          []byte // 12 byte Log Sequence Number of the latest operation modifying the page
// 	Items        int32  // No of items (4 Bytes)
// 	FreeSpace    int32  // Amount of free space in bytes (4 Bytes)
// 	UpperOffset  int32  //  End of free space
// 	LowerOffset  int32  //  Begining of free space
// 	MagicNumber  int32  // Magic Number 4 Bytes
// 	Checksum     int16  // Checksum 2 Bytes
// 	RightChild   int32  // Right most pointer for internal nodes
// 	RightSibling int32  // PageId of the right sibling. 0 if none.
// 	LeftSibling  int32  // PageId of the left sibling. 0 if none.
// 	mu           sync.RWMutex
// }

// Page 8K
type Page struct {
	// PageId/BlockId
	PageId uint32
	// Page flags
	Flags byte
	LSN   [LSN_SIZE_BYTE]byte

	// raw byte data
	pgeData [PAGE_SIZE_BYTES]byte
	rmu     sync.RWMutex
}

type DiskManager struct {
	// Root Page ID
	RootPage int32
	// 4 bytes No. of pages
	PageCount int32
	// No of pages flushed to disk. Default to PageCount on startup
	FlushedPages int32

	// 4 bytes Max Page ID issued monotinically (starts from 1)
	MaxPageId int32

	// Map of job queues for read and write requests {PageId: *JobQueue}
	Queues sync.Map

	//  free list containin available page IDs
	freeList *FreeList

	// database file
	fd *os.File
	wg sync.WaitGroup
	mu sync.RWMutex
}

// Disk req for reads and writes
type IOReq struct {
	Read      bool        // Is read request
	PageId    uint32      // ID of page to read
	ReadPage  *chan *Page // if read req, new page read
	Flushed   chan int32  // Amount of bytes written.
	lsnChan   chan []byte // channel to send back log sequence number of page after flushing. This is used by the background writer to create a checkpoint in WAL
	WritePage *Page       // if write req, page to write
	dManager  *DiskManager
}

type JobQueue struct {
	// PageId/Block Id for which this job is for
	pageId  uint32
	jobs    []IOReq
	running bool
	mu      sync.Mutex
}

// traverse nodes and set page count, lookupID and maxPageID
func (d *DiskManager) startupTraversal(rootPageId int32) {
	fmt.Println("READING FROM OFFSET => ", rootPageId)

	rootPage, err := d.loadPage(rootPageId)

	if err != nil {
		log.Fatal(err.Error())
	}
	d.RootPage = int32(rootPage.PageId)
}

// Create an in-memory Page from an existing on-disk page. Can be run as a goroutine
func (d *DiskManager) loadPage(pageId int32) (*Page, error) {
	offset := pageId * PAGE_SIZE_BYTES

	pageData := make([]byte, PAGE_SIZE_BYTES)

	fmt.Println("(loadPage) Reading at offet -> ", offset)
	_, err := d.fd.ReadAt(pageData, int64(offset))

	if err != nil && !errors.Is(err, io.EOF) {
		panic(fmt.Sprintf("Unable to read offset %d: %v", offset, err.Error()))
	}

	fmt.Println("READING FROM OFFSET => ", offset)
	fmt.Println("PAGE DATA LEN -> ", len(pageData))

	// Page Header items
	pgeHeader := pageData[0:HEADER_SIZE_BYTES]
	flag := int(pgeHeader[0])

	fmt.Println("PGE HEADER ===> ", pgeHeader)

	pageID := binary.LittleEndian.Uint32(pgeHeader[1:5])

	// isInternal := h.IsSet(7)

	fmt.Println("PAGEID *-*-*-> ", pageData[1:5], pageID)
	fmt.Println("FLAG => ", flag)

	if uint32(d.MaxPageId) < pageID {
		d.MaxPageId = int32(pageID)
	}

	p := Page{
		PageId:  pageID,
		Flags:   pgeHeader[0],
		LSN:     [LSN_SIZE_BYTE]byte(pageData[5:17]),
		pgeData: [8192]byte(pageData),
	}

	return &p, nil
}

func (d *DiskManager) FlushMetadata() {
	d.mu.Lock()
	defer d.mu.Unlock()
	// only use as fixed size buffer - default is 4K which is too much
	wr := bufio.NewWriterSize(d.fd, METADATA_PAGE_SIZE_BYTES)

	// Go to beginning of file
	d.fd.Seek(0, 0)

	rootPageId := make([]byte, 4)
	pageCount := make([]byte, 4)
	maxPageId := make([]byte, 4)

	binary.LittleEndian.PutUint32(pageCount, uint32(d.PageCount))
	binary.LittleEndian.PutUint32(maxPageId, uint32(d.MaxPageId))

	if d.RootPage != 0 {
		binary.LittleEndian.PutUint32(rootPageId, uint32(d.RootPage))
	}

	// write
	//  root page ID
	_, err := wr.Write(rootPageId)

	if err != nil {
		panic(fmt.Sprintf("Unable to write root page metadata: %v", err))
	}

	// Version
	_, err = wr.Write([]byte{0, 0, 0, 0})

	if err != nil {
		panic(fmt.Sprintf("Unable to write root page metadata: %v", err))
	}
	// tree height
	_, err = wr.Write([]byte{0, 0, 0, 0})

	if err != nil {
		panic(fmt.Sprintf("Unable to write root page metadata: %v", err))
	}

	// No or pages
	_, err = wr.Write(pageCount)

	if err != nil {
		panic(fmt.Sprintf("Unable to write root page metadata: %v", err))
	}

	// Max page Id
	_, err = wr.Write(maxPageId)

	if err != nil {
		panic(fmt.Sprintf("Unable to write root page metadata: %v", err))
	}

	err = wr.Flush()
	if err != nil {
		panic(fmt.Sprintf("Unable to flush metadata buffer: %v", err))
	}
	if err = d.fd.Sync(); err != nil {
		panic(err.Error())
	}
}

// Creates write request for `page` and adds it to queue
func (d *DiskManager) WriteReq(page *Page, pageId uint32, written chan int32, lsnChan chan []byte) error {
	if page == nil {
		written <- -1
		return DiskioError{Message: "Page is required"}
	}

	writeReq := IOReq{
		Read:      false,
		PageId:    pageId,
		Flushed:   written,
		WritePage: page,
		lsnChan:   lsnChan,
		dManager:  d,
	}

	// Check queue
	// d.mu.RLock()
	q, ok := d.Queues.Load(pageId)
	// d.mu.RUnlock()

	if !ok {
		// Create queue
		jQ := newJobQueue(pageId)
		// d.mu.Lock()
		d.Queues.Store(pageId, jQ)
		// d.mu.Unlock()

		jQ.addJob(writeReq)

		return nil
	}

	q.(*JobQueue).addJob(writeReq)
	fmt.Println("(WriteReq) Job added....")

	return nil
}

func (d *DiskManager) ForceFlush() {
	if d == nil || d.fd == nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	err := d.fd.Sync()

	if err != nil {
		panic(fmt.Sprintf("Unable to flush to disk: %s", err.Error()))
	}
}

// Creates a read req for `pageId` and adds it to queue
func (d *DiskManager) ReadReq(pageId uint32, p *chan *Page) error {
	fmt.Println("(ReadReq) Reading page -> ", pageId)
	if p == nil {
		fmt.Println("(ReadReq) ERROR: CHANNEL IS NIL -> ", p)
		return DiskioError{Message: "Page output channel is required."}
	}

	fmt.Println("(diskmanager) Read Req on page -> ", pageId)

	if pageId == 0 {
		// read metadata page
	}

	rReq := IOReq{
		Read:     true,
		ReadPage: p,
		PageId:   pageId,
		dManager: d,
	}

	q, ok := d.Queues.Load(pageId)

	if !ok {
		// Create queue
		fmt.Println("(diskmanager.ReadReq()) No queue, creating...")
		jQ := newJobQueue(pageId)
		d.Queues.Store(pageId, jQ)

		jQ.addJob(rReq)

		return nil
	}

	q.(*JobQueue).addJob(rReq)

	return nil
}

func (d *DiskManager) incrementFlushedPages() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.FlushedPages += 1
}

// safely close file descriptors
func (d *DiskManager) Close() {
	//LookupTable.Close()
	d.freeList.close()
	d.mu.Lock()
	err := d.fd.Close()

	if err != nil {
		panic(err.Error())
	}

	d.mu.Unlock()

	fmt.Println("Closed data file descriptors")
}

// Create a new Page. Requires at least two keys and values/pointers. Key should be sorted in a lexicographical order
func (d *DiskManager) NewPage(lsn []byte, keys [][]byte, values *([][]byte), childPageIds *[]int32, setAsRoot bool) (int32, *Page, error) {
	fmt.Println("(NEW) KEYS ==> ", keys)
	if ((values == nil || len(*values) <= 0) && (len(keys) < DEGREE-1 || len(keys) > ORDER-1)) && !setAsRoot {
		return 0, nil, DiskioError{Message: fmt.Sprintf("Atleast %d keys are required.\n", DEGREE)}
	}

	if ((childPageIds == nil || len(*childPageIds) <= 0) && (len(keys) < DEGREE || len(keys) > ORDER)) && !setAsRoot {
		return 0, nil, DiskioError{Message: fmt.Sprintf("Atleast %d keys are required.\n", DEGREE)}
	}

	if values == nil && childPageIds == nil {
		return 0, nil, DiskioError{Message: "Insufficient input parameters: Either page IDs or values are required to create a new page."}
	}

	log.Printf("VALUES ==> %v\n", values)
	log.Printf("CHILD PAGE IDS ==> %v\n", childPageIds)

	if (values != nil && len(*values) <= 0) && len(*childPageIds) <= 0 {
		return 0, nil, DiskioError{Message: fmt.Sprintf("Atleast %d values or pageIds are required.\n", ORDER)}
	}

	fmt.Println("GETTING PAGE..")

	var rightPtr int32

	d.mu.Lock()
	fmt.Println("(NEW) ROOT NODE ==> ", d.RootPage)

	if d.RootPage != 0 {
		fmt.Println("(NEW) ROOT NODE PAGE ID ==> ", d.RootPage)
	}

	var newPageId int32
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
	if lsn != nil && len(lsn) == LSN_SIZE_BYTE {
		copy(pageByteData[5:17], lsn)
	}
	// item count
	binary.LittleEndian.PutUint32(pageByteData[17:21], uint32(len(keys)))
	// lower offset
	binary.LittleEndian.PutUint32(pageByteData[29:31], HEADER_SIZE_BYTES)
	// Right pointer
	binary.LittleEndian.PutUint32(pageByteData[39:43], uint32(rightPtr))

	// If internal node, set flag
	isInternal := false
	if values == nil || len(*values) <= 0 {
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
			binary.LittleEndian.PutUint32(cData[9:13], uint32((*childPageIds)[i]))
		} else {
			binary.LittleEndian.PutUint32(cData[5:9], uint32(len((*values)[i])))
		}

		cData = append(cData, k...)
		cData = append(cData, (*values)[i]...)

		// calculate offset to start writing
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

func (d *DiskManager) CheckRootPageId() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	root := d.RootPage

	return uint32(root)
}

// Updates the right most pointer
func (p *Page) UpdateRightPtr(pageId int32) error {
	if pageId == 0 {
		return nil
	}

	// check if is internal node
	if !helpers.BitIsSet(&p.pgeData[0], IsInternal) {
		return DiskioError{Message: "Invalid node: Only internal nodes can have right pointer in header."}
	}

	binary.LittleEndian.PutUint32(p.pgeData[39:43], uint32(pageId))

	return nil
}

// Returns right and left siblings respectively
// func (h *PageHeader) GetSiblngs() (rightSibling int32, leftSibling int32) {
// 	h.mu.RLock()
// 	defer h.mu.RUnlock()
// 	rSib := h.RightSibling
// 	lSib := h.LeftSibling
//
// 	return rSib, lSib
// }

// Resets header details
// func (h *PageHeader) clear() error {
// 	if h == nil {
// 		return DiskioError{Message: "Page Header is nil."}
// 	}
//
// 	h.mu.Lock()
// 	defer h.mu.Unlock()
//
// 	h.Flags = byte(0x00)
// 	h.PageId = 0x00
// 	h.Items = 0x00
// 	h.FreeSpace = 0x00
// 	h.UpperOffset = 0x00
// 	h.LowerOffset = 0x00
// 	h.MagicNumber = 0x00
// 	h.Checksum = 0x00
// 	h.RightChild = 0x00
// 	h.RightSibling = 0x00
// 	h.LeftSibling = 0x00
// 	copy(h.LSN, make([]byte, LSN_SIZE_BYTE))
//
// 	return nil
// }

// // Insert cells in new page
//
//	func (p *Page) insertCells(keys [][]byte, vals *([][]byte), pageIds *[]int32) error {
//		fmt.Println("(INSERT CELL) KEYS => ", keys)
//		if vals == nil && pageIds == nil {
//			return DiskioError{Message: "Values or pageIds required"}
//		}
//
//		if len(keys) <= 0 || (len(*vals) <= 0 && len(*pageIds) <= 0) {
//			return nil
//		}
//
//		for i := 0; i < len(keys); i++ {
//			kLen := len(keys[i])
//			fmt.Println("INSETING KEY SIZE => ", kLen)
//			var vLen int32
//			var pgeId int32
//
//			if vals != nil && len(*vals) > 0 {
//				vLen = int32(len((*vals)[i]))
//			}
//
//			if pageIds != nil && len(*pageIds) > 0 {
//				pgeId = (*pageIds)[i]
//			}
//
//			cellFlag := make([]byte, 1)
//			cellFlag[0] = 0
//
//			cell := Cell{
//				Flags:     cellFlag,
//				KeySize:   int32(kLen),
//				ValueSize: vLen,
//				Key:       keys[i],
//				Value:     (*vals)[i],
//				PageId:    pgeId,
//			}
//
//			// Calculate cell offset upperoffset - size of cell
//			off := p.Header.UpperOffset - (13 + cell.KeySize + cell.ValueSize)
//
//			ptr := CellPointer{
//				Flags:   cellFlag,
//				offset:  off,
//				CellRef: &cell,
//			}
//
//			// Update pointer and cell
//			p.CellPointers = append(p.CellPointers, ptr)
//			p.Cells = append(p.Cells, cell)
//
//			// Update header items
//			p.Header.Items += 1
//			p.Header.UpperOffset = off
//			p.Header.LowerOffset += 5
//		}
//
//		// Mark as dirty
//		// p.Header.setFlag(5)
//
//		log.Println("(insertCells) KEYS AFTER INSERTING TO PAGE:")
//		for i, c := range p.Cells {
//			log.Printf("%d: %v\n", i, c.Key)
//		}
//
//		return nil
//	}
//
// marks a page as dead, prepares it for deletion
func (p *Page) MarkAsDead() error {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	helpers.SetFlag(&p.pgeData[0], Dead)

	return nil
}

func (d *DiskManager) SetAsRoot(pageId int32) error {
	d.mu.Lock()

	// d.RootNode = p
	d.RootPage = pageId

	d.mu.Unlock()
	// go d.FlushMetadata()

	return nil

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
		return DiskioError{Message: "Invalid LSN provided"}
	}

	if len(lsn) != LSN_SIZE_BYTE {
		return DiskioError{Message: "LSN size is invalid."}
	}

	copy(p.LSN[:], lsn)

	return nil
}

// Retrieves the Log Sequence Number of the page block
func (p *Page) GetLSN() [LSN_SIZE_BYTE]byte {
	p.rmu.RLock()
	defer p.rmu.RUnlock()

	var lsn [LSN_SIZE_BYTE]byte

	copy(lsn[:], p.LSN[:])

	return lsn
}

func (p *Page) GetPageByteData() (data *[PAGE_SIZE_BYTES]byte, err error) {
	if p == nil {
		return nil, DiskioError{Message: "Page is not set"}
	}

	p.rmu.RLock()
	defer p.rmu.Unlock()

	return &(p.pgeData), nil
}

func (p *Page) SetPageData(d *[PAGE_SIZE_BYTES]byte) error {
	p.rmu.Lock()
	defer p.rmu.Unlock()

	copy(p.pgeData[:], d[:])

	return nil
}

// Synchronizes keys, values and  page IDs in node to items in Page
func (p *Page) Sync(lsn []byte, keys [][]byte, vals [][]byte, pageIds []int32, rightSibling uint32, leftSibling uint32) error {
	if len(lsn) != LSN_SIZE_BYTE {
		return DiskioError{Message: fmt.Sprintf("Invalid LSN length. Got length %d, expected length %d", len(lsn), LSN_SIZE_BYTE)}
	}

	p.rmu.Lock()
	defer p.rmu.Unlock()
	fmt.Println("(Sync) IN SYNC ==> ")
	fmt.Println("(Sync) PAGE ID ==> ", p.PageId)
	fmt.Println("(Sync) KEYS ==> ", keys)
	fmt.Println("(Sync) VALS ==> ", vals)
	fmt.Println("(Sync) IN CHILDREN ==> ", pageIds)
	fmt.Println("(Sync) RIGHT CHILD ==> ", rightSibling)
	fmt.Println("(Sync) LEFT CHILD ==> ", leftSibling)

	// FIX: Remove debug code below
	isinternal, e := p.IsInternal()

	if e != nil {
		panic(e)
	}

	fmt.Println("(Sync) IS INTERNAL ==> ", isinternal)

	// update LSN
	p.LSN = lsn

	if p.Header.IsSet(Dead) {
		// Dead page, scheduled for deletion
		fmt.Println("DEAD PAGE, SCHEDULED FOR DELETION.....")
		// Mark page as dirty
		p.Header.setFlag(Dirty)
		return nil
	}

	isInternal := len(pageIds) > 0

	var keySize int
	var valSize int
	var cellPtrWriteOff int
	cellPtrData := make([]byte, CELL_POINTER_SIZE_BYTE)
	startOffs := PAGE_SIZE_BYTES - LOWER_PADDING_BYTES

	if isInternal {
		for i, k := range keys {
			keySize = len(k)
			cellSize := 13 + int32(keySize)
			off := int32(startOffs) - cellSize

			cellData := make([]byte, cellSize)
			// populate cell data
			binary.LittleEndian.PutUint32(cellData[1:5], uint32(keySize))
			binary.LittleEndian.PutUint32(cellData[5:9], uint32(0))
			binary.LittleEndian.PutUint32(cellData[9:13], uint32(pageIds[i]))
			copy(cellData[13:(13+keySize)], k)

			// populate cell pointer data
			cellPtrData[0] = 0
			binary.LittleEndian.PutUint32(cellPtrData[1:5], uint32(off))

			// add to page data
			cellPtrWriteOff = HEADER_SIZE_BYTES + (i * CELL_POINTER_SIZE_BYTE)
			copy(p.pgeData[cellPtrWriteOff:cellPtrWriteOff+CELL_POINTER_SIZE_BYTE], cellPtrData)
			copy(p.pgeData[off:int(off)+len(cellData)], cellData)

			startOffs = int(off)

			cellData = nil
		}

		// update right ptr
		p.Header.UpdateRightPtr(pageIds[len(pageIds)-1])
	} else {
		// leaf node
		for i, k := range keys {
			keySize = len(k)
			valSize = len(vals[i])
			cellSize := 13 + int32(len(k)) + int32(len(vals[i]))

			off := int32(startOffs) - cellSize

			cellData := make([]byte, cellSize)

			// populate cell data
			binary.LittleEndian.PutUint32(cellData[1:5], uint32(keySize))
			binary.LittleEndian.PutUint32(cellData[5:9], uint32(valSize))
			binary.LittleEndian.PutUint32(cellData[9:13], uint32(0))
			copy(cellData[13:(13+keySize)], k)
			copy(cellData[13+keySize:(13+keySize+valSize)], vals[i])

			// populate cell pointer data
			cellPtrData[0] = 0
			binary.LittleEndian.PutUint32(cellPtrData[1:5], uint32(off))

			// add cells to pgeData
			cellPtrWriteOff = HEADER_SIZE_BYTES + (i * CELL_POINTER_SIZE_BYTE)
			copy(p.pgeData[cellPtrWriteOff:cellPtrWriteOff+CELL_POINTER_SIZE_BYTE], cellPtrData)
			copy(p.pgeData[off:int(off)+len(cellData)], cellData)

			startOffs = int(off)
			cellData = nil

		}
	}

	lowerOff := HEADER_SIZE_BYTES + (CELL_POINTER_SIZE_BYTE * len(keys))

	p.Header.updateUpperOffset(uint32(startOffs))
	p.Header.updateLowerOffset(uint32(lowerOff))
	p.Header.updateItemCount(int32(len(keys)))
	p.Header.updateFreeSpace(int32(startOffs) - int32(lowerOff))
	// update siblings
	p.Header.updateRightSibling(rightSibling)
	p.Header.updateLeftSibling(leftSibling)
	// Mark page as dirty
	p.Header.setFlag(Dirty)

	// add updated Header
	copy(p.pgeData[:HEADER_SIZE_BYTES], p.Header.toBytes())

	return nil
}

// resets the  Page details
func (p *Page) Clear() error {
	if p == nil {
		return DiskioError{Message: "Page is not set"}
	}

	p.rmu.Lock()
	defer p.rmu.Unlock()

	s := make([]byte, PAGE_SIZE_BYTES)
	copy(p.pgeData[:], s)

	return nil
}

// Flushes page content to disk(does not call sync())
// Sends number of bytes written to channel b
func (d *DiskManager) flushPage(p *Page, b chan int32, lsnChan chan []byte) {
	fmt.Println("(flushPage) Flushing page...")
	p.rmu.Lock()
	defer p.rmu.Unlock()

	fmt.Println("acquired page locks... ")
	// Unmark as dirty
	helpers.UnsetFlag(&p.pgeData[HEADER_SIZE_BYTES], Dirty)

	// retrieve LSN. Already acquired page locks so we can access from the header to avoid deadlocks if we called page.GetLSN()
	seqNo := p.LSN
	// update page header data

	if helpers.BitIsSet(&p.pgeData[HEADER_SIZE_BYTES], Dead) {
		// page marked for deletion, overwrite with 0s
		var isRoot bool
		copy(p.pgeData[:], make([]byte, PAGE_SIZE_BYTES))

		d.mu.Lock()
		isRoot = d.RootPage == int32(p.PageId)
		n, err := d.fd.WriteAt(p.pgeData[:], int64(p.PageId*PAGE_SIZE_BYTES))

		if err != nil {
			panic("Could not write page")
		}

		// if page is root and it's the only one, reset root page
		if isRoot && d.PageCount == 1 {
			d.RootPage = 0
			// d.RootNode = nil
		}

		if d.PageCount > 0 {
			d.PageCount -= 1
		}

		d.mu.Unlock()

		// add to free list
		added := d.freeList.add(uint32(p.PageId))

		if !added {
			panic("Could not add page to freelist")
		}

		fmt.Println("CLEARED PAGE ", p.Header.PageId)

		// send to channel
		b <- int32(n)
		if lsnChan != nil {
			lsnChan <- seqNo
		}

		// d.FlushMetadata()

		return
	}

	// set stored in disk flag and offset to lookup table
	d.incrementFlushedPages()
	// write page to disk
	p.Header.mu.RLock()
	offs := p.Header.PageId * PAGE_SIZE_BYTES
	p.Header.mu.RUnlock()

	fmt.Println("WRITING TO OFFSET: ", offs)

	fmt.Println("ACQURING LOCK ON DISK MANAGER -> ")
	d.mu.RLock()
	fmt.Println("ACQURED LOCK ON DISK MANAGER -> ")
	n, err := d.fd.WriteAt(p.pgeData[:], int64(offs))
	d.mu.RUnlock()

	if err != nil {
		panic("Could not write page")
	}

	fmt.Printf("WRITTEN PAGE %d to DISK\n", p.Header.PageId)

	// Unmark as dirty
	// p.Header.unsetFlag(5)
	// set stored to disk flag
	fmt.Println("SETTING FLAG...")
	p.Header.setFlag(Written)
	fmt.Println("SET FLAG SUCCESSFULLY...")
	// send to channel
	fmt.Println("SENDING BYTES TO CHANNEL....")
	b <- int32(n)
	fmt.Println("SENDING SEQNO TO CHANNEL....")
	fmt.Println("LSNCHAN -> ", lsnChan)
	if lsnChan != nil {
		lsnChan <- seqNo
	}
	fmt.Println("SUCCESSFULLY SEND DATA TO CHANNELS....")

	return
}

// Check whether the page represents an internal node
func (p *Page) IsInternal() (bool, error) {
	p.rmu.RLock()
	defer p.rmu.RUnlock()

	return helpers.BitIsSet(p.pgeData[HEADER_SIZE_BYTES], IsInternal), nil
}

func (p *Page) IsDirty() (bool, error) {
	p.rmu.RLock()
	defer p.rmu.RUnlock()

	return helpers.BitIsSet(p.pgeData[HEADER_SIZE_BYTES], Dirty), nil
}

// returns keys,  values and children from pgeData []byte and error if any
func (p *Page) GetCellData() (keys [][]byte, vals [][]byte, children []int32, rightPtr int32, err error) {
	p.rmu.RLock()
	defer p.rmu.RUnlock()

	p.Header.mu.RLock()
	itemCount := p.Header.Items
	isInternal := p.Header.IsSet(IsInternal)
	rightChild := p.Header.RightChild
	// rightSib := p.Header.RightSibling
	// leftSib := p.Header.LeftSibling
	p.Header.mu.RUnlock()

	var cellOff uint32
	var keySize uint32
	var valSize uint32
	var pageId uint32

	fmt.Println("(GetCellData) ITEM COUNT => ", itemCount)
	k := make([][]byte, itemCount)
	v := make([][]byte, 0)
	child := make([]int32, 0)

	for i := range itemCount {
		// read cell offset data
		cellOff = binary.LittleEndian.Uint32(p.pgeData[HEADER_SIZE_BYTES+(i*CELL_POINTER_SIZE_BYTE)+1 : HEADER_SIZE_BYTES+(i*CELL_POINTER_SIZE_BYTE)+CELL_POINTER_SIZE_BYTE])

		// read cell data
		keySize = binary.LittleEndian.Uint32(p.pgeData[cellOff+1 : cellOff+5])
		valSize = binary.LittleEndian.Uint32(p.pgeData[cellOff+5 : cellOff+9])

		if isInternal {
			pageId = binary.LittleEndian.Uint32(p.pgeData[cellOff+9 : cellOff+13])

			child = append(child, int32(pageId))
		} else {
			val := p.pgeData[cellOff+13+keySize : cellOff+13+keySize+valSize]
			v = append(v, val)
		}

		key := p.pgeData[cellOff+13 : cellOff+13+keySize]
		fmt.Println("(Get Cell data) KEY TO ADD => ", key)
		k[i] = key
		fmt.Println("(Get Cell data) Keys after adding =>", k)

	}

	return k, v, child, rightChild, nil
}

// Convert items in cell to byte array
func (c *Cell) toBytes() []byte {
	cellSize := 13 + c.KeySize + c.ValueSize
	totBytes := make([]byte, cellSize)

	// Flags
	totBytes[0] = c.Flags[0]

	// KeySize, ValueSize, PageId
	binary.LittleEndian.PutUint32(totBytes[1:5], uint32(c.KeySize))
	binary.LittleEndian.PutUint32(totBytes[5:9], uint32(c.ValueSize))
	binary.LittleEndian.PutUint32(totBytes[9:13], uint32(c.PageId))

	placeHolder := append([]byte{}, c.Key...)
	//  totBytes = append(append([]byte{}, totBytes[13:]...), append(placeHolder, totBytes[13+c.KeySize:]...)...)

	totBytes = append(totBytes[:13], append(placeHolder, totBytes[13+c.KeySize:]...)...)

	valPlaceholder := append([]byte{}, c.Value...)
	// totBytes = append(append([]byte{}, totBytes[13+c.KeySize:]...), append(valPlaceholder, totBytes[:13+c.KeySize+c.ValueSize]...)...)

	totBytes = append(totBytes[:13+c.KeySize], append(valPlaceholder, totBytes[13+c.KeySize+c.ValueSize:]...)...)

	fmt.Printf("CELL WITH KEY SIZE %d and VAL SIZE %d : %v\n", c.KeySize, c.ValueSize, totBytes)

	return totBytes
}

func (p *CellPointer) toBytes() []byte {
	totbytes := make([]byte, 5)

	// fmt.Println("FLAG BYTES ===============================================================================> ", p.Flags)
	totbytes[0] = p.Flags[0]

	binary.LittleEndian.PutUint32(totbytes[1:], uint32(p.offset))

	fmt.Println("CEL PONTER BTES ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++> ", totbytes)
	return totbytes
}

// // Convert page header to bytes
// func (h *PageHeader) toBytes() []byte {
// 	h.mu.RLock()
// 	defer h.mu.RUnlock()
//
// 	headerBytes := make([]byte, HEADER_SIZE_BYTES)
//
// 	headerBytes[0] = h.Flags
// 	binary.LittleEndian.PutUint32(headerBytes[1:5], uint32(h.PageId))
// 	copy(headerBytes[5:17], h.LSN)
// 	binary.LittleEndian.PutUint32(headerBytes[17:21], uint32(h.Items))
// 	binary.LittleEndian.PutUint32(headerBytes[21:25], uint32(h.FreeSpace))
// 	binary.LittleEndian.PutUint32(headerBytes[25:29], uint32(h.UpperOffset))
// 	binary.LittleEndian.PutUint32(headerBytes[29:33], uint32(h.LowerOffset))
// 	binary.LittleEndian.PutUint32(headerBytes[33:37], uint32(h.MagicNumber))
// 	binary.LittleEndian.PutUint16(headerBytes[37:39], uint16(h.Checksum))
// 	binary.LittleEndian.PutUint32(headerBytes[39:43], uint32(h.RightChild))
// 	binary.LittleEndian.PutUint32(headerBytes[43:47], uint32(h.RightSibling))
// 	binary.LittleEndian.PutUint32(headerBytes[47:], uint32(h.LeftSibling))
//
// 	fmt.Println("HEADER TO BYTES => ", headerBytes)
//
// 	return headerBytes
// }

// set page header flag at provided position(1 - 7)
func (h *PageHeader) setFlag(pos int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var mask byte
	mask = 1 << byte(pos)

	fmt.Println("MASK -> ", mask)

	// set flag (bitwise OR)
	h.Flags |= mask
}

func (h *PageHeader) unsetFlag(pos int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var mask byte
	mask = 1 << byte(pos)

	// unset flag (^AND)
	h.Flags = h.Flags & (^mask)
}

// Check if flag is et
func (h *PageHeader) IsSet(pos int) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var mask byte
	mask = 1 << byte(pos)

	// Check if set
	r := h.Flags & mask

	return r > 0
}

func (h *PageHeader) updateUpperOffset(off uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.UpperOffset = int32(off)
}

func (h *PageHeader) updateLowerOffset(off uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.LowerOffset = int32(off)
}

func (h *PageHeader) updateItemCount(count int32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Items = count
}

func (h *PageHeader) updateFreeSpace(free int32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.FreeSpace = free
}

func (h *PageHeader) updateRightSibling(pageId uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.RightSibling = int32(pageId)
}

func (h *PageHeader) updateLeftSibling(pageId uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.LeftSibling = int32(pageId)
}

func (h *PageHeader) markAsDead() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.setFlag(Dead)
}

func (h *PageHeader) setLSN(lsn []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.LSN = lsn
}

func (h *PageHeader) getLSN() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	l := h.LSN

	return l
}

func (q *JobQueue) run() {
	fmt.Println("(run) acquiring queue lock() to set running=true...")
	q.mu.Lock()
	fmt.Printf("(run) acquired queue lock() to set running=true on page ID  %d...\n", q.pageId)
	q.running = true
	q.mu.Unlock()

	var job IOReq
	for {
		fmt.Println("(run) acquiring queue lock()...")
		q.mu.Lock()
		fmt.Println("(run) acquired queue lock()...")
		if len(q.jobs) == 0 {
			fmt.Println("(run) exiting run goroutine...")
			q.running = false
			q.mu.Unlock()

			fmt.Println("(run) is running -> ", q.running)
			return // all jobs done
		} else if len(q.jobs) < 0 {
			panic("Invalid state. Length of jobs is negative")
		}

		job = q.jobs[0]
		q.jobs = q.jobs[1:]
		q.mu.Unlock()

		job.execute() // execute job
	}
}

func (q *JobQueue) addJob(job IOReq) {
	fmt.Println("(addJob) adding job to queue")
	fmt.Println("(addJob) Acquiring queue lock....")
	q.mu.Lock()
	fmt.Println("(addJob) Acquired queue lock....")
	q.jobs = append(q.jobs, job)
	shouldStart := !q.running
	q.mu.Unlock()
	fmt.Println("(addJob) Should start running? -> ", shouldStart)

	// FIX: Remove debug code below
	q.mu.Lock()
	fmt.Println("(addJob) Queue running -> ", q.running)
	q.mu.Unlock()

	if shouldStart {
		fmt.Println("Queue not running, starting job...")
		go q.run()
	}
}

// executes queue job
func (r *IOReq) execute() {
	fmt.Println("(execute) diskmanager.execute()...")
	if r.Read {
		// Read from disk, create Page and return that in channel
		fmt.Println("(execute) executing read request...")
		p, err := r.dManager.loadPage(int32(r.PageId))

		if err != nil {
			panic(err.Error())
		}
		fmt.Println("(execute) sending back read page...")

		*(r.ReadPage) <- p
	} else {
		// Write page to disk
		fmt.Printf("(execute) executing write request -> %d...\n", r.PageId)
		r.dManager.flushPage(r.WritePage, r.Flushed, r.lsnChan)
		fmt.Printf("(execute) executed write request -> %d...\n", r.PageId)
	}
	fmt.Println("(execute) diskmanager.execute() DONE.")
}

// Generates new page header for test purpose only
func newTestPageHeader(pgeId int32, rightPtr int32) *PageHeader {
	h := PageHeader{
		Flags:       byte(32), // 0010000
		PageId:      pgeId,
		Items:       0,
		FreeSpace:   PAGE_SIZE_BYTES - HEADER_SIZE_BYTES,
		UpperOffset: PAGE_SIZE_BYTES - LOWER_PADDING_BYTES,
		LowerOffset: HEADER_SIZE_BYTES,
		RightChild:  rightPtr,
	}

	return &h
}

// Generates new page for test use only
func NewTestPage(pageId int32) *Page {
	h := newTestPageHeader(pageId, 0)
	p := Page{
		Header:  h,
		pgeData: [PAGE_SIZE_BYTES]byte{},
	}

	return &p
}

func NewDiskManager() *DiskManager {
	fmt.Println("IN INIT()")
	// InitLookupTable()
	PgFreeList := NewFreeList()
	PgFreeList.loadFreeList()

	fd, err := os.OpenFile("data", os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		fmt.Println("ERR while opening file")
		log.Fatal(err)
	}

	diskManager := &DiskManager{
		// RootNode:  nil,
		RootPage:  0,
		PageCount: 0,
		// Queues:    make(map[uint32]*JobQueue),
		fd:       fd,
		freeList: PgFreeList,
	}

	// Calculate PageCount
	metadataPage := make([]byte, METADATA_PAGE_SIZE_BYTES)
	r, err := diskManager.fd.Read(metadataPage)

	if err != nil && !errors.Is(err, io.EOF) {
		fmt.Println("ERR reading data file: ", err.Error())
		log.Fatal(err)
	}

	if r <= 0 {
		// no metedata page(no data). Create.
		fmt.Println("Read 0 Bytes => ", r)
		// Set Root page
		binary.LittleEndian.PutUint32(metadataPage[:4], uint32(0)) // 0 signifies no root page
		// Version 1
		binary.LittleEndian.PutUint32(metadataPage[4:8], uint32(1))
		// Tree Height
		binary.LittleEndian.PutUint32(metadataPage[8:12], uint32(0))
		// No of pages
		binary.LittleEndian.PutUint32(metadataPage[12:16], uint32(0))
		// Max page ID
		binary.LittleEndian.PutUint32(metadataPage[16:], uint32(0))
		//fmt.Println("METADATA PAGE => ", metadataPage)

		_, err = diskManager.fd.Write(metadataPage)

		if err != nil {
			log.Fatal(err)
		}

		// Flush
		err = diskManager.fd.Sync()

		return diskManager
	}

	// fmt.Println("CONTENT ==> ", metadataPage)

	// read root node Page ID
	rootPgeID := binary.LittleEndian.Uint32(metadataPage[0:4])

	pageCount := binary.LittleEndian.Uint32(metadataPage[12:16])
	maxPageId := binary.LittleEndian.Uint32(metadataPage[16:])
	fmt.Printf("Root Page ID => %b, %d\n", metadataPage[0:4], rootPgeID)

	fmt.Println("Page Count => ", pageCount)
	fmt.Println("Max Page Id => ", maxPageId)
	diskManager.PageCount = int32(pageCount)
	diskManager.MaxPageId = int32(maxPageId)

	// If root is present, traverse
	if rootPgeID != 0 {
		// Create root node
		// Set DiskManager Variable
		// traverse
		diskManager.startupTraversal(int32(rootPgeID))

		return diskManager
	}

	fmt.Println("DISKBTREE ROOT NODE => ", diskManager.RootPage)
	fmt.Println("Initialized d....")

	return diskManager
}

func newJobQueue(pageId uint32) *JobQueue {
	return &JobQueue{
		pageId:  pageId,
		jobs:    make([]IOReq, 0),
		running: false,
	}
}
