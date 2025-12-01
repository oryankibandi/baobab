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

	"github.com/oryankibandi/baobab/pkg/errors"
	//"github.com/oryankibandi/baobab/pkg/helpers"
)

const (
	DEGREE                   = 2
	ORDER                    = DEGREE * 2
	PAGE_SIZE_BYTES          = 8192
	HEADER_SIZE_BYTES        = 39
	METADATA_PAGE_SIZE_BYTES = 8192 //20
	LOWER_PADDING_BYTES      = 16
	CELL_POINTER_SIZE_BYTE   = 5
	CELL_KEY_SIZE_BYTES      = 4
	CELL_VAL_SIZE_BYTES      = 4
	CELL_CHILD_PAGEID_SIZE   = 4
)

// Page Header Flag bits
const (
	Dead = iota + 4
	Dirty
	Written
	IsInternal
)

var (
	maxPageOffset = 0
)

// var d *DiskManager
// var maxPageID uint32     // Use this to assign a pageID to new Pages.
var FreeSpaceMap []int64 // Simplistic Free Space Map. Contains an array of offsets where data has been deleted. On real-world databases this is a B-Tree structure.
var PgFreeList *FreeList

type PageHeaderFlagPos int

type Cell struct {
	Flags     []byte // 1 Byte
	KeySize   int32  // 4 Bytes
	ValueSize int32  // 4 Bytes
	PageId    int32  // 4 Bytes PageId of child Node
	Key       []byte // key_size Bytes
	Value     []byte // value_size Bytes
}

// Header 31B
type PageHeader struct {
	Flags        byte  // 1 Byte
	PageId       int32 // ID of page. Possibly aligns with number of block/page number on disk
	Items        int32 // No of items (4 Bytes)
	FreeSpace    int32 // Amount of free space in bytes (4 Bytes)
	UpperOffset  int32 //  End of free space
	LowerOffset  int32 //  Begining of free space
	MagicNumber  int32 // Magic Number 4 Bytes
	Checksum     int16 // Checksum 2 Bytes
	RightChild   int32 // Right most pointer for internal nodes
	RightSibling int32 // PageId of the right sibling. 0 if none.
	LeftSibling  int32 // PageId of the left sibling. 0 if none.
	mu           sync.RWMutex
}

type CellPointer struct {
	Flags   []byte // Flags 1 bytes
	offset  int32  // Offset of cell 4 bytes
	CellRef *Cell  // In mem reference to actual cell
}

// Page 8K
type Page struct {
	Header *PageHeader
	//CellPointers []CellPointer
	//Cells        []Cell
	pgeData [PAGE_SIZE_BYTES]byte
	rmu     sync.RWMutex
}

type DiskManager struct {
	// RootNode     *Page
	RootPage     int32 // Root Page ID
	PageCount    int32 // 4 bytes No. of pages
	FlushedPages int32 // No of pages flushed to disk. Default to PageCount on startup
	MaxPageId    int32 // 4 bytes Max Page ID issued monotinically (starts from 1)
	Queues       sync.Map
	fd           *os.File
	wg           sync.WaitGroup
	mu           sync.RWMutex
}

// Disk req for reads and writes
type IOReq struct {
	Read      bool        // Is read request
	PageId    uint32      // ID of page to read
	ReadPage  *chan *Page // if read req, new page read
	Flushed   *chan int32 // Amount of bytes written.
	WritePage *Page       // if write req, page to write
	dManager  *DiskManager
}

type JobQueue struct {
	jobs    []IOReq
	running bool
	mu      sync.Mutex
}

// traverse nodes and set page count, lookupID and maxPageID
func (d *DiskManager) startupTraversal(rootPageId int32) {
	// pgeOff := make([]byte, PAGE_SIZE_BYTES)
	// _, err := d.fd.ReadAt(pgeOff, int64(rootOffset))

	// if err != nil && err != io.EOF {
	//	log.Fatal(fmt.Sprintf("Unable to read offset %d: %v", rootOffset, err.Error()))
	// }

	fmt.Println("READING FROM OFFSET => ", rootPageId)
	// fmt.Println("PAGEID DATA -*-*->", pgeOff)

	// pageChan := make(chan *Page)
	rootPage, err := d.loadPage(rootPageId)

	if err != nil {
		log.Fatal(err.Error())
	}

	//	fmt.Println("LOADED PAGE ========================> ", rootPage)
	//	fmt.Println("LOADED PAGE HEADER =====================> ", rootPage.Header)

	// fmt.Println("PAGE =+=+> ", rootPage)

	// set RootNode
	// d.RootNode = rootPage
	d.RootPage = rootPage.Header.PageId
}

// Create an in-memory Page from an existing on-disk page. Can be run as a goroutine
func (d *DiskManager) loadPage(pageId int32) (*Page, error) {
	// fmt.Println("(loadPage) ROOT NODE ==> ", d.RootNode)
	// if d.RootNode != nil {
	// 	fmt.Println("ROOT NODE HEADER==> ", d.RootNode.Header)
	// }

	//offset, err := LookupTable.GetPageOffset(int(pageId))

	offset := pageId * PAGE_SIZE_BYTES
	//if err != nil {
	//	return nil, DiskioError{Message: "Cannot retrieve page offset"}
	//}

	pageData := make([]byte, PAGE_SIZE_BYTES)

	_, err := d.fd.ReadAt(pageData, int64(offset))

	if err != nil && !errors.Is(err, io.EOF) {
		panic(fmt.Sprintf("Unable to read offset %d: %v", offset, err.Error()))
	}

	fmt.Println("READING FROM OFFSET => ", offset)
	// fmt.Println("PAGEID DATA -*-*->", pageData)
	fmt.Println("PAGE DATA LEN -> ", len(pageData))

	// Page Header items
	pgeHeader := pageData[0:HEADER_SIZE_BYTES]
	flag := int(pgeHeader[0])

	fmt.Println("PGE HEADER ===> ", pgeHeader)
	fmt.Println("MAGIC NO => ", pgeHeader[25:])

	pageID := binary.LittleEndian.Uint32(pgeHeader[1:5])
	itemCount := binary.LittleEndian.Uint32(pgeHeader[5:9])
	upperOffset := binary.LittleEndian.Uint32(pgeHeader[13:17])
	lowerOff := binary.LittleEndian.Uint32(pgeHeader[17:21])
	freeSpace := binary.LittleEndian.Uint32(pgeHeader[9:13])
	checksum := binary.LittleEndian.Uint32(pgeHeader[21:25])
	magicNumber := binary.LittleEndian.Uint16(pgeHeader[25:27])
	rightPtr := binary.LittleEndian.Uint32(pgeHeader[27:31])
	rightChild := binary.LittleEndian.Uint32(pgeHeader[31:35])
	leftChild := binary.LittleEndian.Uint32(pgeHeader[35:])

	h := PageHeader{
		Flags:        pgeHeader[0],
		PageId:       int32(pageID),
		Items:        int32(itemCount),
		FreeSpace:    int32(freeSpace),
		UpperOffset:  int32(upperOffset),
		LowerOffset:  int32(lowerOff),
		MagicNumber:  int32(magicNumber),
		Checksum:     int16(checksum),
		RightChild:   int32(rightPtr),
		RightSibling: int32(rightChild),
		LeftSibling:  int32(leftChild),
	}

	// fmt.Println("HEADER =========================> ", h)

	// isInternal := h.IsSet(7)

	fmt.Println("PAGEID *-*-*-> ", pageData[1:5], pageID)
	fmt.Println("FLAG => ", flag)

	if uint32(d.MaxPageId) < pageID {
		d.MaxPageId = int32(pageID)
	}

	//fmt.Println("LOOKUP TABLE => ", LookupTable)
	fmt.Println("MAX PAGE ID => ", d.MaxPageId)
	fmt.Println("ITEM COUNT => ", itemCount)
	fmt.Println("Upper Offset => ", upperOffset)
	fmt.Println("Lower Offset => ", lowerOff)
	fmt.Println("CELL POINTERS => ")
	fmt.Println("RightPointer ==> ", rightPtr)

	p := Page{
		Header:  &h,
		pgeData: [8192]byte(pageData),
		// CellPointers: pointers,
		// Cells:        cells,
	}

	// fmt.Println("NEW PAGE => ", p)
	pageData = nil

	return &p, nil
}

func (d *DiskManager) flushMetadata() {
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
		panic(fmt.Sprintf("Unable to write root page metadat: ", err.Error()))
	}

	// Version
	_, err = wr.Write([]byte{0, 0, 0, 0})

	if err != nil {
		panic(fmt.Sprintf("Unable to write root page metadata: ", err.Error()))
	}
	// tree height
	_, err = wr.Write([]byte{0, 0, 0, 0})

	if err != nil {
		panic(fmt.Sprintf("Unable to write root page metadat: ", err.Error()))
	}

	// No or pages
	_, err = wr.Write(pageCount)

	if err != nil {
		panic(fmt.Sprintf("Unable to write root page metadat: ", err.Error()))
	}

	// Max page Id
	_, err = wr.Write(maxPageId)

	if err != nil {
		panic(fmt.Sprintf("Unable to write root page metadat: ", err.Error()))
	}

	//	if d.RootNode != nil {
	//		binary.LittleEndian.PutUint32(rootPageId, uint32(d.RootPage))
	//
	//
	//		_, err := d.fd.WriteAt(rootPageId, 0)
	//
	//		if err != nil {
	//			log.Fatal("Unable to update root offset metadata")
	//		}
	//	}

	err = wr.Flush()

	if err != nil {
		panic(fmt.Sprintf("Unable to flush metadata buffer: ", err.Error()))
	}

	if err = d.fd.Sync(); err != nil {
		panic(err.Error())
	}
}

// Creates write request for `page` and adds it to queue
func (d *DiskManager) WriteReq(page *Page, written *chan int32) error {
	if page == nil {
		*written <- -1
		return DiskioError{Message: "Page is required"}
	}

	writeReq := IOReq{
		Read:      false,
		PageId:    uint32(page.Header.PageId),
		Flushed:   written,
		WritePage: page,
		dManager:  d,
	}

	// Check queue
	// d.mu.RLock()
	q, ok := d.Queues.Load(uint32(page.Header.PageId))
	// d.mu.RUnlock()

	if !ok {
		// Create queue
		jQ := newJobQueue()
		// d.mu.Lock()
		d.Queues.Store(uint32(page.Header.PageId), jQ)
		// d.mu.Unlock()

		jQ.addJob(writeReq)

		return nil
	}

	q.(*JobQueue).addJob(writeReq)

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
	if p == nil {
		return DiskioError{Message: "Page output channel is required."}
	}

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
		jQ := newJobQueue()
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
	PgFreeList.close()
	d.mu.Lock()
	err := d.fd.Close()

	if err != nil {
		panic(err.Error())
	}

	d.mu.Unlock()

	fmt.Println("Closed data file descriptors")
}

// Create a new Page. Requires at least two keys and values/pointers. Key should be sorted in a lexicographical order
func (d *DiskManager) NewPage(keys [][]byte, values *([][]byte), childPageIds *[]int32, setAsRoot bool) (int32, *Page, error) {
	fmt.Println("(NEW) KEYS ==> ", keys)
	if ((values == nil || len(*values) <= 0) && (len(keys) < DEGREE-1 || len(keys) > ORDER-1)) && !setAsRoot {
		return 0, nil, btreeerrors.BTreeError{Message: fmt.Sprintf("Atleast %d keys are required.\n", DEGREE)}
	}

	if ((childPageIds == nil || len(*childPageIds) <= 0) && (len(keys) < DEGREE || len(keys) > ORDER)) && !setAsRoot {
		return 0, nil, btreeerrors.BTreeError{Message: fmt.Sprintf("Atleast %d keys are required.\n", DEGREE)}
	}

	if values == nil && childPageIds == nil {
		return 0, nil, DiskioError{Message: "Insufficient input parameters: Either page IDs or values are required to create a new page."}
	}

	log.Printf("VALUES ==> %v\n", values)
	log.Printf("CHILD PAGE IDS ==> %v\n", childPageIds)

	if (values != nil && len(*values) <= 0) && len(*childPageIds) <= 0 {
		return 0, nil, btreeerrors.BTreeError{Message: fmt.Sprintf("Atleast %d values or pageIds are required.\n", ORDER)}
	}

	fmt.Println("GETTING PAGE..")

	var rightPtr int32

	d.mu.Lock()
	fmt.Println("(NEW) ROOT NODE ==> ", d.RootPage)

	if d.RootPage != 0 {
		fmt.Println("(NEW) ROOT NODE PAGE ID ==> ", d.RootPage)
	}

	var newPageId int32
	newPageId = PgFreeList.pop()

	if newPageId <= 0 {
		newPageId = d.MaxPageId + 1
		d.MaxPageId = newPageId
	}

	h := PageHeader{
		Flags:       byte(32), // 0010000
		PageId:      newPageId,
		Items:       0,
		FreeSpace:   PAGE_SIZE_BYTES - HEADER_SIZE_BYTES,
		UpperOffset: PAGE_SIZE_BYTES - LOWER_PADDING_BYTES,
		LowerOffset: HEADER_SIZE_BYTES,
		RightChild:  rightPtr,
	}

	// If internal node, set flag
	if values == nil || len(*values) <= 0 {
		h.setFlag(IsInternal)
	}

	// new page
	p := Page{
		Header:  &h,
		pgeData: [PAGE_SIZE_BYTES]byte{},
	}

	// fmt.Println("NEW PAGE ==> ", p)
	// fmt.Println("NEW PAGE HEADER ==> ", p.Header)
	// return &p, nil

	// set right ptr
	if childPageIds != nil && len(*childPageIds) > 0 {
		p.Header.UpdateRightPtr((*childPageIds)[len(*childPageIds)-1])
	}

	// Add page count & offset
	if d.RootPage == 0 && d.PageCount <= 0 {
		d.RootPage = p.Header.PageId
	}

	// if new root, set as root node
	// if setAsRoot {
	// 	d.SetAsRoot(p.Header.PageId)
	// 	// d.RootPage = p.Header.PageId
	// }

	// p.flush(0, false)
	// p.flushMany(false)

	d.PageCount += 1
	d.mu.Unlock()

	d.flushMetadata() // TODO: Perform this op concurrently
	fmt.Println("PAGE COUNT --------> ", d.PageCount)
	fmt.Println("MAX PAGE ID ----------> ", d.MaxPageId)
	fmt.Println("ROOT PAGE ID ==> ", d.RootPage)
	fmt.Println("NEW PAGE PAGEID ---------------> ", p.Header.PageId)
	return newPageId, &p, nil
}

func (d *DiskManager) CheckRootPageId() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	root := d.RootPage

	return uint32(root)
}

// Updates the right most pointer
func (h *PageHeader) UpdateRightPtr(pageId int32) error {
	if pageId == 0 {
		return nil
	}

	// check if is internal node
	if !h.IsSet(IsInternal) {
		return DiskioError{Message: "Invalid node: Only internal nodes can have right pointer in header."}
	}

	h.RightChild = pageId

	return nil
}

// Returns right and left siblings respectively
func (h *PageHeader) GetSiblngs() (rightSibling int32, leftSibling int32) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	rSib := h.RightSibling
	lSib := h.LeftSibling

	return rSib, lSib
}

// // Insert cells in new page
//
//	func (p *Page) insertCells(keys [][]byte, vals *([][]byte), pageIds *[]int32) error {
//		fmt.Println("(INSERT CELL) KEYS => ", keys)
//		if vals == nil && pageIds == nil {
//			return btreeerrors.BTreeError{Message: "Values or pageIds required"}
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
	p.Header.setFlag(Dead)

	return nil
}

func (d *DiskManager) SetAsRoot(pageId int32) error {
	d.mu.Lock()

	// d.RootNode = p
	d.RootPage = pageId

	d.mu.Unlock()
	go d.flushMetadata()

	return nil

}

// Check if page is marked for deletion
func (p *Page) IsDeleted() bool {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	d := p.Header.IsSet(Dead)

	return d
}

// Synchronizes keys, values and  page IDs in node to items in Page
func (p *Page) Sync(keys [][]byte, vals [][]byte, pageIds []int32, rightSibling uint32, leftSibling uint32) error {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	fmt.Println("(Sync) IN SYNC ==> ")
	fmt.Println("(Sync) PAGE ID ==> ", p.Header.PageId)
	fmt.Println("(Sync) KEYS ==> ", keys)
	fmt.Println("(Sync) VALS ==> ", vals)
	fmt.Println("(Sync) IN CHILDREN ==> ", pageIds)
	fmt.Println("(Sync) RIGHT CHILD ==> ", rightSibling)
	fmt.Println("(Sync) LEFT CHILD ==> ", leftSibling)

	if p.Header.IsSet(Dead) {
		// Dead page, scheduled for deletion
		fmt.Println("DEAD PAGE, SCHEDULED FOR DELETION.....")
		// Mark page as dirty
		p.Header.setFlag(Dirty)
		return nil
	}

	isInternal := len(pageIds) > 0

	data := [PAGE_SIZE_BYTES]byte{}
	var keySize int
	var valSize int
	var cellPtrWriteOff int
	cellPtrData := make([]byte, CELL_POINTER_SIZE_BYTE)
	startOffs := PAGE_SIZE_BYTES - LOWER_PADDING_BYTES

	if isInternal {
		//

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
			copy(data[cellPtrWriteOff:cellPtrWriteOff+CELL_POINTER_SIZE_BYTE], cellPtrData)
			copy(data[off:int(off)+len(cellData)], cellData)

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
			copy(data[cellPtrWriteOff:cellPtrWriteOff+CELL_POINTER_SIZE_BYTE], cellPtrData)
			copy(data[off:int(off)+len(cellData)], cellData)

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
	copy(data[:HEADER_SIZE_BYTES], p.Header.toBytes())

	p.pgeData = data

	return nil
}

// Flush page content to disk(do not call sync())
func (d *DiskManager) flushPage(p *Page, b chan int32) {
	p.rmu.Lock()
	defer p.rmu.Unlock()

	// Unmark as dirty
	p.Header.unsetFlag(Dirty)

	// update page header data
	hdrBytes := p.Header.toBytes()
	copy(p.pgeData[:HEADER_SIZE_BYTES], hdrBytes)

	if p.Header.IsSet(Dead) {
		// page marked for deletion, overwrite with 0s
		var isRoot bool
		p.pgeData = [PAGE_SIZE_BYTES]byte{}

		d.mu.RLock()
		isRoot = d.RootPage == p.Header.PageId
		n, err := d.fd.WriteAt(p.pgeData[:], int64(p.Header.PageId*PAGE_SIZE_BYTES))

		if err != nil {
			panic("Could not write page")
		}

		d.mu.RUnlock()

		d.mu.Lock()

		// if page is root and it's the onlly one, reset root page
		if isRoot && d.PageCount == 1 {
			d.RootPage = 0
			// d.RootNode = nil
		}

		if d.PageCount > 0 {
			d.PageCount -= 1
		}
		d.mu.Unlock()

		// add to free list
		added := PgFreeList.add(uint32(p.Header.PageId))

		if !added {
			panic("Could not add page to freelist")
		}

		fmt.Println("CLEARED PAGE ", p.Header.PageId)

		// send to channel
		b <- int32(n)

		d.flushMetadata()

		return
	}

	// set stored in disk flag and offset to lookup table
	d.incrementFlushedPages()
	// write page to disk
	p.Header.mu.RLock()
	offs := p.Header.PageId * PAGE_SIZE_BYTES
	p.Header.mu.RUnlock()

	fmt.Println("WRITING TO OFFSET: ", offs)

	d.mu.RLock()
	n, err := d.fd.WriteAt(p.pgeData[:], int64(offs))
	d.mu.RUnlock()

	if err != nil {
		panic("Could not write page")
	}

	// Unmark as dirty
	// p.Header.unsetFlag(5)
	// set stored to disk flag
	p.Header.setFlag(Written)
	fmt.Println("flushed page:=> ", p.Header.PageId)
	// send to channel
	b <- int32(n)
}

// Check wheather the page represents an internal node
func (p *Page) IsInternal() (bool, error) {
	if p.Header == nil {
		return false, DiskioError{Message: "No header set for this page"}
	}

	return p.Header.IsSet(IsInternal), nil
}

func (p *Page) IsDirty() (bool, error) {
	if p.Header == nil {
		return false, DiskioError{Message: "No header set for this page"}
	}

	return p.Header.IsSet(Dirty), nil
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

// Convert page header to bytes
func (h *PageHeader) toBytes() []byte {
	h.mu.RLock()
	defer h.mu.RUnlock()

	headerBytes := make([]byte, HEADER_SIZE_BYTES)

	headerBytes[0] = h.Flags
	binary.LittleEndian.PutUint32(headerBytes[1:5], uint32(h.PageId))
	binary.LittleEndian.PutUint32(headerBytes[5:9], uint32(h.Items))
	binary.LittleEndian.PutUint32(headerBytes[9:13], uint32(h.FreeSpace))
	binary.LittleEndian.PutUint32(headerBytes[13:17], uint32(h.UpperOffset))
	binary.LittleEndian.PutUint32(headerBytes[17:21], uint32(h.LowerOffset))
	binary.LittleEndian.PutUint32(headerBytes[21:25], uint32(h.MagicNumber))
	binary.LittleEndian.PutUint16(headerBytes[25:27], uint16(h.Checksum))
	binary.LittleEndian.PutUint32(headerBytes[27:31], uint32(h.RightChild))
	binary.LittleEndian.PutUint32(headerBytes[31:35], uint32(h.RightSibling))
	binary.LittleEndian.PutUint32(headerBytes[35:], uint32(h.LeftSibling))

	fmt.Println("HEADER TO BYTES => ", headerBytes)

	return headerBytes
}

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

func (q *JobQueue) run() {
	q.mu.Lock()
	q.running = true
	q.mu.Unlock()

	var job IOReq
	for {
		q.mu.Lock()
		if len(q.jobs) == 0 {
			q.running = false
			q.mu.Unlock()
			return // all jobs done
		}

		job = q.jobs[0]
		q.jobs = q.jobs[1:]
		q.mu.Unlock()

		job.execute() // execute job
	}
}

func (q *JobQueue) addJob(job IOReq) {
	q.mu.Lock()
	q.jobs = append(q.jobs, job)
	shouldStart := !q.running
	q.mu.Unlock()

	if shouldStart {
		go q.run()
	}
}

// executes queue job
func (r *IOReq) execute() {
	if r.Read {
		// Read from disk, create Page and return that in channel
		p, err := r.dManager.loadPage(int32(r.PageId))

		if err != nil {
			panic(err.Error())
		}

		*(r.ReadPage) <- p
	} else {
		// Write page to disk
		r.dManager.flushPage(r.WritePage, *r.Flushed)
	}
}

func NewDiskManager() *DiskManager {
	fmt.Println("IN INIT()")
	// InitLookupTable()
	PgFreeList = NewFreeList()
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
		fd: fd,
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
	fmt.Println("Root Page ID => ", metadataPage[0:4], rootPgeID)

	fmt.Println("Page Count => ", pageCount)
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

func newJobQueue() *JobQueue {
	return &JobQueue{
		jobs:    make([]IOReq, 0),
		running: false,
	}
}
