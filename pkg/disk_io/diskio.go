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

	"github.com/oryankibandi/on_disk_btree/pkg/errors"
	"github.com/oryankibandi/on_disk_btree/pkg/helpers"
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

var (
	maxPageOffset = 0
)

var DiskBTree *DiskTree
var maxPageID uint32     // Use this to assign a pageID to new Pages.
var FreeSpaceMap []int64 // Simplistic Free Space Map. Contains an array of offsets where data has been deleted. On real-world databases this is a B-Tree structure.

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
	Header       *PageHeader
	CellPointers []CellPointer
	Cells        []Cell
	rmu          sync.Mutex
}

type DiskTree struct {
	RootNode     *Page
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
}

type JobQueue struct {
	jobs    []IOReq
	running bool
	mu      sync.Mutex
}

// traverse nodes and set page count, lookupID and maxPageID
func (d *DiskTree) startupTraversal(rootPageId int32) {
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

	fmt.Println("LOADED PAGE ========================> ", rootPage)
	fmt.Println("LOADED PAGE HEADER =====================> ", rootPage.Header)

	fmt.Println("PAGE =+=+> ", rootPage)

	// set RootNode
	d.RootNode = rootPage
	d.RootPage = rootPage.Header.PageId
}

// Create an in-memory Page from an existing on-disk page. Can be run as a goroutine
func (d *DiskTree) loadPage(pageId int32) (*Page, error) {
	fmt.Println("(loadPage) ROOT NODE ==> ", d.RootNode)
	if d.RootNode != nil {
		fmt.Println("ROOT NODE HEADER==> ", d.RootNode.Header)
	}

	offset, err := LookupTable.GetPageOffset(int(pageId))

	if err != nil {
		return nil, DiskioError{Message: "Cannot retrieve page offset"}
	}

	pageData := make([]byte, PAGE_SIZE_BYTES)

	_, err = d.fd.ReadAt(pageData, int64(offset))

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

	fmt.Println("HEADER =========================> ", h)

	isInternal := h.isSet(7)

	fmt.Println("PAGEID *-*-*-> ", pageData[1:5], pageID)
	fmt.Println("FLAG => ", flag)

	if maxPageID < pageID {
		maxPageID = pageID
	}

	//fmt.Println("LOOKUP TABLE => ", LookupTable)
	fmt.Println("MAX PAGE ID => ", maxPageID)
	fmt.Println("ITEM COUNT => ", itemCount)
	fmt.Println("Upper Offset => ", upperOffset)
	fmt.Println("Lower Offset => ", lowerOff)
	fmt.Println("CELL POINTERS => ")
	fmt.Println("RightPointer ==> ", rightPtr)

	pointers := make([]CellPointer, 0)
	cells := make([]Cell, 0)
	cellPointerData := make([]byte, 0)
	key := make([]byte, 0)
	val := make([]byte, 0)

	// cell pointers
	var startOff int
	var endOff int
	var cellP CellPointer
	var cellFlag byte
	var keySize uint32
	var valSize uint32
	var kO int32
	var vO int32

	for i := range itemCount {
		startOff = HEADER_SIZE_BYTES + (int(i) * CELL_POINTER_SIZE_BYTE)
		endOff = startOff + CELL_POINTER_SIZE_BYTE
		cellPointerData = pageData[startOff:endOff]
		log.Println("CURR ITERATION: ", i)
		fmt.Println("CELL POINTER DATA ==> ", cellPointerData)
		cellOffset := int32(binary.LittleEndian.Uint32(cellPointerData[1:]))

		cellP = CellPointer{
			Flags:  []byte{cellPointerData[0]},
			offset: cellOffset,
		}

		log.Println("CELL OFFSET -> ", cellP)

		//log.Println("******** CELL DATA => ", pageData[cellOffset:(cellOffset+32)])
		log.Println("******** EXPECTED CELL DATA=> ", pageData[cellOffset:])
		// Get cell data
		cellFlag = pageData[cellOffset]
		keySize = binary.LittleEndian.Uint32(pageData[cellOffset+1 : cellOffset+1+CELL_KEY_SIZE_BYTES])
		valSize = binary.LittleEndian.Uint32(pageData[cellOffset+1+CELL_KEY_SIZE_BYTES : cellOffset+1+CELL_KEY_SIZE_BYTES+CELL_VAL_SIZE_BYTES])

		fmt.Println("Key Size: ", keySize)
		fmt.Println("Val Size:: ", valSize)

		var childPageId int32
		if isInternal {
			// Read child page ID
			childPageId = int32(binary.LittleEndian.Uint32(pageData[cellOffset+1+CELL_KEY_SIZE_BYTES+CELL_VAL_SIZE_BYTES : cellOffset+1+CELL_KEY_SIZE_BYTES+CELL_VAL_SIZE_BYTES+CELL_CHILD_PAGEID_SIZE]))
		}

		// Key and Value Offsets
		kO = cellOffset + 1 + CELL_KEY_SIZE_BYTES + CELL_VAL_SIZE_BYTES + CELL_CHILD_PAGEID_SIZE
		vO = kO + int32(keySize)

		// Read key and value
		key = pageData[kO : kO+int32(keySize)]
		log.Println("KEY DATA ===> ", key)
		//fmt.Println("KEY-=-=-==-=-=-=-=-=-=--=-=-=-> ", binary.LittleEndian.Uint32(key))
		fmt.Println("KEY-=-=-==-=-=-=-=-=-=--=-=-=-> ", key)

		val = pageData[vO : vO+int32(valSize)]
		fmt.Println("VAL-=-=-==-=-=-=-=-=-=--=-=-=-> ", string(val))

		// Create Cell
		c := Cell{
			Flags:     []byte{cellFlag},
			KeySize:   int32(keySize),
			ValueSize: int32(valSize),
			PageId:    childPageId,
			Key:       key,
			Value:     val,
		}

		cellP.CellRef = &c

		pointers = append(pointers, cellP)
		cells = append(cells, c)

	}

	p := Page{
		Header:       &h,
		CellPointers: pointers,
		Cells:        cells,
	}

	fmt.Println("NEW PAGE => ", p)
	pageData = nil

	return &p, nil
}

func (d *DiskTree) flushMetadata() {
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

	if d.RootNode != nil {
		fmt.Println("(flushMetadata) ROOT NODE PAGE ID ==--------------------> ", d.RootNode.Header)
		fmt.Println("(flushMetadata) ROOT PAGE ID -=-=-=___________--> ", d.RootPage)
		binary.LittleEndian.PutUint32(rootPageId, uint32(d.RootPage))
	} else {
		fmt.Println("(flushMetadata) ROOTNODE IS NULL ==> ", d.RootNode)
	}

	fmt.Println("(flushMetadata) ROOT PAGE ID =---------------------> ", rootPageId)
	fmt.Println("(flushMetadata) MAX PAGE ID =----------------------+> ", maxPageId)
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
func (d *DiskTree) WriteReq(page *Page, written *chan int32) error {
	if page == nil {
		*written <- -1
		return DiskioError{Message: "Page is required"}
	}

	writeReq := IOReq{
		Read:      false,
		PageId:    uint32(page.Header.PageId),
		Flushed:   written,
		WritePage: page,
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

func (d *DiskTree) forceFlush() {
	if d == nil || d.fd == nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	err := d.fd.Sync()

	if err != nil {
		fmt.Println("Unable to flush to disk")
	}
}

// Creates a read req for `pageId` and adds it to queue
func (d *DiskTree) ReadReq(pageId uint32, p *chan *Page) error {
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

func (d *DiskTree) incrementFlushedPages() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.FlushedPages += 1
}

// safely close file descriptors
func (d *DiskTree) Close() {
	LookupTable.Close()
	d.mu.Lock()
	err := d.fd.Close()

	if err != nil {
		panic(err.Error())
	}

	d.mu.Unlock()
}

// Create a new Page. Requires at least two keys and values/pointers. Key should be sorted in a lexicographical order
func New(keys [][]byte, values *([][]byte), childPageIds *[]int32, setAsRoot bool) (*Page, error) {
	fmt.Println("(NEW) KEYS ==> ", keys)
	if ((values == nil || len(*values) <= 0) && (len(keys) < DEGREE-1 || len(keys) > ORDER-1)) && !setAsRoot {
		return nil, btreeerrors.BTreeError{Message: fmt.Sprintf("Atleast %d keys are required.\n", DEGREE)}
	}

	if ((childPageIds == nil || len(*childPageIds) <= 0) && (len(keys) < DEGREE || len(keys) > ORDER)) && !setAsRoot {
		return nil, btreeerrors.BTreeError{Message: fmt.Sprintf("Atleast %d keys are required.\n", DEGREE)}
	}

	if values == nil && childPageIds == nil {
		return nil, DiskioError{Message: "Insufficient input parameters: Either page IDs or values are required to create a new page."}
	}

	log.Printf("VALUES ==> %v\n", values)
	log.Printf("CHILD PAGE IDS ==> %v\n", childPageIds)

	if (values != nil && len(*values) <= 0) && len(*childPageIds) <= 0 {
		return nil, btreeerrors.BTreeError{Message: fmt.Sprintf("Atleast %d values or pageIds are required.\n", ORDER)}
	}

	fmt.Println("GETTING PAGE..")
	// pge, err := AssignPage()

	//if err != nil {
	//	log.Fatal(err)
	//}

	// fmt.Println("RETIEVED NEW NODE PAGE -> ", pge)
	var rightPtr int32

	DiskBTree.mu.Lock()
	fmt.Println("(NEW) ROOT NODE ==> ", DiskBTree.RootNode)

	if DiskBTree.RootNode != nil {
		fmt.Println("(NEW) ROOT NODE PAGE ID ==> ", DiskBTree.RootNode.Header.PageId)
	}

	newPageId := DiskBTree.MaxPageId + 1
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
		h.setFlag(7)
	}

	// new page
	p := Page{
		Header:       &h,
		CellPointers: make([]CellPointer, 0),
		Cells:        make([]Cell, 0),
	}

	fmt.Println("NEW PAGE ==> ", p)
	fmt.Println("NEW PAGE HEADER ==> ", p.Header)
	// return &p, nil

	// set right ptr
	if childPageIds != nil && len(*childPageIds) > 0 {
		p.Header.UpdateRightPtr((*childPageIds)[len(*childPageIds)-1])
	}

	// Create cells and pointer
	// p.insertCells(keys, values, childPageIds)
	for i, _ := range keys {
		p.insertNewCells(keys, values, childPageIds, i)
	}

	// Add page count & offset
	if DiskBTree.RootNode == nil && DiskBTree.PageCount <= 0 {
		DiskBTree.RootNode = &p
		// newOffset := 16 + (p.Header.PageId * DiskBTree.PageCount)
		DiskBTree.RootPage = p.Header.PageId
	}

	// Update max page ID
	if p.Header.PageId > DiskBTree.MaxPageId {
		fmt.Println("UPDATING MAX PAGE ID +>")
		fmt.Println("CURREMT MAX PAGE ID => ", DiskBTree.MaxPageId)
		fmt.Println("MAX PAGE ID to set => ", p.Header.PageId)
		DiskBTree.MaxPageId = p.Header.PageId
	}

	// if new root, set as root node
	if setAsRoot {
		DiskBTree.RootPage = p.Header.PageId
		DiskBTree.RootNode = &p
	}

	// p.flush(0, false)
	// p.flushMany(false)

	DiskBTree.PageCount += 1
	DiskBTree.mu.Unlock()
	DiskBTree.flushMetadata()
	fmt.Println("PAGE COUNT --------> ", DiskBTree.PageCount)
	fmt.Println("MAX PAGE ID ----------> ", DiskBTree.MaxPageId)
	fmt.Println("ROOT PAGE ID ==> ", DiskBTree.RootPage)
	fmt.Println("NEW PAGE PAGEID ---------------> ", p.Header.PageId)
	return &p, nil
}

// Updates the right most pointer
func (h *PageHeader) UpdateRightPtr(pageId int32) error {
	if pageId == 0 {
		return nil
	}

	// check if is internal node
	if !h.isSet(7) {
		return DiskioError{Message: "Invalid node: Only internal nodes can have right pointer in header."}
	}

	h.RightChild = pageId

	return nil
}

func (p *Page) replaceCells(key []byte, val *[]byte, pageId *int32, idx int) error {
	fmt.Printf("(replaceCells): KEY: %v....VAL: %v\n", key, *val)
	kLen := len(key)

	var vLen int32
	var pgeId int32

	if val != nil {
		vLen = int32(len((*val)))
		p.Cells[idx].ValueSize = vLen
		p.Cells[idx].Value = *val
	}

	if pageId != nil {
		pgeId = *pageId
		p.Cells[idx].PageId = pgeId
	}

	//  update existing cell
	p.Cells[idx].KeySize = int32(kLen)

	// TODO: If new value is larger, delete cell(add to FSM) and move cell to different offset

	return nil
}

// Insert cells in existing page
func (p *Page) insertNewCells(keys [][]byte, vals *[][]byte, pageIds *[]int32, idx int) error {
	flag := make([]byte, 1)
	flag[0] = 0

	keyLen := len(keys[idx])

	var vLen int32
	var pgeId int32
	vl := make([]byte, 0)

	if vals != nil && len(*vals) > 0 {
		vLen = int32(len((*vals)[idx]))

		if len(*vals)-1 >= idx {
			vl = (*vals)[idx]
		}
	}

	if pageIds != nil && len(*pageIds) > 0 {
		pgeId = (*pageIds)[idx]
	}

	newCell := Cell{
		Flags:     flag,
		KeySize:   int32(keyLen),
		ValueSize: vLen,
		Key:       keys[idx],
		Value:     vl,
		PageId:    pgeId,
	}

	// offset = upperoffset - size of cell
	// flag (1) + key_size(4) + value_size(4) + childpageid(4) = 13
	off := p.Header.UpperOffset - (13 + newCell.KeySize + newCell.ValueSize)
	fmt.Println("-------------------PREV UPPFER OFFSET -------------------> ", p.Header.UpperOffset)
	fmt.Println("-------------------CURRCELL SIZE -------------------> ", (13 + newCell.KeySize + newCell.ValueSize))
	fmt.Println("--------------------UPPER OFFSET--------------------------> ", off)
	newCellOffset := CellPointer{
		Flags:   flag,
		offset:  off,
		CellRef: &newCell,
	}

	// Insert new cell offset at idx to maintain lexicographical order
	helpers.InsertToList[CellPointer](&p.CellPointers, idx, newCellOffset)

	// Append cell to existing cells
	p.Cells = append(p.Cells, newCell)

	// update free space offset
	// TODO: Check if the upper offset is used while writing to disk before updating it
	p.Header.UpperOffset = off

	//update item count
	p.Header.Items += 1

	// if internal node, update right most pointer
	//if pageIds != nil && len(*pageIds) > 0 {
	//	p.Header.RightChild = (*pageIds)[len(*pageIds)-1]
	//}

	return nil
}

// Insert cells in new page
func (p *Page) insertCells(keys [][]byte, vals *([][]byte), pageIds *[]int32) error {
	fmt.Println("(INSERT CELL) KEYS => ", keys)
	if vals == nil && pageIds == nil {
		return btreeerrors.BTreeError{Message: "Values or pageIds required"}
	}

	if len(keys) <= 0 || (len(*vals) <= 0 && len(*pageIds) <= 0) {
		return nil
	}

	for i := 0; i < len(keys); i++ {
		kLen := len(keys[i])
		fmt.Println("INSETING KEY SIZE => ", kLen)
		var vLen int32
		var pgeId int32

		if vals != nil && len(*vals) > 0 {
			vLen = int32(len((*vals)[i]))
		}

		if pageIds != nil && len(*pageIds) > 0 {
			pgeId = (*pageIds)[i]
		}

		cellFlag := make([]byte, 1)
		cellFlag[0] = 0

		cell := Cell{
			Flags:     cellFlag,
			KeySize:   int32(kLen),
			ValueSize: vLen,
			Key:       keys[i],
			Value:     (*vals)[i],
			PageId:    pgeId,
		}

		// Calculate cell offset upperoffset - size of cell
		off := p.Header.UpperOffset - (13 + cell.KeySize + cell.ValueSize)

		ptr := CellPointer{
			Flags:   cellFlag,
			offset:  off,
			CellRef: &cell,
		}

		// Update pointer and cell
		p.CellPointers = append(p.CellPointers, ptr)
		p.Cells = append(p.Cells, cell)

		// Update header items
		p.Header.Items += 1
		p.Header.UpperOffset = off
		p.Header.LowerOffset += 5
	}

	// Mark as dirty
	// p.Header.setFlag(5)

	log.Println("(insertCells) KEYS AFTER INSERTING TO PAGE:")
	for i, c := range p.Cells {
		log.Printf("%d: %v\n", i, c.Key)
	}

	return nil
}

// marks a page as dead, prepares it for deletion
func (p *Page) MarkAsDead() error {
	// Add offset to Free Space Map
	off, err := LookupTable.GetPageOffset(int(p.Header.PageId))

	if err != nil {
		return err
	}

	FreeSpaceMap = append(FreeSpaceMap, int64(off))
	// set deleted header
	p.Header.setFlag(4)

	return nil

}

// Synchronizes keys, values and  page IDs in node to items in Page
func (p *Page) Sync(keys [][]byte, vals [][]byte, pageIds []int32, rightSibling uint32, leftSibling uint32) error {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	fmt.Println("(Sync) IN SYNC ==> ")
	fmt.Println("(Sync) KEYS ==> ", keys)
	fmt.Println("(Sync) VALS ==> ", vals)
	fmt.Println("(Sync) IN CHILDREN ==> ", pageIds)

	if p.Header.isSet(4) {
		// Dead page, scheduled for deletion
		fmt.Println("DEAD PAGE, SCHEDULED FOR DELETION.....")
		// Mark page as dirty
		p.Header.setFlag(5)
		return nil
	}

	isInternal := len(pageIds) > 0

	var ptr CellPointer
	cells := make([]Cell, 0)
	cellPtrs := make([]CellPointer, 0)
	startOffs := PAGE_SIZE_BYTES - LOWER_PADDING_BYTES
	if isInternal {
		//

		for i, k := range keys {
			newCell := Cell{
				Flags:     make([]byte, 1),
				KeySize:   int32(len(k)),
				ValueSize: int32(0),
				Key:       k,
				Value:     make([]byte, 0),
				PageId:    pageIds[i],
			}

			cellSize := 13 + int32(len(k))
			off := int32(startOffs) - cellSize

			ptr = CellPointer{
				Flags:   make([]byte, 1),
				offset:  off,
				CellRef: &newCell,
			}

			startOffs = int(off)

			cells = append(cells, newCell)
			cellPtrs = append(cellPtrs, ptr)
		}

		// update right ptr
		p.Header.UpdateRightPtr(pageIds[len(pageIds)-1])
	} else {
		// leaf node
		for i, k := range keys {
			newCell := Cell{
				Flags:     make([]byte, 1),
				KeySize:   int32(len(k)),
				ValueSize: int32(len(vals[i])),
				Key:       k,
				Value:     vals[i],
				PageId:    0,
			}

			cellSize := 13 + int32(len(k)) + int32(len(vals[i]))
			off := int32(startOffs) - cellSize

			ptr = CellPointer{
				Flags:   make([]byte, 1),
				offset:  off,
				CellRef: &newCell,
			}

			startOffs = int(off)

			cells = append(cells, newCell)
			cellPtrs = append(cellPtrs, ptr)
		}
	}

	lowerOff := HEADER_SIZE_BYTES + (CELL_POINTER_SIZE_BYTE * len(keys))

	p.Cells = cells
	p.CellPointers = cellPtrs

	p.Header.updateUpperOffset(uint32(startOffs))
	p.Header.updateLowerOffset(uint32(lowerOff))
	p.Header.updateItemCount(int32(len(cellPtrs)))
	p.Header.updateFreeSpace(int32(startOffs) - int32(lowerOff))
	// update siblings
	p.Header.updateRightSibling(rightSibling)
	p.Header.updateLeftSibling(leftSibling)
	// Mark page as dirty
	p.Header.setFlag(5)

	return nil
}

// Persist page to disk if dirty
func (p *Page) Persist(idx int, replace bool) error {
	if !p.Header.isSet(5) {
		log.Println("Page not dirty. Exiting.")

		return nil
	}

	_, err := p.flush(idx, replace)

	if err != nil {
		return err
	}

	// flush metadata
	// DiskBTree.flushMetadata()

	return nil
}

// FIX: DELETE FUNC
func (p *Page) flush(idx int, replace bool) (bool, error) {
	fmt.Println("Flushing content to page: ", p.Header.PageId)
	fmt.Println("PAGE CONTENTS _ > ", *p)

	// set flags
	// p.Header.setFlag(5) // Should be set when updating in func insertCells

	// Calculate cell offsets ((page_size * )tot_cell_size)
	var startOffset int64
	var firstWrite bool
	if p.Header.isSet(6) {
		firstWrite = false
		// if already occupies space on disk, get offset from lookup table
		fmt.Println("PAGE IS ALREADY ON DISK")
		offs, err := LookupTable.GetPageOffset(int(p.Header.PageId))

		if err != nil {
			log.Fatal("(flush) Invalid offset")
		}

		fmt.Println("RETRIEVED OFFSET FRO LUT: ", offs)

		if offs == 0 {
			log.Fatal("(flush) Invalid offset")
		}

		startOffset = int64(offs)

	} else {
		// not on disk
		// Check from free space map first
		firstWrite = true

		if len(FreeSpaceMap) > 0 {
			// Check for empty space in Free Space Map
			startOffset = FreeSpaceMap[(len(FreeSpaceMap) - 1)]
			FreeSpaceMap = append(FreeSpaceMap[:len(FreeSpaceMap)-1], []int64{}...)
		} else {
			// Write page at end of file
			startOffset = METADATA_PAGE_SIZE_BYTES + (int64(DiskBTree.PageCount) * PAGE_SIZE_BYTES)
		}

		// set stored in disk flag and offset to lookup table
		p.Header.setFlag(6)
		LookupTable.AddPageOffset(int(p.Header.PageId), uint32(startOffset))
		// lookupTable[int(p.Header.PageId)] = uint32(startOffset)
	}

	// construct byte content
	byteContent := make([]byte, 0)
	// Insert header and cell offsets
	byteContent = append(byteContent, p.Header.toBytes()...)
	// Insert cell offsets
	for _, v := range p.CellPointers {
		byteContent = append(byteContent, v.toBytes()...)
	}

	var size int32
	for _, v := range p.Cells {
		fmt.Println("KEY SIZE ==> ", v.KeySize)
		fmt.Println("ACTUAL KEY SIZE => ", len(v.Key))

		fmt.Println("VAL SIZE ==> ", v.ValueSize)
		fmt.Println("ACTUAL VAL SIZE => ", len(v.Value))
		size += (13 + v.KeySize + v.ValueSize)
	}

	fmt.Println("CALCULATED SIZE TO -------> ", size)
	fmt.Println("START OFFSET ===> ", startOffset)

	// cellOffset := (int32(startOffset) + PAGE_SIZE_BYTES) - (LOWER_PADDING_BYTES + size)
	cellOffset := int(startOffset) + int(p.CellPointers[idx].offset)
	paddingOffset := (int32(startOffset) + PAGE_SIZE_BYTES) - LOWER_PADDING_BYTES
	padding := make([]byte, LOWER_PADDING_BYTES)

	// Cell slice
	cells := make([]byte, 0)
	if replace {
		cells = append(append([]byte{}, p.Cells[idx].toBytes()...), cells...)
	} else {
		cells = append(append([]byte{}, p.Cells[len(p.Cells)-1].toBytes()...), cells...)
	}
	//	for _, v := range p.Cells {
	//		cells = append(append([]byte{}, v.toBytes()...), cells...)
	//	}

	// write to file concurrently at different offsets. Ensure no overlap

	fmt.Println("BYTE CONTENT => ", byteContent)
	fmt.Println("CELL CONTENT => ", cells)
	fmt.Println("ALL CELLS => ", p.Cells)

	DiskBTree.wg.Add(3)

	go func() {
		fmt.Println("WRITING HEADER AND CELL OFFSETS......", startOffset)
		defer DiskBTree.wg.Done()
		b, err := DiskBTree.fd.WriteAt(byteContent, startOffset)

		if err != nil {
			fmt.Println("Unable to write page")
			// reset has unflushed data flag
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		fmt.Printf("Wrote %d bytes\n", b)

	}()

	go func() {
		fmt.Println("WRITING CELL DATA AT OFFSET: ......", cellOffset)
		defer DiskBTree.wg.Done()
		b, err := DiskBTree.fd.WriteAt(cells, int64(cellOffset))

		if err != nil {
			fmt.Println("Unable to cells to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		fmt.Printf("Wrote %d bytes\n", b)
	}()

	go func() {
		fmt.Println("WRITING PADDING AT .....", paddingOffset)
		defer DiskBTree.wg.Done()

		if firstWrite {
			b, err := DiskBTree.fd.WriteAt(padding, int64(paddingOffset))

			if err != nil {
				fmt.Println("Unable to write padding to page")
				p.Header.unsetFlag(5)
				log.Fatal(err.Error())
			}

			fmt.Printf("Wrote %d bytes\n", b)
		}

	}()

	DiskBTree.wg.Wait()

	DiskBTree.fd.Sync()

	return true, nil
}

// Flush page content to disk(do not call sync())
func (p *Page) flushPage(b chan int32) {
	// get write offset
	offs, err := LookupTable.GetPageOffset(int(p.Header.PageId))

	if err != nil {
		fmt.Printf("ERR: %v\n", err.Error())
		log.Fatal("(flushPage) Invalid offset")
	}

	if p.Header.isSet(4) {
		if offs <= 0 {
			log.Println("Zero offset on deleted page.")
			b <- int32(0)
			return
		}
		// page marked for deletion, overwrite with 0s
		data := make([]byte, PAGE_SIZE_BYTES)

		n, err := DiskBTree.fd.WriteAt(data, int64(offs))

		if err != nil {
			panic("Could not write page")
		}

		// delete from LUT
		err = LookupTable.DeletePageOffset(int(p.Header.PageId))

		if err != nil {
			panic("Could not clear page from LUT")
		}

		fmt.Println("CLEARED PAGE ", p.Header.PageId)

		// send to channel
		b <- int32(n)

		return
	}

	if offs <= 0 {
		fmt.Errorf("OFFSET IS ZERO or less than zero\n")
		// page not flushed before, need to get new offset
		if len(FreeSpaceMap) > 0 {
			// Check for empty space in Free Space Map
			offs = int32(FreeSpaceMap[(len(FreeSpaceMap) - 1)])
			FreeSpaceMap = append(FreeSpaceMap[:len(FreeSpaceMap)-1], []int64{}...)
		} else {
			offs = int32(METADATA_PAGE_SIZE_BYTES + (int64(DiskBTree.FlushedPages) * PAGE_SIZE_BYTES))
			LookupTable.AddPageOffset(int(p.Header.PageId), uint32(offs))
		}

		// set stored in disk flag and offset to lookup table
		DiskBTree.incrementFlushedPages()
		p.Header.setFlag(6)
	}

	// construct data
	data := make([]byte, 0)

	cellPtrs := make([]byte, 0)

	for _, ptr := range p.CellPointers {
		cellPtrs = append(cellPtrs, ptr.toBytes()...)
	}

	cells := make([]byte, 0)
	for _, c := range p.Cells {
		cells = append(c.toBytes(), cells...)
	}

	// calculate free space upper offset
	upperOff := PAGE_SIZE_BYTES - LOWER_PADDING_BYTES - len(cells)
	lowerOff := HEADER_SIZE_BYTES + len(cellPtrs)

	// update free space offsets
	p.Header.updateLowerOffset(uint32(lowerOff))
	p.Header.updateUpperOffset(uint32(upperOff))
	// Update dirty header
	p.Header.unsetFlag(5)

	header := p.Header.toBytes()
	fmt.Println("HEADER ===> ", header)

	data = append(data, header...)
	data = append(data, cellPtrs...)
	data = append(data, append(make([]byte, (upperOff-lowerOff)), cells...)...)
	data = append(data, make([]byte, LOWER_PADDING_BYTES)...)

	// write page to disk

	fmt.Println("WRITING TO OFFSET: ", offs)
	n, err := DiskBTree.fd.WriteAt(data, int64(offs))

	if err != nil {
		panic("Could not write page")
	}

	fmt.Println("flushed page:=> ", p.Header.PageId)
	// send to channel
	b <- int32(n)
}

func (p *Page) flushMany(replace bool) (bool, error) {
	fmt.Println("Flushing content to page: ", p.Header.PageId)
	fmt.Println("PAGE CONTENTS _ > ", *p)

	// set flags
	// p.Header.setFlag(5) // Should be set when updating in func insertCells

	// Calculate cell offsets ((page_size * )tot_cell_size)
	var startOffset int64
	var firstWrite bool
	if p.Header.isSet(6) {
		firstWrite = false
		// if already occupies space on disk, get offset from lookup table
		fmt.Println("PAGE IS ALREADY ON DISK")
		offs, err := LookupTable.GetPageOffset(int(p.Header.PageId))

		if err != nil {
			log.Fatal("(flushMany) Invalid offset")
		}

		fmt.Println("RETRIEVED OFFSET FRO LUT: ", offs)

		if offs == 0 {
			log.Fatal("(flushMany) (Invalid offset")
		}

		startOffset = int64(offs)

	} else {
		// not on disk
		// Check from free space map first
		firstWrite = true

		if len(FreeSpaceMap) > 0 {
			// Check for empty space in Free Space Map
			startOffset = FreeSpaceMap[(len(FreeSpaceMap) - 1)]
			FreeSpaceMap = append(FreeSpaceMap[:len(FreeSpaceMap)-1], []int64{}...)
		} else {
			// Write page at end of file
			startOffset = METADATA_PAGE_SIZE_BYTES + (int64(DiskBTree.PageCount) * PAGE_SIZE_BYTES)
		}

		// set stored in disk flag and offset to lookup table
		p.Header.setFlag(6)
		LookupTable.AddPageOffset(int(p.Header.PageId), uint32(startOffset))
		// lookupTable[int(p.Header.PageId)] = uint32(startOffset)
	}

	// construct byte content
	byteContent := make([]byte, 0)
	// Insert header and cell offsets
	byteContent = append(byteContent, p.Header.toBytes()...)
	// Insert cell offsets
	for _, v := range p.CellPointers {
		byteContent = append(byteContent, v.toBytes()...)
	}

	var size int32
	for _, v := range p.Cells {
		fmt.Println("KEY SIZE ==> ", v.KeySize)
		fmt.Println("ACTUAL KEY SIZE => ", len(v.Key))

		fmt.Println("VAL SIZE ==> ", v.ValueSize)
		fmt.Println("ACTUAL VAL SIZE => ", len(v.Value))
		size += (13 + v.KeySize + v.ValueSize)
	}

	fmt.Println("CALCULATED SIZE TO -------> ", size)
	fmt.Println("START OFFSET ===> ", startOffset)

	// cellOffset := (int32(startOffset) + PAGE_SIZE_BYTES) - (LOWER_PADDING_BYTES + size)
	cellOffset := int(startOffset) + int(p.CellPointers[len(p.CellPointers)-1].offset)
	paddingOffset := (int32(startOffset) + PAGE_SIZE_BYTES) - LOWER_PADDING_BYTES
	padding := make([]byte, LOWER_PADDING_BYTES)

	// Cell slice
	cells := make([]byte, 0)

	for _, c := range p.Cells {
		cells = append(append([]byte{}, c.toBytes()...), cells...)
	}
	//	for _, v := range p.Cells {
	//		cells = append(append([]byte{}, v.toBytes()...), cells...)
	//	}

	// write to file concurrently at different offsets. Ensure no overlap

	fmt.Println("BYTE CONTENT => ", byteContent)
	fmt.Println("CELL CONTENT => ", cells)
	fmt.Println("ALL CELLS => ", p.Cells)

	DiskBTree.wg.Add(3)

	go func() {
		fmt.Println("WRITING HEADER AND CELL OFFSETS......", startOffset)
		defer DiskBTree.wg.Done()
		b, err := DiskBTree.fd.WriteAt(byteContent, startOffset)

		if err != nil {
			fmt.Println("Unable to write page")
			// reset has unflushed data flag
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		fmt.Printf("Wrote %d bytes\n", b)

	}()

	go func() {
		fmt.Println("WRITING CELL DATA AT OFFSET: ......", cellOffset)
		defer DiskBTree.wg.Done()
		b, err := DiskBTree.fd.WriteAt(cells, int64(cellOffset))

		if err != nil {
			fmt.Println("Unable to cells to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		fmt.Printf("Wrote %d bytes\n", b)
	}()

	go func() {
		fmt.Println("WRITING PADDING AT .....", paddingOffset)
		defer DiskBTree.wg.Done()

		if firstWrite {
			b, err := DiskBTree.fd.WriteAt(padding, int64(paddingOffset))

			if err != nil {
				fmt.Println("Unable to write padding to page")
				p.Header.unsetFlag(5)
				log.Fatal(err.Error())
			}

			fmt.Printf("Wrote %d bytes\n", b)
		}

	}()

	DiskBTree.wg.Wait()

	DiskBTree.fd.Sync()

	return true, nil
}

// Deletes cell referenced by cell ptr at index `cellIdx` and updates cell ptr and page item count
func (p *Page) DeleteCell(cellIdx int) error {
	// 1. Check if cell pointer at offset exists
	// 2. Get the offset
	// 3. Calculate cell size
	// 4. Delete cell - replace with zeros
	// 5. Remove Cell Pointer and rewrite Cell Pointers

	if len(p.CellPointers)-1 < cellIdx {
		return DiskioError{Message: fmt.Sprintf("Index %d out of range: Cell to delete does not exist.", cellIdx)}
	}

	if p.CellPointers[cellIdx].CellRef == nil {
		return DiskioError{Message: "Missing cell ref: No Cell linked to offset."}
	}

	isLastItem := cellIdx == len(p.CellPointers)-1

	cellsOff := p.CellPointers[cellIdx].offset

	cellSize := 13 + p.CellPointers[cellIdx].CellRef.KeySize + p.CellPointers[cellIdx].CellRef.ValueSize
	originalCellPointerSize := len(p.CellPointers) * CELL_POINTER_SIZE_BYTE

	newCellContent := make([]byte, cellSize)
	newCellPtrContent := make([]byte, originalCellPointerSize)

	p.CellPointers = append(p.CellPointers[:cellIdx], p.CellPointers[cellIdx+1:]...)

	pageOffs, err := LookupTable.GetPageOffset(int(p.Header.PageId))

	if err != nil {
		// log.Fatal("Invalid offset")
		return err
	}

	for i, v := range p.CellPointers {
		newCellPtrContent = append(newCellPtrContent[:(i*CELL_POINTER_SIZE_BYTE)], append(v.toBytes(), newCellPtrContent[(i*CELL_POINTER_SIZE_BYTE)+CELL_POINTER_SIZE_BYTE:]...)...)
	}

	// update item count
	if p.Header.Items > 0 {
		p.Header.Items -= 1
	}

	// update free space offset
	if isLastItem {
		p.Header.UpperOffset += cellSize
	}

	// rewrite cell Pointers
	ch := make(chan int, 3)
	go func() {
		b, err := DiskBTree.fd.WriteAt(newCellPtrContent, int64(pageOffs+HEADER_SIZE_BYTES))

		if err != nil {
			fmt.Println("Unable to rewrite cell pointers to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		ch <- b
	}()

	// overwrite cell content
	go func() {
		b, err := DiskBTree.fd.WriteAt(newCellContent, int64(pageOffs)+int64(cellsOff))

		if err != nil {
			fmt.Println("Unable to overwrite cells to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		ch <- b

	}()

	// Update item count in header
	go func() {
		b, err := DiskBTree.fd.WriteAt(binary.LittleEndian.AppendUint32(make([]byte, 0), uint32(p.Header.Items)), int64(pageOffs)+5)

		if err != nil {
			fmt.Println("Unable to overwrite cells to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		// Update offset
		b, err = DiskBTree.fd.WriteAt(binary.LittleEndian.AppendUint32(make([]byte, 0), uint32(p.Header.UpperOffset)), int64(pageOffs)+13)

		if err != nil {
			fmt.Println("Unable to overwrite cells to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		ch <- b
	}()

	// wait for go routines to complete
	<-ch
	<-ch
	<-ch

	fmt.Println("************REWROTE NEW CELLS***************************")

	return nil
}

// deletes cells in an internal node
func (p *Page) DeleteInternalNodeCell(cellIdx int) error {
	// Check if is internal node
	if isInternal, err := p.IsInternal(); err != nil || !isInternal {
		return DiskioError{Message: fmt.Sprintf("Invalid node operation", err.Error())}
	}

	if len(p.CellPointers)-1 < cellIdx {
		return DiskioError{Message: fmt.Sprintf("Index %d out of range: Cell to delete does not exist.", cellIdx)}
	}

	if p.CellPointers[cellIdx].CellRef == nil {
		return DiskioError{Message: "Missing cell ref: No Cell linked to offset."}
	}
	isLastItem := cellIdx == len(p.CellPointers)-1

	oldCellPageId := p.CellPointers[cellIdx].CellRef.PageId

	cellsOff := p.CellPointers[cellIdx].offset

	cellSize := 13 + p.CellPointers[cellIdx].CellRef.KeySize + p.CellPointers[cellIdx].CellRef.ValueSize
	originalCellPointerSize := len(p.CellPointers) * CELL_POINTER_SIZE_BYTE

	newCellContent := make([]byte, cellSize)
	newCellPtrContent := make([]byte, originalCellPointerSize)

	p.CellPointers = append(p.CellPointers[:cellIdx], p.CellPointers[cellIdx+1:]...)

	pageOffs, err := LookupTable.GetPageOffset(int(p.Header.PageId))

	if err != nil {
		// log.Fatal("Invalid offset")
		return err
	}

	for i, v := range p.CellPointers {
		newCellPtrContent = append(newCellPtrContent[:(i*CELL_POINTER_SIZE_BYTE)], append(v.toBytes(), newCellPtrContent[(i*CELL_POINTER_SIZE_BYTE)+CELL_POINTER_SIZE_BYTE:]...)...)
	}

	// update item count
	if p.Header.Items > 0 {
		p.Header.Items -= 1
	}

	// update free space offset
	if isLastItem {
		p.Header.UpperOffset += cellSize
	}

	// Update right ptr
	p.Header.RightChild = oldCellPageId

	ch := make(chan int, 3)

	go func() {
		b, err := DiskBTree.fd.WriteAt(newCellPtrContent, int64(pageOffs+HEADER_SIZE_BYTES))

		if err != nil {
			fmt.Println("Unable to rewrite cell pointers to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		ch <- b
	}()

	// overwrite cell content
	go func() {
		b, err := DiskBTree.fd.WriteAt(newCellContent, int64(pageOffs)+int64(cellsOff))

		if err != nil {
			fmt.Println("Unable to overwrite cells to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		ch <- b

	}()

	// Update item count and right ptr in header
	go func() {
		b, err := DiskBTree.fd.WriteAt(binary.LittleEndian.AppendUint32(make([]byte, 0), uint32(p.Header.Items)), int64(pageOffs)+5)

		if err != nil {
			fmt.Println("Unable to overwrite cells to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		// Write right child
		b, err = DiskBTree.fd.WriteAt(binary.LittleEndian.AppendUint32(make([]byte, 0), uint32(p.Header.RightChild)), int64(pageOffs)+27)

		if err != nil {
			fmt.Println("Unable to update right child")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		// Update offset
		b, err = DiskBTree.fd.WriteAt(binary.LittleEndian.AppendUint32(make([]byte, 0), uint32(p.Header.UpperOffset)), int64(pageOffs)+13)

		if err != nil {
			fmt.Println("Unable to overwrite cells to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		ch <- b
	}()

	// wait for go routines to complete
	<-ch
	<-ch
	<-ch

	return nil
}

// Check wheather the page represents an internal node
func (p *Page) IsInternal() (bool, error) {
	if p.Header == nil {
		return false, DiskioError{Message: "No header set for this page"}
	}

	return p.Header.isSet(7), nil
}

func (p *Page) isDirty() (bool, error) {
	if p.Header == nil {
		return false, DiskioError{Message: "No header set for this page"}
	}

	return p.Header.isSet(5), nil
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
	h.mu.RLock()
	defer h.mu.RUnlock()
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
func (h *PageHeader) isSet(pos int) bool {
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

func (h *PageHeader) markAsDead(pageId uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.setFlag(4)
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
		p, err := DiskBTree.loadPage(int32(r.PageId))

		if err != nil {
			panic(err.Error())
		}

		*(r.ReadPage) <- p
	} else {
		// Write page to disk
		r.WritePage.flushPage(*r.Flushed)
	}
}

func init() {
	fmt.Println("IN INIT()")
	InitLookupTable()

	fd, err := os.OpenFile("data", os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		fmt.Println("ERR while opening file")
		log.Fatal(err)
	}

	DiskBTree = &DiskTree{
		RootNode:  nil,
		RootPage:  0,
		PageCount: 0,
		// Queues:    make(map[uint32]*JobQueue),
		fd: fd,
	}

	// Calculate PageCount
	metadataPage := make([]byte, METADATA_PAGE_SIZE_BYTES)
	r, err := DiskBTree.fd.Read(metadataPage)

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

		_, err = DiskBTree.fd.Write(metadataPage)

		if err != nil {
			log.Fatal(err)
		}

		// Flush
		err = DiskBTree.fd.Sync()

		return
	}

	fmt.Println("CONTENT ==> ", metadataPage)

	// read root node Page ID
	rootPgeID := binary.LittleEndian.Uint32(metadataPage[0:4])

	pageCount := binary.LittleEndian.Uint32(metadataPage[12:16])
	maxPageId := binary.LittleEndian.Uint32(metadataPage[16:])
	fmt.Println("Root Page ID => ", metadataPage[0:4], rootPgeID)

	fmt.Println("Page Count => ", pageCount)
	DiskBTree.PageCount = int32(pageCount)
	DiskBTree.MaxPageId = int32(maxPageId)

	// If root is present, traverse
	if rootPgeID != 0 {
		// Create root node
		// Set DiskTree Variable
		// traverse
		DiskBTree.startupTraversal(int32(rootPgeID))

		return
	}

	fmt.Println("DISKBTREE ROOT NODE => ", DiskBTree.RootNode)
	fmt.Println("Initialized DiskBTree....")
}

func newJobQueue() *JobQueue {
	return &JobQueue{
		jobs:    make([]IOReq, 0),
		running: false,
	}
}
