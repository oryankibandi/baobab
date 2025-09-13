package diskio

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/oryankibandi/on_disk_btree/pkg/errors"
)

const (
	DEGREE                   = 2
	ORDER                    = DEGREE * 2
	PAGE_SIZE_BYTES          = 8192
	HEADER_SIZE_BYTES        = 27
	METADATA_PAGE_SIZE_BYTES = 20
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

// Cell Layout
// 0		   1		    5                  9             13        + key_size      + value_size
// +------------------------------------------------------------------------------------------------+
// | [bytes] Flags | [int] Key Size | [int] value size | []int pageId | [bytes] key | [bytes] value |
// +------------------------------------------------------------------------------------------------+
type Cell struct {
	Flags     []byte // 1 Byte
	KeySize   int32  // 4 Bytes
	ValueSize int32  // 4 Bytes
	PageId    int32  // 4 Bytes PageId of child Node
	Key       []byte // key_size Bytes
	Value     []byte // value_size Bytes
}

// Header 24B
type PageHeader struct {
	Flags       byte  // 1 Byte
	PageId      int32 // ID of page. Possibly aligns with number of block/page number on disk
	Items       int32 // No of items (4 Bytes)
	FreeSpace   int32 // Amount of free space in bytes (4 Bytes)
	UpperOffset int32 //  End of free space
	LowerOffset int32 //  Begining of free space
	MagicNumber int32 // Magic Number 4 Bytes
	Checksum    int16 // Checksum 2 Bytes
}

type CellPointer struct {
	Flags  []byte // Flags 1 bytes
	offset int32  // Offset of cell 4 bytes
}

// Page Structure(Heap Page)
// +----------------------+  offset 0
// | PageHeaderData       |  (fixed-size metadata)
// +----------------------+  offset 27
// | cell pointer         |  ↓ (one entry per tuple)
// +----------------------+
// | ... free space ...   |
// +----------------------+
// | tuple data ("cells") |  ↑ Data cells
// +----------------------+  offset 8176
// | special space        |  Padding 16 Bytes (rarely used in heap pages)
// +----------------------+  offset 8192

// Page 8K
type Page struct {
	Header       *PageHeader
	CellPointers []CellPointer
	Cells        []Cell
	rmu          sync.Mutex
}

type DiskTree struct {
	RootNode  *Page
	RootPage  int32 // Root Page ID
	PageCount int32 // 4 bytes No. of pages TODO: Add logic to increment and decrement pageCount while creating or deleting pages
	MaxPageId int32 // 4 bytes Max Page ID issued monotinically (starts from 1)
	fd        *os.File
	wg        sync.WaitGroup
	mu        sync.Mutex
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
		fd:        fd,
	}

	// Calculate PageCount
	metadataPage := make([]byte, 20)
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
		fmt.Println("METADATA PAGE => ", metadataPage)

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

	// Get size
	info, err := DiskBTree.fd.Stat()

	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("INFO => ", info)
	// If root is present, traverse

	if rootPgeID != 0 {
		// Create root node
		// Set DiskTree Variable
		// traverse
		DiskBTree.startupTraversal(int32(rootPgeID))

		return
	}

	fmt.Println("Initialized DiskBTree....")
}

//func AssignPage() (int32, error) {
//	fmt.Println("IN ASSIGNPAGE...")
//	if maxPageID != 0 {
//		return int32(maxPageID), nil
//	}
//	// Check the Metadata GetLastPage
//	metadataPage := make([]byte, 20)
//	r, err := DiskBTree.fd.Read(metadataPage)
//
//	if err != nil && !errors.Is(err, io.EOF) {
//		fmt.Println("ERR reading from file: ", err.Error())
//		log.Fatal(err)
//	}
//
//	if r <= 0 {
//		fmt.Println("Read 0 Bytes => ", r)
//		// Set Root page
//		binary.LittleEndian.PutUint32(metadataPage[4:8], uint32(1))
//		for i, v := range []byte("Tree") {
//
//			metadataPage[8+(3-i)] = v
//
//		}
//		_, err = DiskBTree.fd.Write(metadataPage)
//
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		// Flush
//		err = DiskBTree.fd.Sync()
//		return 1, nil
//	}
//
//	fmt.Println("CONTENT ==> ", metadataPage)
//
//	// read root node
//	root := binary.LittleEndian.Uint32(metadataPage[0:4])
//
//	fmt.Println("Root offset => ", metadataPage[0:4], root)
//
//	// If root is present, traverse
//
//	DiskBTree = &DiskTree{
//		RootNode:  nil,
//		RootPage:  0,
//		PageCount: 0,
//	}
//
//	if root != 0 {
//		// Create root node
//		// Set DiskTree Variable
//		// traverse
//
//		return int32(root), nil
//	}
//
//	return 1, nil
//}

// traverse nodes and set page count, lookupID and maxPageID
func (d *DiskTree) startupTraversal(rootOffset int32) {
	// pgeOff := make([]byte, PAGE_SIZE_BYTES)
	// _, err := d.fd.ReadAt(pgeOff, int64(rootOffset))

	// if err != nil && err != io.EOF {
	//	log.Fatal(fmt.Sprintf("Unable to read offset %d: %v", rootOffset, err.Error()))
	// }

	fmt.Println("READING FROM OFFSET => ", rootOffset)
	// fmt.Println("PAGEID DATA -*-*->", pgeOff)

	rootPage, err := d.LoadPage(rootOffset)

	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("PAGE =+=+> ", rootPage)
}

// Create an in-memory Page from an existing on-disk page
func (d *DiskTree) LoadPage(pageId int32) (*Page, error) {
	offset, err := LookupTable.GetPageOffset(int(pageId))

	if err != nil {
		return nil, DiskioError{Message: "Cannot retrieve page offset"}
	}

	pageData := make([]byte, PAGE_SIZE_BYTES)

	_, err = d.fd.ReadAt(pageData, int64(offset))

	if err != nil && !errors.Is(err, io.EOF) {
		log.Fatal(fmt.Sprintf("Unable to read offset %d: %v", offset, err.Error()))
	}

	fmt.Println("READING FROM OFFSET => ", offset)
	fmt.Println("PAGEID DATA -*-*->", pageData)

	// Page Header items
	pgeHeader := pageData[0:27]
	flag := int(pgeHeader[0])

	fmt.Println("PGE HEADER ===> ", pgeHeader)
	fmt.Println("MAGIC NO => ", pgeHeader[25:])

	pageID := binary.LittleEndian.Uint32(pgeHeader[1:5])
	itemCount := binary.LittleEndian.Uint32(pgeHeader[5:9])
	upperOffset := binary.LittleEndian.Uint32(pgeHeader[13:17])
	lowerOff := binary.LittleEndian.Uint32(pgeHeader[17:21])
	freeSpace := binary.LittleEndian.Uint32(pgeHeader[9:13])
	checksum := binary.LittleEndian.Uint32(pgeHeader[21:25])
	magicNumber := binary.LittleEndian.Uint16(pgeHeader[25:])

	h := PageHeader{
		Flags:       pgeHeader[0],
		PageId:      int32(pageID),
		Items:       int32(itemCount),
		FreeSpace:   int32(freeSpace),
		UpperOffset: int32(upperOffset),
		LowerOffset: int32(lowerOff),
		MagicNumber: int32(magicNumber),
		Checksum:    int16(checksum),
	}

	fmt.Println("HEADER =========================> ", h)

	isInternal := h.isSet(7)

	fmt.Println("PAGEID *-*-*-> ", pageData[1:5], pageID)
	fmt.Println("FLAG => ", flag)

	if maxPageID < pageID {
		maxPageID = pageID
	}

	fmt.Println("LOOKUP TABLE => ", LookupTable)
	fmt.Println("MAX PAGE ID => ", maxPageID)
	fmt.Println("ITEM COUNT => ", itemCount)
	fmt.Println("Upper Offset => ", upperOffset)
	fmt.Println("Lower Offset => ", lowerOff)

	pointers := make([]CellPointer, 0)
	cells := make([]Cell, 0)
	// cell pointers
	for i := 0; i < int(itemCount); i++ {
		startOff := HEADER_SIZE_BYTES + (i * CELL_POINTER_SIZE_BYTE)
		endOff := startOff + CELL_POINTER_SIZE_BYTE
		cellPointerData := pageData[startOff:endOff]
		fmt.Println("CELL POINTER DATA ==> ", cellPointerData)
		cellOffset := int32(binary.LittleEndian.Uint32(cellPointerData[1:]))

		cellP := CellPointer{
			Flags:  []byte{cellPointerData[0]},
			offset: cellOffset,
		}

		pointers = append(pointers, cellP)

		// Get cell data
		cellFlag := pageData[cellOffset]
		keySize := binary.LittleEndian.Uint32(pageData[cellOffset+1 : cellOffset+1+CELL_KEY_SIZE_BYTES])
		valSize := binary.LittleEndian.Uint32(pageData[cellOffset+1+CELL_KEY_SIZE_BYTES : cellOffset+1+CELL_KEY_SIZE_BYTES+CELL_VAL_SIZE_BYTES])

		fmt.Println("Key Size: ", keySize)
		fmt.Println("Val Size:: ", valSize)

		var childPageId int32
		if isInternal {
			// Read child page ID
			childPageId = int32(binary.LittleEndian.Uint32(pageData[cellOffset+1+CELL_KEY_SIZE_BYTES+CELL_VAL_SIZE_BYTES : cellOffset+1+CELL_KEY_SIZE_BYTES+CELL_VAL_SIZE_BYTES+CELL_CHILD_PAGEID_SIZE]))
		}

		// Key and Value Offsets
		kO := cellOffset + 1 + CELL_KEY_SIZE_BYTES + CELL_VAL_SIZE_BYTES + CELL_CHILD_PAGEID_SIZE
		vO := kO + int32(keySize)

		// Read key and value
		key := pageData[kO : kO+int32(keySize)]
		fmt.Println("KEY-=-=-==-=-=-=-=-=-=--=-=-=-> ", binary.LittleEndian.Uint32(key))

		val := pageData[vO : vO+int32(valSize)]
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

		cells = append(cells, c)

	}

	p := Page{
		Header:       &h,
		CellPointers: pointers,
		Cells:        cells,
	}

	fmt.Println("NEW PAGE => ", p)

	return &p, nil
}

func (d *DiskTree) flushMetadata() {
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
		binary.LittleEndian.PutUint32(rootPageId, uint32(d.RootPage))
	}

	fmt.Println("ROOT PAGE ID =---------------------> ", rootPageId)
	fmt.Println("MAX PAGE ID =----------------------+> ", maxPageId)
	// write
	//  root page ID
	_, err := wr.Write(rootPageId)

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to write root page metadat: ", err.Error()))
	}

	// Version
	_, err = wr.Write([]byte{0, 0, 0, 0})

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to write root page metadat: ", err.Error()))
	}
	// tree height
	_, err = wr.Write([]byte{0, 0, 0, 0})

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to write root page metadat: ", err.Error()))
	}

	// No or pages
	_, err = wr.Write(pageCount)

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to write root page metadat: ", err.Error()))
	}

	// Max page Id
	_, err = wr.Write(maxPageId)

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to write root page metadat: ", err.Error()))
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
		log.Fatal(fmt.Sprintf("Unable to flush metadata buffer: ", err.Error()))
	}

	if err = d.fd.Sync(); err != nil {
		panic(err)
	}
}

// Create a new Page. Requires at least two keys and values/pointers. Key should be sorted in a lexicographical order
// TODO: Add Pointers
func New[T cmp.Ordered](keys [][]byte, values *([][]byte), pageId *[]int32) (*Page, error) {
	fmt.Println("(NEW) KEYS ==> ", keys)
	if len(keys) < DEGREE {
		return nil, btreeerrors.BTreeError{Message: fmt.Sprintf("Atleast %d keys are required.\n", DEGREE)}
	}

	if len(*values) <= 0 && len(*pageId) <= 0 {
		return nil, btreeerrors.BTreeError{Message: fmt.Sprintf("Atleast %d values or pageIds are required.\n", ORDER)}
	}

	fmt.Println("GETTING PAGE..")
	// pge, err := AssignPage()

	//if err != nil {
	//	log.Fatal(err)
	//}

	// fmt.Println("RETIEVED NEW NODE PAGE -> ", pge)

	h := PageHeader{
		Flags:       byte(32), // 0010000
		PageId:      DiskBTree.MaxPageId + 1,
		Items:       0,
		FreeSpace:   PAGE_SIZE_BYTES - HEADER_SIZE_BYTES,
		UpperOffset: PAGE_SIZE_BYTES - LOWER_PADDING_BYTES,
		LowerOffset: HEADER_SIZE_BYTES,
	}

	// Check if internal node
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
	return &p, nil

	// Create cells and pointer
	p.insertCells(keys, values, pageId)

	// Add page count & offset
	if DiskBTree.RootNode == nil && DiskBTree.PageCount <= 0 {
		DiskBTree.RootNode = &p
		// newOffset := 16 + (p.Header.PageId * DiskBTree.PageCount)
		DiskBTree.RootPage = p.Header.PageId
	}

	// Update max page ID
	if p.Header.PageId > DiskBTree.MaxPageId {
		DiskBTree.MaxPageId = p.Header.PageId
	}

	p.flush()

	DiskBTree.PageCount += 1
	DiskBTree.flushMetadata()
	fmt.Println("PAGE COUNT --------> ", DiskBTree.PageCount)
	fmt.Println("MAX PAGE ID ----------> ", DiskBTree.MaxPageId)
	fmt.Println("ROOT PAGE ID ==> ", DiskBTree.RootPage)
	fmt.Println("NEW PAGE PAGEID ---------------> ", p.Header.PageId)
	return &p, nil
}

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

		if vals != nil {
			vLen = int32(len((*vals)[i]))
		}

		if pageIds != nil {
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
			Flags:  cellFlag,
			offset: off,
		}

		// Update  pointer and cell
		p.CellPointers = append(p.CellPointers, ptr)
		p.Cells = append(p.Cells, cell)

		// Update header items
		p.Header.Items += 1
		p.Header.UpperOffset = off
		p.Header.LowerOffset += 5

		// TODO: Add check for overflow
	}

	return nil
}

func (p *Page) flush() (bool, error) {
	fmt.Println("Flushing content to page: ", p.Header.PageId)
	fmt.Println("PAGE CONTENTS _ > ", *p)

	// set flags
	p.Header.setFlag(5) // has unflushed data flag

	// construct byte content
	byteContent := make([]byte, 0)
	// Insert header and cell offsets
	byteContent = append(byteContent, p.Header.toBytes()...)
	// Insert cell offsets
	for _, v := range p.CellPointers {
		byteContent = append(byteContent, v.toBytes()...)
	}

	// Calculate cell offsets ((page_size * )tot_cell_size)
	var startOffset int64
	if p.Header.isSet(6) {
		// if already occupies space on disk, get offset from lookup table
		fmt.Println("PAGE IS ALREADY ON DISK")
		offs, err := LookupTable.GetPageOffset(int(p.Header.PageId))

		if err != nil {
			log.Fatal("Invalid offset")
		}

		if offs == 0 {
			log.Fatal("Invalid offset")
		}

	} else {
		// not on disk
		// Check from free space map first

		if len(FreeSpaceMap) > 0 {
			startOffset = FreeSpaceMap[(len(FreeSpaceMap) - 1)]
			FreeSpaceMap = append(FreeSpaceMap[:len(FreeSpaceMap)-1], []int64{}...)
		} else {
			startOffset = METADATA_PAGE_SIZE_BYTES + (int64(DiskBTree.PageCount) * PAGE_SIZE_BYTES)
		}

		// set stored in disk flag and offset to lookup table
		p.Header.setFlag(6)
		LookupTable.AddPageOffset(int(p.Header.PageId), uint32(startOffset))
		// lookupTable[int(p.Header.PageId)] = uint32(startOffset)
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

	cellOffset := (int32(startOffset) + PAGE_SIZE_BYTES) - (LOWER_PADDING_BYTES + size)
	paddingOffset := (int32(startOffset) + PAGE_SIZE_BYTES) - LOWER_PADDING_BYTES
	padding := make([]byte, LOWER_PADDING_BYTES)

	// Cell slice
	cells := make([]byte, 0)
	for _, v := range p.Cells {
		cells = append(append([]byte{}, v.toBytes()...), cells...)
	}

	// write to file concurrently at different offsets. Ensure no overlap

	fmt.Println("BYTE CONTENT => ", byteContent)
	fmt.Println("CELL CONTENT => ", cells)

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

		b, err := DiskBTree.fd.WriteAt(padding, int64(paddingOffset))

		if err != nil {
			fmt.Println("Unable to write padding to page")
			p.Header.unsetFlag(5)
			log.Fatal(err.Error())
		}

		fmt.Printf("Wrote %d bytes\n", b)

	}()

	DiskBTree.wg.Wait()

	DiskBTree.fd.Sync()

	return true, nil
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

// COnvert page header to bytes
func (h *PageHeader) toBytes() []byte {
	headerBytes := make([]byte, HEADER_SIZE_BYTES)

	headerBytes[0] = h.Flags
	binary.LittleEndian.PutUint32(headerBytes[1:5], uint32(h.PageId))
	binary.LittleEndian.PutUint32(headerBytes[5:9], uint32(h.Items))
	binary.LittleEndian.PutUint32(headerBytes[9:13], uint32(h.FreeSpace))
	binary.LittleEndian.PutUint32(headerBytes[13:17], uint32(h.UpperOffset))
	binary.LittleEndian.PutUint32(headerBytes[17:21], uint32(h.LowerOffset))
	binary.LittleEndian.PutUint32(headerBytes[21:25], uint32(h.MagicNumber))
	binary.LittleEndian.PutUint16(headerBytes[25:], uint16(h.Checksum))

	fmt.Println("HEADER TO BYTES => ", headerBytes)

	return headerBytes
}

// set page header flag at provided position(1 - 7)
func (h *PageHeader) setFlag(pos int) {
	var mask byte
	mask = 1 << byte(pos)

	fmt.Println("MASK -> ", mask)

	// set flag (bitwise OR)
	h.Flags |= mask
}

func (h *PageHeader) unsetFlag(pos int) {
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
