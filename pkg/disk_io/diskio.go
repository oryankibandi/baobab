package diskio

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"
)

const (
	DEGREE = 2
	ORDER               = DEGREE*2
	PAGE_SIZE_BYTES     = 8192
	HEADER_SIZE_BYTES   = 26
	LOWER_PADDING_BYTES = 16
)

var (
	maxPageOffset = 0
)

var DiskBTree *DiskTree

// 0		   1		    5                  9             13        + key_size      + value_size
// +------------------------------------------------------------------------------------------------+
// | [bytes] Flags | [int] Key Size | [int] value size | []int pageId | [bytes] key | [bytes] value |
// +------------------------------------------------------------------------------------------------+
type Cell struct {
	Flags     []byte // 1 Byte
	KeySize   int32  // 4 Bytes
	ValueSize int32  // 4 Bytes
	PageId    int32  // 4 Bytes
	Key       []byte // key_size Bytes
	Value     []byte // value_size Bytes
}

// Header 24B
type PageHeader struct {
	PageId      int32 // ID of page. Possibly aligns with number of block/page number on disk
	Items       int32 // No of items (4 Bytes)
	FreeSpace   int32 // Amount of free space in bytes (4 Bytes)
	UpperOffset int32 //  End of free space
	LowerOffset int32 //  Begining of free space
	MagicNumber int32 // Magic Number 4 Bytes
	Checksum    int16 // Checksum 2 Bytes
}

type CellPointer struct {
	Flags  []byte // Flags
	offset int32  // Offset of cell
}

// Page Structure(Heap Page)
// +----------------------+  offset 0
// | PageHeaderData       |  (fixed-size metadata)
// +----------------------+  offset 22
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
}

type DiskTree struct {
	RootNode   *Page
	RootOffset int32
	PageCount  int32
	fd         *os.File
	wg         sync.WaitGroup
}

func AssignPage() (int32, error) {
	fd, err := os.OpenFile("data", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		fmt.Println("ERR while opening file")
		log.Fatal(err)
	}

	fmt.Println("Got File descriptor...")
	defer fd.Close()

	// Check the Metadata GetLastPage
	metadataPage := make([]byte, 16)
	r, err := fd.Read(metadataPage)

	if err != nil && err.Error() != "EOF" {
		fmt.Println("ERR reading from file: ", err.Error())
		log.Fatal(err)
	}

	if r <= 0 {
		fmt.Println("Read 0 Bytes => ", r)
		// Set Root page
		binary.LittleEndian.PutUint32(metadataPage[4:8], uint32(1))
		for i, v := range []byte("Tree") {

			metadataPage[8+(3-i)] = v

		}
		_, err = fd.Write(metadataPage)

		if err != nil {
			log.Fatal(err)
		}

		// Flush
		err = fd.Sync()
		return 0, nil
	}

	fmt.Println("CONTENT ==> ", metadataPage)

	// read root node
	root := binary.LittleEndian.Uint32(metadataPage[0:4])

	fmt.Println("Root offset => ", metadataPage[0:4], root)

	// If root is present, traverse

	DiskBTree = &DiskTree{
		RootNode:   nil,
		RootOffset: 0,
		PageCount:  0,
		fd:         fd,
	}

	if root != 0 {
		// Create root node
		// Set DiskTree Variable
		// traverse
	}

	return 1, nil
}

// Create a new Page. Requires at least two keys and values/pointers. Key should be sorted in a lexicographical order
func New[T cmp.Ordered](keys []byte, values *[]byte, pageId *[]int32) (*Page, error) {
	if len(keys) < 2
	// TODO: get page ID from offset
	// create new page

	fmt.Println("GETTING PAGE..")
	pge, err := AssignPage()

	if err != nil {
		log.Fatal(err)
	}

	h := PageHeader{
		PageId:      pge,
		Items:       0,
		FreeSpace:   PAGE_SIZE_BYTES - HEADER_SIZE_BYTES,
		UpperOffset: PAGE_SIZE_BYTES - LOWER_PADDING_BYTES,
		LowerOffset: HEADER_SIZE_BYTES,
	}

	// new page
	p := Page{
		Header:       &h,
		CellPointers: make([]CellPointer, 0),
		Cells:        make([]Cell, 0),
	}

	fmt.Println("NEW PAGE ==> ", p)

	// Create cells and pointer

	p.flush()
	return &p, nil
}

func (p *Page) flush() (bool, error) {
	fmt.Println("Flushing content to page: ", p.Header.PageId)

	// construct byte content
	byteContent := make([]byte, 0)
	// Insert header and cell offset
	byteContent = append(byteContent, p.Header.toBytes()...)
	// Insert cell offsets
	for _, v := range p.CellPointers {
		byteContent = append(byteContent, v.toBytes()...)
	}

	// Calculate cell offsets ((page_size * )tot_cell_size)
	var size int32
	for _, v := range p.Cells {
		size += 13 + v.KeySize + v.ValueSize
	}

	cellOffset := PAGE_SIZE_BYTES - (LOWER_PADDING_BYTES + size)

	// Cell slice
	cells := make([]byte, 0)
	for _, v := range p.Cells {
		cells = append(append([]byte{}, v.toBytes()...), cells...)
	}

	// write to file concurrently

	go func() {
		DiskBTree.wg.Add(1)
		defer DiskBTree.wg.Done()
		b, err := DiskBTree.fd.WriteAt(byteContent, 0)

		if err != nil {
			fmt.Println("Unable to write page")
			log.Fatal(err.Error())
		}

		fmt.Printf("Wrote %d bytes\n", b)

	}()

	go func() {
		DiskBTree.wg.Add(1)
		defer DiskBTree.wg.Done()
		b, err := DiskBTree.fd.WriteAt(cells, int64(cellOffset))

		if err != nil {
			fmt.Println("Unable to cells to page")
			log.Fatal(err.Error())
		}

		fmt.Printf("Wrote %d bytes\n", b)
	}()

	DiskBTree.wg.Wait()

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
	binary.LittleEndian.PutUint32(totBytes[5:9], uint32(c.KeySize))
	binary.LittleEndian.PutUint32(totBytes[9:13], uint32(c.KeySize))

	placeHolder := append([]byte{}, c.Key...)
	totBytes = append(append([]byte{}, totBytes[13:]...), append(placeHolder, totBytes[13+c.KeySize:]...)...)

	valPlaceholder := append([]byte{}, c.Value...)
	totBytes = append(append([]byte{}, totBytes[13+c.KeySize:]...), append(valPlaceholder, totBytes[13+c.KeySize+c.ValueSize:]...)...)

	return totBytes
}

func (p *CellPointer) toBytes() []byte {
	totbytes := make([]byte, 5)

	totbytes = append(totbytes, p.Flags...)

	binary.LittleEndian.PutUint32(totbytes[1:], uint32(p.offset))

	return totbytes
}

// COnvert page header to bytes
func (h *PageHeader) toBytes() []byte {
	headerBytes := make([]byte, HEADER_SIZE_BYTES)

	binary.LittleEndian.PutUint32(headerBytes[:4], uint32(h.PageId))
	binary.LittleEndian.PutUint32(headerBytes[4:8], uint32(h.Items))
	binary.LittleEndian.PutUint32(headerBytes[8:12], uint32(h.FreeSpace))
	binary.LittleEndian.PutUint32(headerBytes[12:16], uint32(h.UpperOffset))
	binary.LittleEndian.PutUint32(headerBytes[16:20], uint32(h.LowerOffset))
	binary.LittleEndian.PutUint32(headerBytes[20:24], uint32(h.MagicNumber))
	binary.LittleEndian.PutUint32(headerBytes[24:], uint32(h.Checksum))

	return headerBytes
}
