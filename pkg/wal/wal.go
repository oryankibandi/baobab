package wal

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/oryankibandi/baobab/pkg/logger"
)

// sizes in bytes
const (
	WAL_PAGE_SIZE        = 8192
	WAL_PAGE_HEADER_SIZE = 8
	WAL_SEG_FILE_SIZE    = 16777216
	BLOG_HEADER_SIZE     = 25
	CHECKPOINT_SIZE      = 17
	LSN_SIZE             = 12
)

// WAL config
const (
	WAL_FLUSH_DELAY = 200         // period after which to flush WAL buffer contents
	WAL_PATH        = "bb.wal"    // file path to use as wal
	WAL_CONFIG_PATH = "bb_config" // bb_config file stores LSN of latest checkpoint
	WAL_MAGIC_NO    = 65544       // 4 byte number that stores wal version (2 bytes) & page size (2 bytes)
)

const (
	PUT OperationType = iota
	DEL
)

// Log and Checkpoint Flag positions
const (
	LOGTYPE_FLAG_POS = 7
)

type nodeValue interface {
	toBytes() []byte
}

type OperationType int

// Structure of a log, otherwise known as B-LOG
type BLog struct {
	header  *BLogHeader   // 25 byte Header
	opType  OperationType // Type of operation (1 byte)
	keySize uint32        // Size of key (4 bytes)
	key     []byte        // Key (keySize)
	valSize uint32        // Size of val (4 bytes)
	val     []byte        // Value (valSize)
	mu      sync.Mutex
}

// Structure of the B-LOG header
type BLogHeader struct {
	flag   byte   // 1 byte header flags
	lsn    []byte // 12 byte log sequence number
	pageId uint32 // ID of page where this change affects. This will be used to compare LSN during recovery
	crc    uint32 // Cyclic Redundacy Check number for integrity checks
	lSize  uint32 // Size of B-LOG
	mu     sync.Mutex
}

// Structure of a Checkpoint
type CheckPoint struct {
	flag          byte   // Checkpoint flag (1 byte)
	redoPoint     uint32 // REDO point from which to begin recovery (4 bytes)
	checkpointLSN []byte // 8-byte LSN
}

// Structure of WAL 8K Page Header
type WALPageHeader struct {
	magicNo uint32 // 4 bytes
	pageId  uint32 // page address/ID of the WAL paga - 4 bytes
}

type WAL struct {
	walBuff   *WALBuffer
	walWriter *WalWriter
	walReader *WalReader
	logger    *logger.BaobabLogger
}

// Returns the raw byte  value of the B-LOG heeader
func (h *BLogHeader) toBytes() [BLOG_HEADER_SIZE]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	var hdr [BLOG_HEADER_SIZE]byte

	hdr[0] = h.flag
	copy(hdr[1:13], h.lsn)
	binary.LittleEndian.PutUint32(hdr[13:17], h.lSize)
	binary.LittleEndian.PutUint32(hdr[17:21], h.pageId)
	binary.LittleEndian.PutUint32(hdr[21:25], h.crc)

	log.Println("(toBytes) BLOG HEADER ==> ", hdr)

	return hdr
}

// Returns the raw byte  value of the B-LOG
func (b *BLog) toBytes() []byte {
	if b.header == nil {
		panic("No header in BLOG")
	}

	bLogData := make([]byte, BLOG_HEADER_SIZE)

	// add header
	hdr := b.header.toBytes()
	copy(bLogData, hdr[:])

	// Get pageId and Offset
	pgeId := binary.LittleEndian.Uint32(hdr[1:5])
	off := binary.LittleEndian.Uint32(hdr[5:9])

	// data
	bLogData = append(bLogData, byte(b.opType))
	// bLogData = binary.LittleEndian.AppendUint32(bLogData, uint32(b.opType))
	bLogData = binary.LittleEndian.AppendUint32(bLogData, b.keySize)
	bLogData = append(bLogData, b.key...)

	if b.valSize > 0 {
		bLogData = binary.LittleEndian.AppendUint32(bLogData, b.valSize)
		bLogData = append(bLogData, b.val...)
	}

	// check for overflow
	bLogData = checkLogOverflow(bLogData, pgeId, off)

	return bLogData
}

// checks if a B-LOG data has crossed page boundary and inserts
// WAL page header in the appropriate position, and returns the
// new formatted log data
func checkLogOverflow(data []byte, page uint32, offset uint32) []byte {
	endOff := int(offset) + len(data)

	if endOff < WAL_PAGE_SIZE {
		// no overflow, check if first page
		if offset <= WAL_PAGE_HEADER_SIZE {
			// add curr WAL page header to the beginning.
			hdr := WALPageHeader{
				pageId:  page,
				magicNo: WAL_MAGIC_NO,
			}

			hdrBytes := hdr.toBytes()

			data = append(hdrBytes, data...)
		}

		return data
	}

	// get index of overflow
	idx := endOff - WAL_PAGE_SIZE

	// Create WAL page size
	walPgeHdr := WALPageHeader{
		pageId:  page + 1,
		magicNo: WAL_MAGIC_NO,
	}

	walPgeHdrByte := walPgeHdr.toBytes()
	// insert data at idx
	data = append(data[:idx], append(walPgeHdrByte, data[idx:]...)...)

	return data
}

// Returns the raw byte value of the Checkpoint
func (c *CheckPoint) toBytes() []byte {
	chckpnt := make([]byte, CHECKPOINT_SIZE)

	chckpnt[0] = c.flag
	binary.LittleEndian.PutUint32(chckpnt[1:5], c.redoPoint)
	chckpnt = append(chckpnt[:5], c.checkpointLSN...)

	page := binary.LittleEndian.Uint32(c.checkpointLSN[:4])
	off := binary.LittleEndian.Uint32(c.checkpointLSN[4:])

	// check if log crosses page boundary.
	chckpnt = checkLogOverflow(chckpnt, page, off)

	return chckpnt
}

func (wPageHder *WALPageHeader) toBytes() []byte {
	hdr := make([]byte, WAL_PAGE_HEADER_SIZE)

	binary.LittleEndian.PutUint32(hdr[:4], wPageHder.magicNo)
	binary.LittleEndian.PutUint32(hdr[4:], wPageHder.pageId)

	return hdr
}

// Adds a PUT B-LOG to WAL buffer, and return LSN
func (w *WAL) AddPutLog(pageId uint32, key []byte, val []byte) ([]byte, error) {
	kLen := len(key)
	vLen := len(val)

	if kLen <= 0 {
		return nil, WalError{Message: "Cannot add empty key in WAL"}
	}

	if vLen <= 0 {
		return nil, WalError{Message: "Cannot add empty val in WAL"}
	}

	// calculate size of log
	// HEADER_SIZE + Operation(1 byte) + key size (4 bytes) + val size (4 bytes) + kLen + vLen
	lSize := BLOG_HEADER_SIZE + 9 + kLen + vLen

	lsn := w.walWriter.assignLSN(uint32(lSize))

	hdr := BLogHeader{
		flag:   0x0, // Initialized with first bit flag unset for logs
		lsn:    lsn, // 12-byte LSN
		pageId: pageId,
		crc:    0, // TBC when adding CRC
		lSize:  uint32(lSize),
	}

	log := BLog{
		header:  &hdr,
		opType:  PUT,
		keySize: uint32(kLen),
		key:     key,
		valSize: uint32(vLen),
		val:     val,
	}

	// Add to WAL buffer
	w.walBuff.Add(&log)

	return lsn, nil
}

// Adds a checkpoint to tail of the list. This checkpoint contains the REDO point where recovery begins.
// It is primarily called by the background writer
func (w *WAL) AddCheckpoint(latestLSN []byte) error {
	if len(latestLSN) != LSN_SIZE {
		return WalError{Message: "Invalid LSN size"}
	}

	// get log size from LSN
	// s, err := w.walReader.getLogSize(latestLSN)

	// if err != nil {
	// 	w.logger.Write("logger", "AddCheckpoint", logger.LevelError, fmt.Sprintf("(wal) Unable to get size of log at LSN: %v\r ERR: %s", latestLSN, err.Error()), nil)
	// 	panic(fmt.Errorf("(wal) Unable to get size of log at LSN: %v", latestLSN))
	// }
	s := binary.LittleEndian.Uint32(latestLSN[8:12])

	// calculate offset(REDO point). This should be after the latest applied log
	page := binary.LittleEndian.Uint32(latestLSN[:4])
	redoOffset := binary.LittleEndian.Uint32(latestLSN[4:])

	redoPoint := (page * WAL_PAGE_SIZE) + redoOffset + s

	// get LSN
	lsn := w.walWriter.assignLSN(CHECKPOINT_SIZE)

	// construct checkpoint and add to linked list
	cp := CheckPoint{
		flag:          0x80, // 1000 0000
		redoPoint:     redoPoint,
		checkpointLSN: lsn,
	}

	w.walBuff.Add(&cp)

	w.logger.Write("logger", "AddCheckpoint", logger.LevelInfo, "ADDED CHECKPOINT, writing config file ", nil)
	err := w.walWriter.saveCheckpoint(lsn)

	if err != nil {
		panic(err)
	}

	return nil
}

// Adds a DEL B-LOG to WAL buffer, and return LSN
func (w *WAL) AddDelLog(pageId uint32, key []byte) ([]byte, error) {
	kLen := len(key)

	if kLen <= 0 {
		w.logger.Write("logger", "AddDelLog", logger.LevelError, "Cannot add empty key in WAL", nil)

		return nil, WalError{Message: "Cannot add empty key in WAL"}
	}

	// HEADER_SIZE + operation(1 byte) + key size(4 bytes) + kLen
	lSize := BLOG_HEADER_SIZE + 5 + kLen

	lsn := w.walWriter.assignLSN(uint32(lSize))

	hdr := BLogHeader{
		flag:   0x0,
		lsn:    lsn,
		pageId: pageId,
		lSize:  uint32(lSize),
		crc:    0, // TBC when adding CRC
	}

	log := BLog{
		header:  &hdr,
		opType:  DEL,
		keySize: uint32(kLen),
		key:     key,
	}

	// Add to WAL buffer
	w.walBuff.Add(&log)

	return lsn, nil
}

// Flushes wal buffer contents to wal segment.
// This can be called:
// 1. By a background wal buffer writer periodically
// 2. After a transaction is commited
// 3. WAL Buffer is filled up.
func (w *WAL) BLogWrite() {
	data, err := w.walBuff.flushWal()

	if err != nil {
		w.logger.Write("logger", "AddDelLog", logger.LevelError, fmt.Sprintf("(BLogWrite) Unable to flush: %v", err.Error()), nil)
		return
	}

	if data == nil {
		// empty buffer
		return
	}

	// create write req
	w.logger.Write("logger", "BLogWrite", logger.LevelInfo, "Flushing WAL data.....................", nil)

	c := make(chan int)

	w.walWriter.AddJob(data, &c)

	n := <-c

	log.Printf("Written %d bytes in wal\n", n)
	w.logger.Write("logger", "AddDelLog", logger.LevelInfo, fmt.Sprintf("Written %d bytes in wal\n", n), nil)

	return
}

// A background process that flushed WAL buffer contents to disk
// by calling BLogWrite()
func (w *WAL) walBgWriter() {
	for {
		w.BLogWrite()

		time.Sleep(time.Millisecond * WAL_FLUSH_DELAY)
	}
}

// Create new WAL
func NewWal(l *logger.BaobabLogger) *WAL {
	wal := WAL{
		walBuff:   NewWalBuff(l),
		walWriter: NewWalWriter(WAL_PATH, l),
		walReader: NewWalReader(WAL_PATH, l),
		logger:    l,
	}

	go wal.walBgWriter()

	return &wal
}
