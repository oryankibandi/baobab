package wal

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"
)

type WriteReq struct {
	data      []byte
	writeChan *chan int
}

type WalWriter struct {
	fd        *os.File
	confFD    *os.File
	queue     *WriteQueue
	maxPage   uint32 // Latest page in WAL file
	maxOffset uint32 // Largest offset in WAL segment
	mu        sync.Mutex
}

type WriteQueue struct {
	head    *WriteNode
	tail    *WriteNode
	running bool
	mu      sync.Mutex
}

type WriteNode struct {
	req  *WriteReq
	prev *WriteNode
	next *WriteNode
}

// adds first checkpoint if empty
func (wr *WalWriter) initializeWriter() error {
	wr.mu.Lock()

	// check size of wal
	info, err := wr.fd.Stat()

	if err != nil {
		panic(err)
	}

	walSize := info.Size()

	if walSize <= 0 {
		// if wal is empty, add the first checkpoint
		lsn := make([]byte, LSN_SIZE)

		// set page and offset in lsn. Offset should consider
		// size of wal page header
		binary.LittleEndian.PutUint32(lsn[:4], 0)
		binary.LittleEndian.PutUint32(lsn[4:], WAL_PAGE_HEADER_SIZE)

		cp := CheckPoint{
			flag:          0x80,
			redoPoint:     WAL_PAGE_HEADER_SIZE,
			checkpointLSN: lsn,
		}

		data := cp.toBytes()
		data = append(make([]byte, WAL_PAGE_HEADER_SIZE), data...)
		_, err := wr.fd.Write(data)

		if err != nil {
			panic(fmt.Errorf("(walwriter) Unable to write first checkpoint: ", err))
		}

		// update maxOffset
		wr.maxOffset = WAL_PAGE_HEADER_SIZE + CHECKPOINT_SIZE

		// save checkpoint position
		wr.mu.Unlock()
		err = wr.saveCheckpoint(lsn)

		if err != nil {
			panic(fmt.Errorf("(walwriter) Unable to save first checkpoint: ", err))
		}

		return nil
	}

	wr.mu.Unlock()

	return nil
}

// Adds a write req to tail of queue
func (wr *WalWriter) AddJob(data []byte, c *chan int) {
	wr.queue.mu.Lock()
	req := WriteReq{
		data:      data,
		writeChan: c,
	}

	wNode := WriteNode{
		req: &req,
	}

	if wr.queue.head == nil {
		if wr.queue.tail != nil {
			panic("Invalid linked list")
		}

		// list empty, add as first item
		wr.queue.head = &wNode
		wr.queue.tail = &wNode
	} else {
		// add to tail
		wr.queue.tail.next = &wNode
		wr.queue.tail = &wNode
	}

	shouldStart := !wr.queue.running

	wr.queue.mu.Unlock()

	if shouldStart {
		go wr.queue.run(wr.fd)
	}
}

// constructs and increments LSN
func (wr *WalWriter) assignLSN(logSize uint32) []byte {
	wr.mu.Lock()
	defer wr.mu.Unlock()
	lsn := make([]byte, 8)

	binary.LittleEndian.PutUint32(lsn[:4], wr.maxPage)
	binary.LittleEndian.PutUint32(lsn[4:], wr.maxOffset)

	// increment
	newMaxOff := wr.maxOffset + logSize

	if newMaxOff >= WAL_PAGE_SIZE {
		// Oveflows into next page. include WAL page header size
		wr.maxOffset = (newMaxOff - WAL_PAGE_SIZE) + WAL_PAGE_HEADER_SIZE
		wr.maxPage += 1
	} else {
		wr.maxOffset = newMaxOff
	}

	return lsn
}

// Writes the LSN of the latest checkpoint to a separate file for recovery.
func (wr *WalWriter) saveCheckpoint(lsn []byte) error {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if wr.confFD == nil {
		return WalError{Message: fmt.Sprintf("No file descriptor for config file: %s", WAL_CONFIG_PATH)}
	}

	// write lsn to file. Overwrite existing data.
	err := wr.confFD.Truncate(0)

	if err != nil {
		panic(fmt.Errorf("Unable to truncate config file: %v", err))
	}
	_, err = wr.confFD.Seek(0, 0)
	if err != nil {
		panic(fmt.Errorf("Unable to Seek config file: %v", err))
	}

	n, err := wr.confFD.Write(lsn)
	if err != nil {
		panic(fmt.Errorf("Unable to write to config file: %v", err))
	}

	log.Printf("(saveCheckpoint) Written %d bytes\n", n)

	return nil
}

// remove and returns the head item from the queue
func (wq *WriteQueue) unshift() *WriteNode {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if wq.tail == nil || wq.head == nil {
		return nil
	}

	h := wq.head

	if wq.head == wq.tail {
		// only one item in linked list
		wq.head = nil
		wq.tail = nil
	} else {
		wq.head = h.next
	}

	return h
}

func (wq *WriteQueue) run(fd *os.File) {
	var currJob *WriteNode

	currJob = wq.unshift()

	for currJob != nil {
		// append to wal file
		currJob.req.writeWal(fd)
		// get next job
		currJob = wq.unshift()
	}

	// update running status
	wq.mu.Lock()
	wq.running = false
	wq.mu.Unlock()

}

// writes byte chunk to WAL file in append only mode
func (wr *WriteReq) writeWal(fd *os.File) bool {
	n, err := fd.Write(wr.data)

	if err != nil {
		panic(fmt.Sprintf("Unable to write to wal: %s", err.Error()))
	}

	// send back info
	*wr.writeChan <- n

	return true
}

// Create new WAL Writer
func NewWalWriter(path string) *WalWriter {
	// Read & calculate max page and offset
	maxPage, maxOff := loadMaxLSN(path)

	// open wal in append mode. Create if does not exist
	fd, err := os.OpenFile(WAL_PATH, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Unable to open wal: %v", err))
	}

	// open config file with write only flag
	confFd, err := os.OpenFile(WAL_CONFIG_PATH, os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		panic(fmt.Sprintf("Unable to open config file: %d: %v", WAL_CONFIG_PATH, err))
	}

	jobQueue := WriteQueue{}

	wr := WalWriter{
		fd:        fd,
		confFD:    confFd,
		queue:     &jobQueue,
		maxPage:   maxPage,
		maxOffset: maxOff,
	}

	// initalize
	err = wr.initializeWriter()

	if err != nil {
		panic(err)
	}

	return &wr
}

// Reads wal file and calculates tha max LSN from the size of the file
func loadMaxLSN(path string) (maxPage uint32, off uint32) {
	rFd, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)

	if err != nil {
		panic(err.Error())
	}

	defer rFd.Close()

	// get size
	info, err := rFd.Stat()

	if err != nil {
		panic(err.Error())
	}

	var page uint32
	var offset uint32

	s := info.Size()

	if s <= 0 {
		page = 0
		offset = WAL_PAGE_HEADER_SIZE
	} else {
		// calculate page
		page, offset, err = calculatePageAndOffset(s)

		if err != nil {
			panic(err.Error())
		}
	}

	fmt.Println("LOADED PAGE :=> ", page)
	fmt.Println("LOADED OFFSET :=> ", offset)

	return page, offset
}

func calculatePageAndOffset(walSize int64) (page uint32, offset uint32, err error) {
	if walSize <= 0 {
		return 0, 0, WalError{Message: "Invalid wal size."}
	}

	pge := walSize / WAL_PAGE_SIZE
	off := walSize - (WAL_PAGE_SIZE * pge)

	return uint32(pge), uint32(off), nil
}
