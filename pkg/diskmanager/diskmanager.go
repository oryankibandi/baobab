package diskmanager

import (
	//"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	//"runtime"
	"sync"

	"github.com/oryankibandi/baobab/pkg/helpers"
)

const (
	CACHE_LINE_SIZE = 64
)

type DiskManager struct {
	// Map of job queues for read and write requests {PageId: *JobQueue}
	Queues sync.Map

	// database file
	fd *os.File
	wg sync.WaitGroup
	mu sync.RWMutex
}

type DiskManagerConfig struct {
	// relative path to database file
	DataFile string
}

// Disk req for reads and writes
// pointer pointer to buffer where page data is retrieved from for writes. or
//
//	in case of a read request, data is written to.
//
// size size of buffer
// off offset to read/write at
// errChan chan to send errors to
// flag flags representing the type of request and dead page
//
//	+-----+-----+-----+-----+-----+-----+-----+-----+
//	| b7  | b6  | b5  | b4  | b3  | b2  | b1  | b0  |
//	+-----+-----+-----+-----+-----+-----+-----+-----+
//	|  1  |  1  |  0  |  0  |  0  |  0  |  0  |  0  |
//	+-----+-----+-----+-----+-----+-----+-----+-----+
//
//	b7: request type. 1 if read request, 0 if write request
//	b6: dead page flag. 1 if dead page, 0 if not.Dead pages are overwritten
//	    with zero
//
// Total size: 40 bytes
type ioReq struct {
	buff    *[]byte
	size    int64
	errChan chan error
	off     uint64
	flag    byte
	flush   bool
	_       [6]byte // padding
}

type JobQueue struct {
	// PageId/Block Id for which this job is for
	pageId   uint32
	jobs     []ioReq
	running  bool
	mu       sync.Mutex
	dManager *DiskManager
}

func currentFileSize(fd *os.File) int64 {
	info, _ := fd.Stat()
	return info.Size()
}

// Reads a page from disk at offset off into buff
//
// Parameters:
//
//	off offset to start reading from
//	buff buffer to store read page content
//
// Return:
//
//	err error if any
func (d *DiskManager) loadPage(off uint32, buff *[]byte) error {
	if buff == nil {
		return DiskManagerError{Message: "Invalid buffer provided"}
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	// read into buffer
	_, err := d.fd.ReadAt(*buff, int64(off))
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	//bif binary.LittleEndian.Uint32((*buff)[1:5]) == 0 && off != 0 {
	//b	// read file hole
	//b	panic(fmt.Sprintf(
	//b		"zero read: pageID=%d n=%d offset=%d fileSize=%d goroutine=%d",
	//b		off/8192, n, off, currentFileSize(d.fd), runtime.NumGoroutine(),
	//b	))
	//b}

	// fmt.Printf("(diskmanager) reading at offset %d(page %d), result -> %v\n", off, off/8192, (*buff))

	return nil
}

// Creates write request for `page` and adds it to queue
//
// Parameters:
//
//	off  - offset to write at.
//	buff - pointer to buffer with page byte content
//	size - size of buff
//	isDead - if page is marked for deletion
//	errChan - pointer to channel to receive any errors
//	flush	- if true, Sync() is called to write content in buffer to disk.
//		  Should be used cautiously as it's expensive but necessary for
//		  durability.
func (d *DiskManager) WriteReq(off uint32, buff *[]byte, size int64, isDead bool, errChan chan error, flush bool) error {
	if buff == nil {
		return DiskManagerError{Message: "invalid buffer pointer provided."}
	}
	if errChan == nil {
		return DiskManagerError{Message: "invalid error channel provided."}
	}

	var flag byte
	if isDead {
		// set dead flag
		helpers.SetFlag(&flag, 6) // 0100 000
	}
	writeReq := ioReq{
		buff:    buff,
		size:    size,
		off:     uint64(off),
		flag:    flag,
		errChan: errChan,
		flush:   flush,
	}

	// Check queue
	q, ok := d.Queues.Load(off)

	if !ok {
		// Create queue
		jQ := d.newJobQueue(off)
		d.Queues.Store(off, jQ)
		jQ.addJob(writeReq)

		return nil
	}

	q.(*JobQueue).addJob(writeReq)
	//fmt.Println("(WriteReq) Job added....")

	return nil
}

// Calls fsync() on buffered contents.
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
func (d *DiskManager) ReadReq(buff *[]byte, off uint32, readErr chan error) error {
	if buff == nil {
		return DiskManagerError{Message: "invalid buffer pointer provided."}
	}
	if readErr == nil {
		return DiskManagerError{Message: "invalid error channel provided."}
	}

	//  fmt.Println("(diskmanager) Read Req at offset -> ", off)

	rReq := ioReq{
		flag:    0x80, // 1000 0000
		buff:    buff,
		off:     uint64(off),
		errChan: readErr,
	}

	q, ok := d.Queues.Load(off)

	if !ok {
		// Create queue
		fmt.Println("(diskmanager.ReadReq()) No queue, creating...")
		jQ := d.newJobQueue(off)
		d.Queues.Store(off, jQ)

		jQ.addJob(rReq)

		return nil
	}

	q.(*JobQueue).addJob(rReq)

	return nil
}

// safely close file descriptors
func (d *DiskManager) Close() {
	d.mu.Lock()
	err := d.fd.Close()

	if err != nil {
		panic(err.Error())
	}

	d.mu.Unlock()

	fmt.Println("Closed db file descriptors")
}

// Flushes page content to disk(does not call sync())
// Paramters:
//
//	pData pointer to buffer with data to flush
//	offset offset at which to write data
//	size size of data being written
//	isDead if the page is dead(has logically deleted). If true, overwrite with 0
func (d *DiskManager) flushPage(pData *[]byte, size uint32, offset uint32, isDead bool, flush bool) error {
	if isDead {
		// rewrite with zeros
		d.mu.Lock()
		// to prevent false sharing, ensure pData is divided into
		// 64 byte chunks for processing
		for i := range size / CACHE_LINE_SIZE {
			d.wg.Add(1)
			go func(arr []byte) {
				for j := range arr {
					arr[j] &= 0
				}
				d.wg.Done()
			}((*pData)[i*CACHE_LINE_SIZE : (i*CACHE_LINE_SIZE)+CACHE_LINE_SIZE])
		}

		d.wg.Wait()
		d.mu.Unlock()

		d.mu.RLock()
		defer d.mu.RUnlock()
		// page marked for deletion, overwrite with 0s
		_, err := d.fd.WriteAt(*pData, int64(offset))
		if err != nil {
			return err
		}

		return nil
	}

	d.mu.RLock()
	_, err := d.fd.WriteAt(*pData, int64(offset))

	if err != nil {
		d.mu.RUnlock()
		return err
	}
	d.mu.RUnlock()

	if flush {
		d.mu.Lock()
		err = d.fd.Sync()

		if err != nil {
			d.mu.Unlock()
			return err
		}
		d.mu.Unlock()
	}

	return nil
}

func (q *JobQueue) run() {
	q.mu.Lock()
	q.running = true
	q.mu.Unlock()

	var job ioReq
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

		job.execute(q.dManager) // execute job
	}
}

func (q *JobQueue) addJob(job ioReq) {
	q.mu.Lock()
	q.jobs = append(q.jobs, job)
	shouldStart := !q.running
	q.mu.Unlock()

	if shouldStart {
		// fmt.Println("Queue not running, starting job...")
		go q.run()
	}
}

// executes queue job
func (r *ioReq) execute(dMan *DiskManager) {
	if helpers.BitIsSet(&r.flag, 7) {
		// Read req. Read from disk, create Page and return that in channel
		err := dMan.loadPage(uint32(r.off), r.buff)
		// send err to channel
		r.errChan <- err
	} else {
		// Write page to disk
		err := dMan.flushPage(r.buff, uint32(r.size), uint32(r.off), helpers.BitIsSet(&r.flag, 6), r.flush)

		// send error to channel
		r.errChan <- err
	}
}

func (d *DiskManager) newJobQueue(pageId uint32) *JobQueue {
	return &JobQueue{
		pageId:   pageId,
		jobs:     make([]ioReq, 0),
		running:  false,
		dManager: d,
	}
}

// NewDiskManager Opens a database file and returns a new instance of disk manager
func NewDiskManager(config DiskManagerConfig) (*DiskManager, error) {
	if len(config.DataFile) == 0 {
		return nil, DiskManagerError{Message: "data file path not provided"}
	}

	fd, err := os.OpenFile(config.DataFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("ERR while opening file")
		log.Fatal(err)
	}

	diskManager := &DiskManager{
		fd: fd,
	}

	return diskManager, nil
}
