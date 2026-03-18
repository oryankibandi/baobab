package diskmanager

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/oryankibandi/baobab/pkg/helpers"
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
type ioReq struct {
	buff    *[]byte
	size    int64
	errChan chan error
	off     uint32
	flag    byte
}

type JobQueue struct {
	// PageId/Block Id for which this job is for
	pageId   uint32
	jobs     []ioReq
	running  bool
	mu       sync.Mutex
	dManager *DiskManager
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

	// read into buffer
	_, err := d.fd.ReadAt(*buff, int64(off))
	if err != nil && !errors.Is(err, io.EOF) {
		panic(fmt.Sprintf("Unable to read offset %d: %v", off, err.Error()))
	}

	return nil
}

// Creates write request for `page` and adds it to queue
func (d *DiskManager) WriteReq(off uint32, buff *[]byte, size int64, isDead bool, errChan chan error) error {
	if buff == nil {
		return DiskManagerError{Message: "invalid buffer pointer provided."}
	}
	if errChan == nil {
		return DiskManagerError{Message: "invalid error channel provided."}
	}

	var flag byte
	if isDead {
		// set dead flag
		helpers.SetFlag(&flag, 6)
	}
	writeReq := ioReq{
		buff:    buff,
		size:    size,
		off:     off,
		flag:    flag,
		errChan: errChan,
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
	fmt.Println("(WriteReq) Job added....")

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

	fmt.Println("(diskmanager) Read Req at offset -> ", off)

	rReq := ioReq{
		flag:    0x80, // 1000 0000
		buff:    buff,
		off:     off,
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
func (d *DiskManager) flushPage(pData *[]byte, size uint32, offset uint32, isDead bool) {
	fmt.Println("(flushPage) Flushing page...")

	if isDead {
		// rewrite with zeros
		d.mu.Lock()
		for i := range size {
			d.wg.Add(1)
			go func() {
				(*pData)[i] &= 0
				d.wg.Done()
			}()
		}
		d.wg.Wait()
		d.mu.Unlock()

		// page marked for deletion, overwrite with 0s
		_, err := d.fd.WriteAt(*pData, int64(offset))

		if err != nil {
			panic("Could not write page")
		}
		return
	}

	// page marked for deletion, overwrite with 0s
	_, err := d.fd.WriteAt(*pData, int64(offset))

	if err != nil {
		panic("Could not write page")
	}
	return
}

func (q *JobQueue) run() {
	fmt.Println("(run) acquiring queue lock() to set running=true...")
	q.mu.Lock()
	fmt.Printf("(run) acquired queue lock() to set running=true on page ID  %d...\n", q.pageId)
	q.running = true
	q.mu.Unlock()

	var job ioReq
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
		} else if len(q.jobs) == 0 {
			panic("Invalid state. Length of jobs is negative")
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
		fmt.Println("Queue not running, starting job...")
		go q.run()
	}
}

// executes queue job
func (r *ioReq) execute(dMan *DiskManager) {
	fmt.Println("(execute) diskmanager.execute()...")
	if helpers.BitIsSet(&r.flag, 7) {
		// Read req. Read from disk, create Page and return that in channel
		fmt.Println("(execute) executing read request...")
		err := dMan.loadPage(r.off, r.buff)
		if err != nil {
			r.errChan <- err
		}
		fmt.Println("(execute) sending back read page...")

		r.errChan <- nil
	} else {
		// Write page to disk
		fmt.Printf("(execute) executing write request -> %d...\n", r.off)
		dMan.flushPage(r.buff, uint32(r.size), r.off, helpers.BitIsSet(&r.flag, 6))
		fmt.Printf("(execute) executed write request -> %d...\n", r.off)
	}
	fmt.Println("(execute) diskmanager.execute() DONE.")
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
