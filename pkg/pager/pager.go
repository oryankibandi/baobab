/*
The pager is responsible for managing page layout, page header and
page metadata.

If provides an interface to modify page byte content and control concurrent
access.

It manages metadata such as root page Id, number of flushed pages, max page Id.
It maps page IDs to offsets in data file and issues read and write requests
to the disk manager.

It also maintains a free list of available pageIds that may have been deleted
and need recycling.
*/

package pager

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/oryankibandi/baobab/pkg/diskmanager"
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

// I/O Operation timeouts
const (
	READ_PAGE_TIMEOUT_MILL     = 200
	NO_SYNC_WRITE_TIMEOUT_MILL = 200
	SYNC_WRITE_TIMEOUT_MILL    = 400
)

const (
	MAX_REQ_JOBS = 100 // max number of io requests to send to disk manager
	MAX_WORKERS  = 50  // default no. of workers.
)

type IOJob struct {
	// stores boolean values
	// 0x07->1 for write 0 for read
	// 0x06->dead page
	// 0x05->flush
	flag       byte
	buff       *[]byte
	errCh      *chan error
	sizeOfBuff int64
	pageId     uint32
}

type workerpool struct {
	// buffered channel to provide back-pressure. This regulates how many requests can be
	// sent to the diskmanager.
	jobQ chan IOJob
	wg   sync.WaitGroup
}

type Pager struct {
	// free list
	freeList *FreeList
	// disk manager
	dManager *diskmanager.DiskManager
	// id of the root page
	rootPageId uint32
	// total number of pages
	pageCount uint32
	// max no of pageId issued. pageId increases monotonically. This is
	// used to issue new page Ids.
	maxPageId uint32
	workers   *workerpool
	mu        sync.RWMutex
}

type PagerConfig struct {
	FreeListFile string
	DManager     *diskmanager.DiskManager
	WorkerSize   uint64
}

func (j *IOJob) executeJob(pgr *Pager) {
	if pgr == nil {
		panic("No pager provided")
	}

	if helpers.BitIsSet(&j.flag, 0x07) {
		// write req
		isDead := helpers.BitIsSet(&j.flag, 0x06)
		flush := helpers.BitIsSet(&j.flag, 0x05)

		errChan := make(chan error)
		err := pgr.dManager.WriteReq(j.pageId*PAGE_SIZE_BYTES, j.buff, PAGE_SIZE_BYTES, isDead, &errChan, flush)
		if err != nil {
			*(j.errCh) <- err
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(NO_SYNC_WRITE_TIMEOUT_MILL)*time.Millisecond)

		select {
		case e := <-errChan:
			cancel()
			if e != nil {
				*j.errCh <- e
				return
			}
		case <-ctx.Done():
			cancel()
			*j.errCh <- PagerError{Message: fmt.Sprintf("timeout writing to page: %d", j.pageId)}
			return
		}

	} else {
		errChan := make(chan error)
		err := pgr.dManager.ReadReq(j.buff, j.pageId*PAGE_SIZE_BYTES, &errChan)
		if err != nil {
			*j.errCh <- err
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(READ_PAGE_TIMEOUT_MILL)*time.Millisecond)

		select {
		case e := <-errChan:
			cancel()
			if e != nil {
				*j.errCh <- e
				return
			}
		case <-ctx.Done():
			cancel()
			*j.errCh <- PagerError{Message: "timeout reading page."}
			return
		}
	}

	*j.errCh <- nil
}

// NewPage - initializes a blank page by assigning a pageId and setting a flag
// returns new page Id, and error if any
func (pgr *Pager) NewPage(setAsRoot bool, isInternal bool, pge *Page) (uint32, error) {
	if pge == nil {
		return 0, PagerError{Message: "No page entry provided."}
	}
	pgr.mu.Lock()
	defer pgr.mu.Unlock()

	var newPgeId uint32
	if n := pgr.freeList.pop(); n < 0 {
		newPgeId = pgr.maxPageId + 1
		pgr.maxPageId++
	} else {
		newPgeId = uint32(n)
	}

	err := pge.initializePage(newPgeId, isInternal)
	if err != nil {
		return 0, err
	}

	if setAsRoot {
		pgr.rootPageId = newPgeId
	}

	// increment page count
	pgr.pageCount++
	return newPgeId, nil
}

// readMetadata get metadata page and reads content into buff
func (pgr *Pager) readMetadata(buff *[]byte) error {
	if len(*buff) != PAGE_SIZE_BYTES {
		return PagerError{Message: fmt.Sprintf("Invalid buffer size. Expected buffer size of %d bytes", PAGE_SIZE_BYTES)}
	}

	workerErrChan := make(chan error)
	ioJob := IOJob{
		flag:       0x00, // 0000 0000
		buff:       buff,
		errCh:      &workerErrChan,
		pageId:     0,
		sizeOfBuff: -1,
	}

	// blocks if channel is full
	pgr.workers.jobQ <- ioJob

	// blocks until we a response
	err := <-workerErrChan
	if err != nil {
		return err
	}
	return nil
}

// ReadPage submits a read request to diskmanager which reads page
// content into buff. Access to buff should be controlled to prevent data races.
func (pgr *Pager) ReadPage(pageId uint32, buff *[]byte) error {
	if pgr.workers.jobQ == nil {
		return PagerError{Message: "job queue not initialized."}
	}

	if len(*buff) != PAGE_SIZE_BYTES {
		return PagerError{Message: fmt.Sprintf("Invalid buffer size. Expected buffer size of %d bytes", PAGE_SIZE_BYTES)}
	}

	workerErrChan := make(chan error)
	ioJob := IOJob{
		flag:       0x00, // 0000 0000
		buff:       buff,
		errCh:      &workerErrChan,
		pageId:     pageId,
		sizeOfBuff: -1,
	}

	// blocks if channel is full
	pgr.workers.jobQ <- ioJob

	// blocks until we a response
	err := <-workerErrChan
	if err != nil {
		return err
	}

	return nil
}

// WritePage submits a write request to diskmanager and writes content of buff
// to disk. Access to buff should be controlled to prevent data races.
// Parameters:
//
//	pageId id of page to flush
//	buff buffer containing page content to write
func (pgr *Pager) WritePage(pageId uint32, buff *[]byte) error {
	if pgr.workers.jobQ == nil {
		return PagerError{Message: "job queue not initialized."}
	}

	if len(*buff) != PAGE_SIZE_BYTES {
		return PagerError{Message: fmt.Sprintf("Invalid buffer size. Expected buffer size of %d bytes", PAGE_SIZE_BYTES)}
	}

	isDead := helpers.BitIsSet(&((*buff)[0]), Dead)

	var flag byte
	flag = 1 << 7
	if isDead {
		flag |= 1 << 6
	}

	workerErrChan := make(chan error)
	ioJob := IOJob{
		flag:       byte(flag),
		buff:       buff,
		errCh:      &workerErrChan,
		pageId:     pageId,
		sizeOfBuff: PAGE_SIZE_BYTES,
	}

	pgr.workers.jobQ <- ioJob
	err := <-workerErrChan
	if err != nil {
		close(workerErrChan)
		return err
	}

	// if page was marked as dead, add pageid to free list
	if isDead {
		pgr.freeList.add(pageId)

		// one page has been cleared, decrement page count
		pgr.mu.Lock()
		if pgr.pageCount > 0 {
			pgr.pageCount--
		}
		pgr.mu.Unlock()
	}
	return nil
}

// FlushMetadata flushes metadata page in buff to disk.
// Parameters:
//
//	buff buffer containing metadata page content. Usually pinned in buffer manager.
func (pgr *Pager) FlushMetadata(buff *[]byte) error {
	if len(*buff) != PAGE_SIZE_BYTES {
		return PagerError{Message: fmt.Sprintf("Invalid buffer size. Expected buffer size of %d bytes", PAGE_SIZE_BYTES)}
	}

	// update data
	pgr.mu.Lock()
	binary.LittleEndian.PutUint32((*buff)[0:4], pgr.rootPageId)
	binary.LittleEndian.PutUint32((*buff)[12:16], pgr.pageCount)
	binary.LittleEndian.PutUint32((*buff)[16:20], pgr.maxPageId)
	pgr.mu.Unlock()

	var flag byte
	flag = 1 << 7
	flag |= 1 << 5 // flush

	workerErrChan := make(chan error)
	ioJob := IOJob{
		flag:       byte(flag),
		buff:       buff,
		errCh:      &workerErrChan,
		pageId:     0,
		sizeOfBuff: PAGE_SIZE_BYTES,
	}

	pgr.workers.jobQ <- ioJob
	err := <-workerErrChan
	if err != nil {
		return err
	}

	return nil
}

// UpdateRootPage updates root page
func (pgr *Pager) UpdateRootPage(pageId uint32) {
	pgr.mu.Lock()
	defer pgr.mu.Unlock()

	pgr.rootPageId = pageId
}

func (pgr *Pager) RootPage() uint32 {
	pgr.mu.RLock()
	defer pgr.mu.RUnlock()

	return pgr.rootPageId
}

func (pgr *Pager) Flush() {
	pgr.dManager.ForceFlush()
}

func (pgr *Pager) FlushFreeList() {
	n := make(chan int)
	pgr.freeList.flushFreeList(&n)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*SYNC_WRITE_TIMEOUT_MILL)
	defer cancel()

	select {
	case t := <-n:
		fmt.Printf("Flushed free list.Written %d bytes\n", t)
	case <-ctx.Done():
		fmt.Println("timeout flushing freelist")
	}
}

// InitFromMetadata - reads metadata page from disk and sets metadata on pager.
//
//			      should be called immediately after initializing pager to set pager fields
//
//	 Parameters:
//		metaPge - pointer to page with an allocated location. Read metadata content is stored in this page.
//	 Returns:
//		error - error if any
func (pgr *Pager) InitFromMetadata(metaPge *Page) error {
	// read metadata page
	// In order to pass *buffer to readMetadata(), we create a slice backed by
	// our original array for in place update
	metadataSlice := metaPge.pgeData[:]
	err := pgr.readMetadata(&metadataSlice)
	if err != nil {
		return err
	}

	pgr.rootPageId = binary.LittleEndian.Uint32(metaPge.pgeData[0:4])
	pgr.pageCount = binary.LittleEndian.Uint32(metaPge.pgeData[12:16])
	pgr.maxPageId = binary.LittleEndian.Uint32(metaPge.pgeData[16:20])

	return nil
}

func (w *workerpool) startWorkers(pgr *Pager, poolsize uint64) error {
	if w.jobQ == nil {
		return PagerError{Message: "job queue not initialized"}
	}

	if poolsize == 0 {
		return PagerError{Message: "Invalid worker poolsize. Atleast one worker required."}
	}

	if poolsize > MAX_WORKERS {
		return PagerError{Message: fmt.Sprintf("Worker pool size cannot go above %d", MAX_WORKERS)}
	}

	for range poolsize {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for j := range w.jobQ {
				j.executeJob(pgr)
			}
		}()
	}

	helpers.PrintInfoMsg(fmt.Sprintf("started %d workers", poolsize))
	return nil
}

func (w *workerpool) shutdown() {
	if w.jobQ != nil {
		fmt.Println("terminating workers...")
		close(w.jobQ)
		w.wg.Wait()
		fmt.Println("terminated all workers...")
	}
}

// NewPager creates and returns an instance of pager, or error if any
// Parameters:
//
//	buff	buffer to store metadata page
//
// Returns:
//
//	pager a pointer to a pager
//	e	error if any
func NewPager(pgrConfig PagerConfig) (pager *Pager, e error) {
	if pgrConfig.DManager == nil {
		return nil, PagerError{Message: "diskmanager instance not proviided"}
	}

	fmt.Println("PAGER CONFIG -> ", pgrConfig)
	if pgrConfig.WorkerSize == 0 {
		// set default value
		fmt.Println("no poolsie provided, setting to default")
		pgrConfig.WorkerSize = MAX_WORKERS
	}

	fl, err := NewFreeList(pgrConfig.FreeListFile)
	if err != nil {
		return nil, err
	}
	fl.loadFreeList()

	wrk := workerpool{
		jobQ: make(chan IOJob, MAX_REQ_JOBS),
	}

	pgr := &Pager{
		freeList: fl,
		dManager: pgrConfig.DManager,
		workers:  &wrk,
	}

	// start workers
	if err = pgr.workers.startWorkers(pgr, pgrConfig.WorkerSize); err != nil {
		pgr.freeList.close()
		pgr.dManager.Close()
		return nil, PagerError{Message: "Unable to start workers"}
	}

	return pgr, nil
}

// Syncs all OS buffer content to disk then calls close on diskmanager and freeList
func (pgr *Pager) Close() {
	pgr.workers.shutdown()
	pgr.dManager.ForceFlush()
	pgr.dManager.Close()
	pgr.freeList.close()

	// check goroutines
	helpers.PrintInfoMsg(fmt.Sprintf("Num of gorutines -> %d", runtime.NumGoroutine()))
}
