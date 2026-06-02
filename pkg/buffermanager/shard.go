package buffermanager

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"sync"
	"unsafe"

	"github.com/oryankibandi/baobab/pkg/helpers"
	pager "github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

type bmJob struct {
	flag       byte
	errChan    chan error
	resultChan chan *Frame
	pid        uint32
}

type bmWorkerPool struct {
	queue chan *bmJob
	wg    sync.WaitGroup
}

type shard struct {
	CacheMap   map[uint32]*clockentry
	wTinyLfu   *WTinyLfu
	rmu        sync.RWMutex
	frameCount uint32
	diryList   *dPages
	pager      *pager.Pager
	shardId    uint64
	// exit channel
	exitchan *chan struct{}

	// workers
	workers *bmWorkerPool

	// wait group
	wg sync.WaitGroup
}

// Adds a page to cache, setting k as key. k is the unique page ID.
func (sh *shard) put(k uint32, en *clockentry, dirty bool) (*Frame, error) {
	// fmt.Println("cache.Put")
	sh.rmu.Lock()
	// fmt.Println("cache.put() acquired xclusive latch")

	val, ok := sh.CacheMap[k]
	if ok {
		// increment count
		sh.wTinyLfu.Increment(val)
	}

	sh.CacheMap[k] = en

	if en.entry.parentEntry == nil {
		// set parent entry ptr
		en.entry.parentEntry = en
	}

	// log.Println("Printing stats ....")
	sh.rmu.Unlock()
	//go c.wTinyLfu.Stat()
	return &en.entry, nil
}

// 2. Retrieve item from cache
func (sh *shard) get(pageId uint32) (*Frame, error) {
	// fmt.Printf("(%d) Get(): %d..\n", sh.shardId, pageId)
	errChan := make(chan error)
	resChan := make(chan *Frame)
	job := bmJob{
		flag:       byte(0x80), // 1000 0000
		errChan:    errChan,
		resultChan: resChan,
		pid:        pageId,
	}

	sh.workers.queue <- &job

	// fmt.Println("added job, waiting to receive result")
	err := <-errChan
	if err != nil {
		return nil, err
	}
	// fmt.Println("Received no error...")
	res := <-resChan

	errChan = nil
	resChan = nil

	return res, nil
}

// marks a frame as dirty and adds it to dirty list LRU
func (sh *shard) markFrameDirty(f *Frame) error {
	f.MarkDirty()
	fmt.Println("(MarkDirtyFrame) Adding frame to dirty list")
	sh.diryList.addDirtyFrame(f)

	return nil
}

// Retrieves a new frame from the buffer pool and assigns it a pageId/blockId.
// A new frame is automatically referenced/pinned to ensure it is not evicted
// when initializing so the caller has to ensure they unreference it after use
// by calling f.Unreference().
//
//	 parameters:
//		internal - true if new frame holds an internal node, else false
//		setAsRoot - if set to true, new pageId is set as the root.
//
//	 returns pointer to new frame, and error if any
func (sh *shard) newFrame(internal bool, setAsRoot bool, pid uint32) (*Frame, error) {
	// fmt.Printf("(%d) NewFrame: %d..\n", sh.shardId, pid)
	errChan := make(chan error)
	resChan := make(chan *Frame)

	flag := byte(0x00)
	if internal {
		flag |= 0x01 << 6
	}

	if setAsRoot {
		flag |= 0x01 << 5
	}

	job := bmJob{
		flag:       flag, // 1000 0000
		errChan:    errChan,
		resultChan: resChan,
		pid:        pid,
	}

	sh.workers.queue <- &job

	err := <-errChan
	if err != nil {
		return nil, err
	}

	res := <-resChan

	return res, nil
}

func (sh *shard) flushFreeList() error {
	sh.pager.FlushFreeList()
	return nil
}

// Prepares page for eviction by flushing page to disk
func (sh *shard) prepareForEviction(f *Frame) error {
	log.Println("lru.PrepareForEviction()")
	if f == nil {
		return BufferManagerError{"Provided frame is nil."}
	}

	err := f.Acquire(false)
	if err != nil {
		panic("deadlock acquiring exclusive lock")
	}
	defer f.Release(false)

	if !f.isDirty() {
		// frame not dirty, skip flushing to disk
		return nil
	}

	fBuff, _, err := f.RawBufferSlice()
	if err != nil {
		return err
	}

	err = sh.pager.WritePage(f.getKey(), fBuff, false)
	if err != nil {
		panic(err.Error())
	}

	return nil
}

// FIXME: Add buffer manager method to find shard where metadata page is stored
// calls disk manager to flush metadata to metadata page
func (sh *shard) FlushMetadata() error {
	sh.rmu.RLock()
	defer sh.rmu.RUnlock()

	metaEntry := sh.wTinyLfu.cBuffer.getReserved()
	if metaEntry == nil {
		return BufferManagerError{Message: "No metadata frame available"}
	}

	metaFrame := &metaEntry.entry
	err := metaFrame.Acquire(false)
	if err != nil {
		return err
	}
	defer metaFrame.Release(false)

	metaEntry.reference()
	defer metaEntry.unref()

	metaBuff, _, err := metaFrame.RawBufferSlice()
	if err != nil {
		return err
	}

	err = sh.pager.FlushMetadata(metaBuff)
	if err != nil {
		return err
	}

	return nil
}

func (sh *shard) close() error {
	// terminate  workers
	sh.workers.shutdownWorkers()
	// stop bgwriter
	if sh.exitchan != nil {
		helpers.PrintInfoMsg("terminating background writers...")
		close(*sh.exitchan)
		sh.wg.Wait()
	}

	if sh.wTinyLfu != nil {
		helpers.PrintInfoMsg("terminating wtinylfu...")
		sh.wTinyLfu.close()
		helpers.PrintInfoMsg("terminated wtinylfu successfully.")
	}

	return nil
}

func (j *bmJob) runJob(sh *shard) {
	// bit 7 set if Get() request
	if helpers.BitIsSet(&j.flag, 7) {
		sh.rmu.RLock()
		// fKey := toKey(key)
		if j.pid == 0 {
			metaFr := sh.wTinyLfu.getMetadataPage()
			if metaFr == nil {
				sh.rmu.RUnlock()
				j.errChan <- BufferManagerError{Message: "Unable to get metadata frame"}
				return
			}

			sh.rmu.RUnlock()
			j.errChan <- nil
			j.resultChan <- metaFr
			return
		}

		val, ok := sh.CacheMap[j.pid]

		if ok {
			// increment count & pin frame
			// fmt.Println("Item found, doing a GetIncrement()")
			sh.wTinyLfu.Increment(val)
			sh.rmu.RUnlock()

			val.reference()
			j.errChan <- nil
			j.resultChan <- &val.entry
			return
		} else {
			// get empty frame. this is where the page read from disk will be stored since memory is already allocated.
			// if window cache is full, a frame will be evicted.
			en, evicted, err := sh.wTinyLfu.getFreeFrame(sh.pager)
			if err != nil {
				sh.rmu.RUnlock()
				j.errChan <- err
				j.resultChan <- nil
				return
			}

			// acquire lock on frame before reading
			err = en.entry.Acquire(false)
			if err != nil {
				sh.rmu.RUnlock()
				j.errChan <- err
				j.resultChan <- nil
				return
			}
			defer en.entry.Release(false)

			// if there are keys evicted, delete from hash table
			if len(evicted) > 0 {
				// acquire write lock
				sh.rmu.RUnlock()
				sh.rmu.Lock()

				for _, ev := range evicted {
					delete(sh.CacheMap, ev)
				}
				sh.rmu.Unlock()
				sh.rmu.RLock()

				evicted = nil
			}

			// Retrieve buffer to read into
			// fmt.Println("Item not found, reading from disk")
			frBuff, _, err := en.entry.RawBufferSlice()
			if err != nil {
				sh.rmu.RUnlock()
				j.errChan <- err
				return
			}

			// Create read request
			err = sh.pager.ReadPage(uint32(j.pid), frBuff)
			if err != nil {
				// fmt.Println("Error encountered, readding to pool...")
				// readd frame to circular buffer pool
				sh.wTinyLfu.readdFrameToPool(en)
				sh.rmu.RUnlock()
				j.errChan <- err
				return
			}

			// invalid page/page doesn't exist
			if p := binary.LittleEndian.Uint32((*frBuff)[1:5]); p != j.pid {
				// fmt.Println("Invalid pageId provided, readding to pool...")
				// readd frame to circular buffer pool
				sh.wTinyLfu.readdFrameToPool(en)
				// fmt.Println("Readded to  pool successfully...")
				sh.rmu.RUnlock()
				// fmt.Println("released lock")
				j.errChan <- BufferManagerError{Message: fmt.Sprintf("Invalid pageId provided: %d, instead got %d\nFrame -> %v", j.pid, p, (*frBuff))}
				return
			}

			// set pageID from read page
			err = sh.pager.UpdatePageMeta(&en.entry.page, binary.LittleEndian.Uint32((*frBuff)[1:5]), helpers.BitIsSet(&((*frBuff)[0]), pager.IsInternal))
			if err != nil {
				j.errChan <- BufferManagerError{Message: err.Error()}
				return
			}

			// update frame metadata with page data
			err = en.entry.SetData(&en.entry.page, false)
			if err != nil {
				j.errChan <- BufferManagerError{Message: err.Error()}
				return
			}

			// fmt.Printf("FRAME BUFF AFTER READING: -> %v\nFRAME -> %v\n", frBuff, fr)

			//  add item  to cache
			sh.rmu.RUnlock()
			// fmt.Println("read page from disk, adding to cache...")
			// fmt.Printf("pageId: %d, Frame: %v\n", pageId, fr)
			f, err := sh.put(j.pid, en, false)
			if err != nil {
				j.errChan <- BufferManagerError{Message: "No item found"}
				return
			}

			// f.Reference()
			j.errChan <- nil
			j.resultChan <- f
			return
		}
	} else {
		if j.pid == 0 {
			j.errChan <- BufferManagerError{Message: "Invalid pid 0"}
		}

		// retrieve frame from buffer manager
		en, evicted, err := sh.wTinyLfu.getFreeFrame(sh.pager)
		if err != nil {
			j.errChan <- err
			return
		}
		// fmt.Println("retrieved free frame....")
		fr := &en.entry
		// fr.Reference()
		// defer fr.Unreference()
		err = fr.Acquire(false)
		if err != nil {
			en.unref()
			j.errChan <- err
			return
		}
		defer fr.Release(false)

		// if there are keys evicted, delete from hash table
		if len(evicted) > 0 {
			// acquire write lock
			//  fmt.Println("items evicted,, deleting from cachemap...")
			sh.rmu.Lock()

			for _, ev := range evicted {
				delete(sh.CacheMap, ev)
			}
			sh.rmu.Unlock()
			// fmt.Println("items evicted, deleted from cachemap...")
		}

		frPge := fr.GetPage()
		if frPge == nil {
			panic("No page allocated to frame")
		}

		// initialize page
		// fmt.Println("Initializing page...")
		pgeId, err := sh.pager.NewPage(helpers.BitIsSet(&j.flag, 6), helpers.BitIsSet(&j.flag, 5), frPge, j.pid)
		if err != nil {
			// fmt.Println("got error while initializing page...")
			en.unref()
			j.errChan <- err
			return
		}
		// fmt.Println("Initializing page, Done...")

		if pgeId == 0 {
			en.unref()
			// invalid state/wrong logic. Should never happen.
			panic("Invalid frame ID 0")
		}

		// update frame metadata
		// fmt.Println("setting frame data...")
		err = fr.SetData(frPge, true)
		if err != nil {
			en.unref()
			j.errChan <- err
			return
		}
		// fmt.Println("setting frame data, Done...")

		if fr.getKey() == 0 {
			panic("Invalid frame ID 0")
		}

		// Add to buffer manager cache
		// log.Println("Adding new page to cache...")
		_, err = sh.put(uint32(pgeId), en, true)
		// log.Printf("Added new page to cache. Page ID: %d\n", fr.getKey())
		if err != nil {
			j.errChan <- err
			return
		}
		// fmt.Printf("Created new frame with ID: %d\n", pgeId)
		// c.wTinyLfu.Stat()

		j.errChan <- nil
		j.resultChan <- fr
		return
	}
}

func (w *bmWorkerPool) startBmWorkers(poolSize uint64, sh *shard) error {
	if w.queue == nil {
		return BufferManagerError{Message: "buffer manager job queue not initialized"}
	}

	if poolSize == 0 {
		return BufferManagerError{Message: "Invalid pool size"}
	}

	for range poolSize {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()

			for j := range w.queue {
				(*j).runJob(sh)
				j = nil
			}

		}()
	}

	helpers.PrintInfoMsg(fmt.Sprintf("(shard) Started %d buffer manager workers", poolSize))

	return nil
}

func (w *bmWorkerPool) shutdownWorkers() {
	if w.queue != nil {
		fmt.Println("terminating buffer manager workers...")
		close(w.queue)
		w.wg.Wait()
		fmt.Println("terminated all buffer manager workers...")
	}
}

// newShard creates a new shard
func newShard(shardId uint64, wal *wal.WAL, pgr *pager.Pager, cacheSize uint64, reserveMetadataEntry bool) (*shard, error) {
	if wal == nil {
		return nil, BufferManagerError{Message: "wal instance  not provided."}
	}

	if pgr == nil {
		return nil, BufferManagerError{Message: "pager instance  not provided."}
	}

	totFrames := math.Round(float64(cacheSize*1024) / float64(unsafe.Sizeof(clockentry{})))
	windSize := math.Round(totFrames * WINDOW_CACHE_RATIO)
	mainSize := math.Round(totFrames * (1 - WINDOW_CACHE_RATIO))
	w, err := NewWTinylfu(uint64(windSize), uint64(mainSize), reserveMetadataEntry)
	if err != nil {
		panic(err)
	}

	dList := NewDirtyPageList()

	if reserveMetadataEntry {
		metadataEntry := w.cBuffer.getReserved()
		if metadataEntry == nil {
			return nil, BufferManagerError{Message: "Unable to initialize metadata frame"}
		}

		metadataPage := metadataEntry.entry.GetPage()
		if metadataPage == nil {
			return nil, BufferManagerError{Message: "Unable to initialize metadata buffer."}
		}
	}

	// worker pool
	wrk := bmWorkerPool{
		queue: make(chan *bmJob, int(windSize)),
	}

	// exit channel
	exChan := make(chan struct{})
	sh := &shard{
		CacheMap: make(map[uint32]*clockentry),
		wTinyLfu: w,
		diryList: dList,
		pager:    pgr,
		workers:  &wrk,
		exitchan: &exChan,
		shardId:  shardId,
	}

	// start workers
	if err = wrk.startBmWorkers(uint64(windSize), sh); err != nil {
		sh.close()
		return nil, BufferManagerError{Message: "Unable to start workers"}
	}

	// create new background writer
	bg := NewBgWriter(sh, wal, w.cBuffer, pgr)
	go bg.Start(&sh.wg)

	return sh, nil
}
