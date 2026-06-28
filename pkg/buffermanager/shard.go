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

// job flags
const (
	GET_CREATE = 7
	DELETE     = 6
	ISINTERNAL = 5
	SETASROOT  = 4
)

type bmJob struct {
	// bit 7 set if Get(), bit 6 set if delete(). createNewFrame if neither set
	flag       byte
	errChan    chan error
	resultChan chan bmWorkerRes
	cacheHit   chan bool
	pid        uint32
}

type bmWorkerPool struct {
	queue chan *bmJob
	wg    sync.WaitGroup
}

type bmWorkerRes struct {
	fr       *Frame
	cacheHit bool
}

type shard struct {
	CacheMap   sync.Map
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
	val, ok := sh.CacheMap.Load(k)
	if ok {
		// increment count
		sh.wTinyLfu.Increment(val.(*clockentry))
	}

	sh.CacheMap.Store(k, en)

	if en.entry.parentEntry == nil {
		// set parent entry ptr
		en.entry.parentEntry = en
	}

	val, ok = sh.CacheMap.Load(k)

	return &en.entry, nil
}

// 2. Retrieve item from cache
func (sh *shard) get(pageId uint32) (*Frame, bool, error) {
	errChan := make(chan error)
	resChan := make(chan bmWorkerRes)
	cacheHitChan := make(chan bool)
	job := bmJob{
		flag:       byte(0x80), // 1000 0000
		errChan:    errChan,
		resultChan: resChan,
		cacheHit:   cacheHitChan,
		pid:        pageId,
	}

	sh.workers.queue <- &job

	select {
	case e := <-errChan:
		return nil, false, e
	case r := <-resChan:
		return r.fr, r.cacheHit, nil
	}
}

// delete deletes the key with pid from the shard
func (sh *shard) delete(pid uint32) error {
	errChan := make(chan error)
	job := bmJob{
		flag:    byte(0x40), // 0100 0000
		errChan: errChan,
		pid:     pid,
	}

	sh.workers.queue <- &job

	e := <-errChan
	return e
}

// marks a frame as dirty and adds it to dirty list LRU
func (sh *shard) markFrameDirty(f *Frame) error {
	f.MarkDirty()
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
	errChan := make(chan error)
	resChan := make(chan bmWorkerRes)

	flag := byte(0x00)
	if internal {
		flag |= 0x01 << ISINTERNAL
	}

	if setAsRoot {
		flag |= 0x01 << SETASROOT
	}

	job := bmJob{
		flag:       flag, // 1000 0000
		errChan:    errChan,
		resultChan: resChan,
		pid:        pid,
	}

	sh.workers.queue <- &job

	select {
	case e := <-errChan:
		return nil, e
	case r := <-resChan:
		return r.fr, nil
	}
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
	sh.workers.shutdownWorkers(sh.shardId)
	// stop bgwriter
	fmt.Printf("(shard %d) shutting down bgwriters....\n", sh.shardId)
	if sh.exitchan != nil {
		close(*sh.exitchan)
		sh.wg.Wait()
	}

	if sh.wTinyLfu != nil {
		sh.wTinyLfu.close()
	}

	return nil
}

func (j *bmJob) runJob(sh *shard) {
	// bit 6 set if delete() request
	if helpers.BitIsSet(&j.flag, DELETE) {
		val, ok := sh.CacheMap.Load(j.pid)
		if ok {
			val.(*clockentry).reference()
			val.(*clockentry).entry.Acquire(false)
			val.(*clockentry).entry.MarkDead()
			val.(*clockentry).entry.Release(false)
			val.(*clockentry).unref()
		} else {
			// cache miss, read from disk and add to cache
			en, err := sh.wTinyLfu.getFreeFrame(sh.pager, &sh.CacheMap)
			if err != nil {
				j.errChan <- err
				return
			}

			// acquire lock on frame before reading
			err = en.entry.Acquire(false)
			if err != nil {
				j.errChan <- err
				return
			}
			defer en.entry.Release(false)

			en.unMarkForEviction()

			// Retrieve buffer to read into
			frBuff, _, err := en.entry.RawBufferSlice()
			if err != nil {
				j.errChan <- err
				return
			}

			// Create read request
			err = sh.pager.ReadPage(uint32(j.pid), frBuff)
			if err != nil {
				// readd frame to circular buffer pool
				sh.wTinyLfu.readdFrameToPool(en)
				j.errChan <- err
				return
			}

			// invalid page/page doesn't exist
			if p := binary.LittleEndian.Uint32((*frBuff)[1:5]); p != j.pid {
				// readd frame to circular buffer pool
				sh.wTinyLfu.readdFrameToPool(en)
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

			_, err = sh.put(j.pid, en, false)
			if err != nil {
				en.unref()
				j.errChan <- err
			}
			// mark as dead
			en.entry.MarkDead()
			en.unref()
		}
		j.errChan <- nil
		return
	}
	// bit 7 set if Get() request
	if helpers.BitIsSet(&j.flag, GET_CREATE) {
		sh.rmu.RLock()
		defer sh.rmu.RUnlock()
		// fKey := toKey(key)
		if j.pid == 0 {
			metaFr := sh.wTinyLfu.getMetadataPage()
			if metaFr == nil {
				j.errChan <- BufferManagerError{Message: "Unable to get metadata frame"}
				return
			}

			j.resultChan <- bmWorkerRes{fr: metaFr, cacheHit: true}
			return
		}

		val, ok := sh.CacheMap.Load(j.pid)

		// if entry exists AND it is not marked for eviction
		if ok && val.(*clockentry).refIfOccupied() {
			// check if entry is already deleted
			if val.(*clockentry).entry.IsDead() {
				// frame deleted, remove from hash table, unref and return err
				if !val.(*clockentry).entry.isDirty() {
					sh.CacheMap.Delete(j.pid)
				}
				val.(*clockentry).unref()
				j.errChan <- BufferManagerError{Message: "No entry found."}
				return
			}
			// increment count & pin frame
			sh.wTinyLfu.Increment(val.(*clockentry))

			j.resultChan <- bmWorkerRes{fr: &val.(*clockentry).entry, cacheHit: true}
			return
		} else {
			// get empty frame. this is where the page read from disk will be stored since memory is already allocated.
			// if window cache is full, a frame will be evicted.
			en, err := sh.wTinyLfu.getFreeFrame(sh.pager, &sh.CacheMap)
			if err != nil {
				j.errChan <- err
				return
			}

			// acquire lock on frame before reading
			err = en.entry.Acquire(false)
			if err != nil {
				j.errChan <- err
				return
			}
			defer en.entry.Release(false)

			en.unMarkForEviction()

			// Retrieve buffer to read into
			frBuff, _, err := en.entry.RawBufferSlice()
			if err != nil {
				j.errChan <- err
				return
			}

			// Create read request
			err = sh.pager.ReadPage(uint32(j.pid), frBuff)
			if err != nil {
				// readd frame to circular buffer pool
				sh.wTinyLfu.readdFrameToPool(en)
				j.errChan <- err
				return
			}

			// invalid page/page doesn't exist
			if p := binary.LittleEndian.Uint32((*frBuff)[1:5]); p != j.pid {
				// readd frame to circular buffer pool
				sh.wTinyLfu.readdFrameToPool(en)
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

			f, err := sh.put(j.pid, en, false)
			if err != nil {
				j.errChan <- BufferManagerError{Message: "No item found"}
				return
			}

			j.resultChan <- bmWorkerRes{fr: f, cacheHit: false}
			return
		}
	} else {
		// create new frame
		if j.pid == 0 {
			j.errChan <- BufferManagerError{Message: "Invalid pid 0"}
		}

		// retrieve frame from buffer manager
		en, err := sh.wTinyLfu.getFreeFrame(sh.pager, &sh.CacheMap)
		if err != nil {
			sh.wTinyLfu.Stat()
			j.errChan <- err
			return
		}
		fr := &en.entry
		err = fr.Acquire(false)
		if err != nil {
			en.unref()
			j.errChan <- err
			return
		}
		defer fr.Release(false)

		en.unMarkForEviction()

		frPge := fr.GetPage()
		if frPge == nil {
			panic("No page allocated to frame")
		}

		// initialize page
		pgeId, err := sh.pager.NewPage(helpers.BitIsSet(&j.flag, SETASROOT), helpers.BitIsSet(&j.flag, ISINTERNAL), frPge, j.pid)
		if err != nil {
			en.unref()
			j.errChan <- err
			return
		}

		if pgeId == 0 {
			en.unref()
			// invalid state/wrong logic. Should never happen.
			panic("Invalid frame ID 0")
		}

		// update frame metadata
		err = fr.SetData(frPge, true)
		if err != nil {
			en.unref()
			j.errChan <- err
			return
		}

		if fr.getKey() == 0 {
			panic("Invalid frame ID 0")
		}

		// Add to buffer manager cache
		_, err = sh.put(uint32(pgeId), en, true)
		if err != nil {
			j.errChan <- err
			return
		}

		j.resultChan <- bmWorkerRes{fr: fr}
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
		go func(id uint64) {
			defer w.wg.Done()

			for j := range w.queue {
				(*j).runJob(sh)
				j = nil
			}

		}(sh.shardId)
	}

	helpers.PrintInfoMsg(fmt.Sprintf("(shard) Started %d buffer manager workers", poolSize))

	return nil
}

func (w *bmWorkerPool) shutdownWorkers(shardId uint64) {
	if w.queue != nil {
		close(w.queue)
		w.wg.Wait()
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

	workerPoolSize := 400

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
		// queue: make(chan *bmJob, int(windSize)),
		queue: make(chan *bmJob, workerPoolSize),
	}

	// exit channel
	exChan := make(chan struct{})
	sh := &shard{
		CacheMap: sync.Map{},
		wTinyLfu: w,
		diryList: dList,
		pager:    pgr,
		workers:  &wrk,
		exitchan: &exChan,
		shardId:  shardId,
	}

	// start workers
	if err = wrk.startBmWorkers(uint64(workerPoolSize), sh); err != nil {
		sh.close()
		return nil, BufferManagerError{Message: "Unable to start workers"}
	}

	// create new background writer
	bg := NewBgWriter(sh, wal, w.cBuffer, pgr)
	go bg.Start(&sh.wg)

	return sh, nil
}
