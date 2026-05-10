package buffermanager

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	//"fmt"
	"log"
	"sync"

	"github.com/oryankibandi/baobab/pkg/helpers"
	pager "github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

// Ideally, window cache size ≈ 10%, main cache ≈ 90%
const (
	// WINDOW_CACHE_SIZE  = 20
	WINDOW_CACHE_RATIO = 0.1
	// MAIN_CACHE_SIZE    = 800
	CACHE_KEY_SIZE = 16
	// CACHE_RATIO        = 0.25
	LSN_SIZE = 12
	// minimum cache size in KB
	MIN_CACHE_SIZE_KB = 8192
)

type CacheConfig struct {
	// size of the cache in KB. Minimum and default size is 8192KB(8MB)
	CacheSize uint64
}

type BufferManager struct {
	CacheMap   map[uint32]*Frame
	wTinyLfu   *WTinyLfu
	rmu        sync.RWMutex
	frameCount uint32
	freeFrames uint32
	diryList   *dPages
	pager      *pager.Pager
	// exit channel
	exitchan *chan struct{}

	// wait group
	wg sync.WaitGroup
}

// Adds a page to cache, setting k as key. k is the unique page ID.
func (c *BufferManager) put(k uint32, fr *Frame, dirty bool) (*Frame, error) {
	fmt.Println("cache.Put")
	c.rmu.Lock()
	fmt.Println("cache.put() acquired xclusive latch")

	val, ok := c.CacheMap[k]
	if ok {
		// increment count
		c.wTinyLfu.Increment(val)
	}

	c.CacheMap[k] = fr

	log.Println("Printing stats ....")
	c.rmu.Unlock()
	//go c.wTinyLfu.Stat()
	return fr, nil
}

// 2. Retrieve item from cache
func (c *BufferManager) Get(pageId uint32) (*Frame, error) {
	c.rmu.RLock()
	// fKey := toKey(key)
	if pageId == 0 {
		metaFr := c.wTinyLfu.getMetadataPage()
		if metaFr == nil {
			c.rmu.RUnlock()
			return nil, BufferManagerError{Message: "Unable to get metadata frame"}
		}

		c.rmu.RUnlock()
		return metaFr, nil
	}

	val, ok := c.CacheMap[pageId]

	if ok {
		// increment count & pin frame
		// fmt.Println("Item found, doing a GetIncrement()")
		c.wTinyLfu.Increment(val)
		c.rmu.RUnlock()

		val.Reference()
		return val, nil
	} else {
		// get empty frame. this is where the page read from disk will be stored since memory is already allocated.
		// if window cache is full, a frame will be evicted.
		fr, evicted, err := c.wTinyLfu.getFreeFrame(c.pager)
		if err != nil {
			c.rmu.RUnlock()
			return nil, err
		}

		// if there are keys evicted, delete from hash table
		if len(evicted) > 0 {
			// acquire write lock
			c.rmu.RUnlock()
			c.rmu.Lock()

			for _, ev := range evicted {
				delete(c.CacheMap, ev)
			}
			c.rmu.Unlock()
			c.rmu.RLock()

			evicted = nil
		}

		// acquire lock on frame before reading
		err = fr.Acquire(false)
		if err != nil {
			c.rmu.RUnlock()
			return nil, err
		}
		defer fr.Release(false)

		// Retrieve buffer to read into
		// fmt.Println("Item not found, reading from disk")
		frBuff, _, err := fr.RawBufferSlice()
		if err != nil {
			c.rmu.RUnlock()
			return nil, err
		}

		// Create read request
		err = c.pager.ReadPage(uint32(pageId), frBuff)
		if err != nil {
			// fmt.Println("Error encountered, readding to pool...")
			// readd frame to circular buffer pool
			c.wTinyLfu.readdFrameToPool(fr)
			c.rmu.RUnlock()
			return nil, err
		}

		// invalid page/page doesn't exist
		if binary.LittleEndian.Uint32((*frBuff)[1:5]) != pageId {
			// fmt.Println("Invalid pageId provided, readding to pool...")
			// readd frame to circular buffer pool
			c.wTinyLfu.readdFrameToPool(fr)
			// fmt.Println("Readded to  pool successfully...")
			c.rmu.RUnlock()
			// fmt.Println("released lock")
			return nil, BufferManagerError{Message: "Invalid pageId provided."}
		}

		// set pageID from read page
		err = c.pager.UpdatePageMeta(&fr.page, binary.LittleEndian.Uint32((*frBuff)[1:5]), helpers.BitIsSet(&((*frBuff)[0]), pager.IsInternal))
		if err != nil {
			return nil, BufferManagerError{Message: err.Error()}
		}

		// update frame metadata with page data
		err = fr.SetData(&fr.page)
		if err != nil {
			return nil, BufferManagerError{Message: err.Error()}
		}

		// fmt.Printf("FRAME BUFF AFTER READING: -> %v\nFRAME -> %v\n", frBuff, fr)

		//  add item  to cache
		c.rmu.RUnlock()
		// fmt.Println("read page from disk, adding to cache...")
		// fmt.Printf("pageId: %d, Frame: %v\n", pageId, fr)
		f, err := c.put(pageId, fr, false)
		if err != nil {
			return nil, BufferManagerError{Message: "No item found"}
		}

		// f.Reference()
		return f, nil
	}
}

// Delete an item from cache. flush parameter is set to true if it's a direct request from client. If bgwriter, it is false since the page is already flushed.
func (c *BufferManager) Delete(key uint32, flush bool) error {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	val, ok := c.CacheMap[key]
	if !ok {
		return BufferManagerError{Message: "No key in cache"}
	}

	if flush && val.isDirty() {
		err := c.prepareForEviction(val)
		fmt.Println("(Delete) prepared for eviction...")

		if err != nil {
			panic(err)
		}

		// delete from dirty page list
		c.diryList.removePage(key)
	}

	delete(c.CacheMap, key)
	c.freeFrames++

	if c.frameCount > 0 {
		c.frameCount++
	}

	fmt.Println("c.Delete() DONE>..")
	return nil
}

func (c *BufferManager) GetRootPageId() uint32 {
	return c.pager.RootPage()
}

// marks a frame as dirty and adds it to dirty list LRU
func (c *BufferManager) MarkFrameDirty(f *Frame) error {
	f.MarkDirty()
	fmt.Println("(MarkDirtyFrame) Adding frame to dirty list")
	c.diryList.addDirtyFrame(f)

	return nil
}

// Retrieves a new frame from the buffer pool and assigns it a pageId/blockId.
// A new frame is automatically referenced to ensure it is not evicted
// when initializing so the caller has to ensure they unreference it after use
// by calling f.Unreference().
//
//	 parameters:
//		internal - true if new frame holds an internal node, else false
//		setAsRoot - if set to true, new pageId is set as the root.
//
//	 returns pointer to new frame, and error if any
func (c *BufferManager) NewFrame(internal bool, setAsRoot bool) (*Frame, error) {
	fmt.Println("Creating new frame.....")
	// retrieve frame from buffer manager
	fr, evicted, err := c.wTinyLfu.getFreeFrame(c.pager)
	if err != nil {
		return nil, err
	}
	fmt.Println("retrieved free frame....")
	// fr.Reference()
	// defer fr.Unreference()
	err = fr.Acquire(false)
	if err != nil {
		fr.Unreference()
		return nil, err
	}
	defer fr.Release(false)

	// if there are keys evicted, delete from hash table
	if len(evicted) > 0 {
		// acquire write lock
		fmt.Println("items evicted,, deleting from cachemap...")
		c.rmu.Lock()

		for _, ev := range evicted {
			delete(c.CacheMap, ev)
		}
		c.rmu.Unlock()
		fmt.Println("items evicted, deleted from cachemap...")
	}

	frPge := fr.GetPage()
	if frPge == nil {
		panic("No page allocated to frame")
	}

	// initialize page
	fmt.Println("Initializing page...")
	pgeId, err := c.pager.NewPage(setAsRoot, internal, frPge)
	if err != nil {
		fmt.Println("got error while initializing page...")
		fr.Unreference()
		return nil, err
	}
	fmt.Println("Initializing page, Done...")

	if pgeId == 0 {
		fr.Unreference()
		// invalid state/wrong logic. Should never happen.
		panic("Invalid frame ID 0")
	}

	// update frame metadata
	fmt.Println("setting frame data...")
	err = fr.SetData(frPge)
	if err != nil {
		fr.Unreference()
		return nil, err
	}
	fmt.Println("setting frame data, Done...")

	if fr.getKey() == 0 {
		panic("Invalid frame ID 0")
	}

	// Add to buffer manager cache
	log.Println("Adding new page to cache...")
	_, err = c.put(uint32(pgeId), fr, true)
	log.Printf("Added new page to cache. Page ID: %d\n", fr.getKey())
	if err != nil {
		return nil, err
	}
	fmt.Printf("Created new frame with ID: %d\n", pgeId)
	c.wTinyLfu.Stat()

	return fr, nil
}

// Calls ForceFlush on disk manager
func (c *BufferManager) flushWritten() {
	c.pager.Flush()
}

func (c *BufferManager) flushFreeList() error {
	c.pager.FlushFreeList()
	return nil
}

// Prepares page for eviction by flushing page to disk
func (c *BufferManager) prepareForEviction(f *Frame) error {
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

	err = c.pager.WritePage(f.getKey(), fBuff)
	if err != nil {
		panic(err.Error())
	}

	return nil
}

// sets the frame's page ID as the new root on the disk manager
func (c *BufferManager) SetNewRoot(f *Frame) error {
	f.Acquire(true)
	pId := f.getKey()
	f.Release(true)

	c.pager.UpdateRootPage(pId)

	return nil
}

// calls disk manager to flush metadata to metadata page
func (c *BufferManager) FlushMetadata() error {
	c.rmu.RLock()
	defer c.rmu.RUnlock()

	metaFrame := c.wTinyLfu.cBuffer.getReserved()
	if metaFrame == nil {
		return BufferManagerError{Message: "No metadata frame available"}
	}

	err := metaFrame.Acquire(false)
	if err != nil {
		return err
	}
	defer metaFrame.Release(false)

	metaFrame.Reference()
	defer metaFrame.Unreference()

	metaBuff, _, err := metaFrame.RawBufferSlice()
	if err != nil {
		return err
	}

	err = c.pager.FlushMetadata(metaBuff)
	if err != nil {
		return err
	}

	return nil
}

// Safely close down cache
func (c *BufferManager) Close() error {
	// stop bgwriter
	if c.exitchan != nil {
		helpers.PrintInfoMsg("terminating background writers...")
		close(*c.exitchan)
		c.wg.Wait()
	}

	if c.wTinyLfu != nil {
		c.wTinyLfu.close()
	}

	if c.pager != nil {
		c.pager.Close()
	}

	return nil
}

// NewCache Create new cache instance\n windowSize, probationSize and protectedSize are sized of the individual segments
//
//	Parameters:
//	cacheSize - size of the cache in KB. Minimum and default size is 8192KB(8MB)
//	wal - initialized wal instance
//	config - disk manager config
//
// Returns:
func NewBufferManager(cacheConfig CacheConfig, wal *wal.WAL, pgr *pager.Pager) (*BufferManager, error) {
	if cacheConfig.CacheSize < MIN_CACHE_SIZE_KB {
		return nil, BufferManagerError{Message: fmt.Sprintf("Minimum cache size is %dKB", MIN_CACHE_SIZE_KB)}
	}

	if wal == nil {
		return nil, BufferManagerError{Message: "wal instance  not provided."}
	}

	if pgr == nil {
		return nil, BufferManagerError{Message: "pager instance  not provided."}
	}

	totFrames := math.Round(float64(cacheConfig.CacheSize*1024) / float64(unsafe.Sizeof(Frame{})))
	windSize := math.Round(totFrames * WINDOW_CACHE_RATIO)
	mainSize := math.Round(totFrames * (1 - WINDOW_CACHE_RATIO))
	w, err := NewWTinylfu(uint64(windSize), uint64(mainSize))
	if err != nil {
		panic(err)
	}

	dList := NewDirtyPageList()

	metadataFr := w.cBuffer.getReserved()
	if metadataFr == nil {
		return nil, BufferManagerError{Message: "Unable to initialize metadata frame"}
	}
	metadataPage := metadataFr.GetPage()
	if metadataPage == nil {
		return nil, BufferManagerError{Message: "Unable to initialize metadata buffer."}
	}

	// exit channel
	exChan := make(chan struct{})
	n := BufferManager{
		CacheMap: make(map[uint32]*Frame),
		wTinyLfu: w,
		diryList: dList,
		pager:    pgr,
		exitchan: &exChan,
	}

	// create new background writer
	bg := NewBgWriter(&n, wal, w.cBuffer, pgr)
	go bg.Start(&n.wg)

	return &n, nil
}

func toBytes(key uint32) []byte {
	b := make([]byte, 4)

	binary.LittleEndian.PutUint32(b, key)

	return b
}
