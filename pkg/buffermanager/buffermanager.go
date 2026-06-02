package buffermanager

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"

	"github.com/oryankibandi/baobab/pkg/helpers"
	pager "github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"

	tiny "github.com/oryankibandi/baobab/internal/tinylfu"
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
	MIN_CACHE_SIZE_KB = 8192 // 65536 // 64MB
)

type CacheConfig struct {
	// size of the cache in KB. Minimum and default size is 8192KB(8MB)
	CacheSize uint64
	// No. of threads to run. Defaults to all available threads.
	numThreads uint64
}

type BufferManager struct {
	// CacheMap   map[uint32]*clockentry
	// wTinyLfu   *WTinyLfu
	mu         sync.RWMutex
	frameCount uint32
	// diryList   *dPages
	pager *pager.Pager
	// xxhash hasher
	hasher tiny.Hasher
	// // exit channel
	// exitchan *chan struct{}

	// // workers
	// workers *bmWorkerPool

	// // wait group
	wg        sync.WaitGroup
	shards    []*shard
	numShards uint64
}

// Delete an item from cache. flush parameter is set to true if it's a direct request from client. If bgwriter, it is false since the page is already flushed.
// func (c *BufferManager) Delete(key uint32, flush bool) error {
// 	c.rmu.Lock()
// 	defer c.rmu.Unlock()
// 	val, ok := c.CacheMap[key]
// 	if !ok {
// 		return BufferManagerError{Message: "No key in cache"}
// 	}
//
// 	if flush && val.isDirty() {
// 		err := c.prepareForEviction(val)
// 		fmt.Println("(Delete) prepared for eviction...")
//
// 		if err != nil {
// 			panic(err)
// 		}
//
// 		// delete from dirty page list
// 		c.diryList.removePage(key)
// 	}
//
// 	delete(c.CacheMap, key)
// 	c.freeFrames++
//
// 	if c.frameCount > 0 {
// 		c.frameCount++
// 	}
//
// 	fmt.Println("c.Delete() DONE>..")
// 	return nil
// }

func (c *BufferManager) GetRootPageId() uint32 {
	return c.pager.RootPage()
}

// Get retrieves frame with the provided pid
func (c *BufferManager) Get(pid uint32) (f *Frame, e error) {
	frChan := make(chan *Frame)
	erChan := make(chan error)
	idx := c.getShard(pid)

	go func(fChan chan *Frame, eChan chan error) {
		fr, err := c.shards[idx].get(pid)
		eChan <- err
		fChan <- fr
	}(frChan, erChan)

	err := <-erChan
	fr := <-frChan
	if err != nil {
		return nil, err
	}

	if fr == nil {
		panic("Could not get frame")
	}

	return fr, nil
}

// Get retrieves frame with the provided pid
func (c *BufferManager) NewFrame(internal bool, setAsRoot bool) (*Frame, error) {
	frChan := make(chan *Frame)
	erChan := make(chan error)

	newPid := c.pager.NewPageId()
	idx := c.getShard(newPid)

	go func(fChan chan *Frame, eChan chan error) {
		fr, err := c.shards[idx].newFrame(internal, setAsRoot, newPid)
		eChan <- err
		fChan <- fr
	}(frChan, erChan)

	err := <-erChan
	fr := <-frChan
	if err != nil {
		return nil, err
	}

	if fr == nil {
		panic("Could not get frame")
	}

	return fr, nil
}

func (c *BufferManager) MarkFrameDirty(f *Frame) error {
	idx := c.getShard(f.getKey())
	return c.shards[idx].markFrameDirty(f)
}

// Calls ForceFlush on disk manager
func (c *BufferManager) flushWritten() {
	c.pager.Flush()
}

func (c *BufferManager) flushShardsFreeList() {
	for _, s := range c.shards {
		s.flushFreeList()
	}
}

// sets the frame's page ID as the new root on the disk manager
func (c *BufferManager) SetNewRoot(f *Frame) error {
	f.Acquire(true)
	pId := f.getKey()
	f.Release(true)

	c.pager.UpdateRootPage(pId)

	return nil
}

// getShard hashes provided key and returns the index of the shard possibly
// containing the key
func (c *BufferManager) getShard(key uint32) uint64 {
	kBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(kBytes, key)
	return c.hasher.Sum64(kBytes) % c.numShards
}

// Safely close down cache
func (c *BufferManager) Close() error {
	c.wg.Add(int(c.numShards))
	for _, sh := range c.shards {
		go func() {
			defer c.wg.Done()
			sh.close()
		}()
	}
	c.wg.Wait()

	if c.pager != nil {
		helpers.PrintInfoMsg("terminating pager...")
		c.pager.Close()
		helpers.PrintInfoMsg("pager terminated successfully.")
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

	numCPU := runtime.NumCPU()

	if cacheConfig.numThreads < uint64(numCPU) && cacheConfig.numThreads != 0 {
		numCPU = int(cacheConfig.numThreads)
	}

	helpers.PrintInfoMsg(fmt.Sprintf("Starting buffer manager in %d threads", numCPU))
	perShardCacheSize := cacheConfig.CacheSize / uint64(numCPU)

	bufferMan := BufferManager{
		numShards: uint64(numCPU),
		shards:    make([]*shard, numCPU),
		hasher:    tiny.NewMapHash(),
		pager:     pgr,
	}

	metadataShard := bufferMan.getShard(0)

	for i := range numCPU {
		sh, err := newShard(uint64(i), wal, pgr, perShardCacheSize, i == int(metadataShard))
		if err != nil {
			return nil, err
		}

		bufferMan.shards[i] = sh
	}

	return &bufferMan, nil
}

func toBytes(key uint32) []byte {
	b := make([]byte, 4)

	binary.LittleEndian.PutUint32(b, key)

	return b
}
