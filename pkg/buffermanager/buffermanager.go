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
	// No. of shards to run. Defaults to all available threads.
	numShards uint64
}

type BufferManager struct {
	mu         sync.RWMutex
	frameCount uint32
	pager      *pager.Pager
	hasher     tiny.Hasher

	// wait group
	wg        sync.WaitGroup
	shards    []*shard
	numShards uint64
}

// Delete removes the frame with pid provided.
func (c *BufferManager) Delete(pid uint32) error {
	idx := c.getShard(pid)

	err := c.shards[idx].delete(pid)
	return err
}

func (c *BufferManager) GetRootPageId() uint32 {
	return c.pager.RootPage()
}

// Get retrieves frame with the provided pid
func (c *BufferManager) Get(pid uint32) (f *Frame, cHit bool, e error) {
	idx := c.getShard(pid)

	fr, hit, err := c.shards[idx].get(pid)
	if err != nil {
		return nil, false, err
	}

	if fr == nil {
		panic("Could not get frame")
	}

	return fr, hit, nil
}

func (c *BufferManager) GetFromShard(pid uint32, shardId uint64) (f *Frame, e error) {
	fr, _, err := c.shards[shardId].get(pid)
	if err != nil {
		return nil, err
	}

	if fr == nil {
		panic("Could not get frame")
	}

	return fr, nil
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
func (c *BufferManager) NewFrame(internal bool, setAsRoot bool) (*Frame, error) {
	newPid := c.pager.NewPageId()
	idx := c.getShard(newPid)

	fr, err := c.shards[idx].newFrame(internal, setAsRoot, newPid)

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
	for _, sh := range c.shards {
		c.wg.Add(1)
		go func() {
			fmt.Printf("(shard %d/%d) terminating shard...\n", sh.shardId, c.numShards)
			defer c.wg.Done()
			sh.close()
			fmt.Printf("(shard %d) shard terminated...\n", sh.shardId)
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
//	testing - if in testmode the provided numShard is used rather than limiting
//		  to numCPU
//
// Returns:
func NewBufferManager(cacheConfig CacheConfig, wal *wal.WAL, pgr *pager.Pager, testing bool) (*BufferManager, error) {
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

	if cacheConfig.numShards < uint64(numCPU) && !testing || cacheConfig.numShards == 0 {
		cacheConfig.numShards = uint64(numCPU)
	}

	helpers.PrintInfoMsg(fmt.Sprintf("Starting buffer manager in %d threads", cacheConfig.numShards))
	perShardCacheSize := cacheConfig.CacheSize / cacheConfig.numShards

	bufferMan := BufferManager{
		numShards: cacheConfig.numShards,
		shards:    make([]*shard, cacheConfig.numShards),
		hasher:    tiny.NewMapHash(),
		pager:     pgr,
	}

	metadataShard := bufferMan.getShard(0)

	for i := range cacheConfig.numShards {
		sh, err := newShard(uint64(i), wal, pgr, perShardCacheSize, i == metadataShard)
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
