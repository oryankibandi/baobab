package buffermanager

import (
	"log"
	"sync"
	"time"

	"github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

const (
	// Max # of pages to flush in one operation
	MAX_PAGES = 100
	// delay between BgWriter activity writes (Milliseconds)
	BGWRITER_DELAY = 200
	// size after which to force flush to disk(bypass OS cache) size after which to force flush to disk(bypass OS cache (Bytes)
	BG_FLUSH_AFTER        = 65536
	FREELIST_WRITER_DELAY = 5000
	// threshold for how long writing a page to disk should take.
	PAGE_WRITE_TIMEOUT = 200
)

type BgWriter struct {
	writtenPages uint32
	writtenBytes uint32
	cache        *BufferManager
	wal          *wal.WAL
	pgr          *pager.Pager

	// Head of the clock buffer
	clockBuffer *clock
	mu          sync.Mutex
	wg          sync.WaitGroup
}

func (bw *BgWriter) Start() {
	// Give time for other processes to initialize
	time.Sleep(time.Second * 60)
	go bw.watchFreeList()

	var currFrame *Frame
	var nextFrame *Frame
	var currFrameBuff *[]byte
	for {
		for range bw.clockBuffer.getCap() {
			currFrame = bw.clockBuffer.clockHead()

			if currFrame.isPinned() {
				currFrame = currFrame.GetNextLink()
				continue
			}

			if !currFrame.isDirty() {
				currFrame = currFrame.GetNextLink()
				continue
			}

			err := currFrame.Acquire(false)
			if err != nil {
				panic("Deadlock acquiring exlusive latch on frame")
			}
			currFrame.Reference()

			currFrameBuff, _, err = currFrame.RawBufferSlice()
			if err != nil {
				panic("Unable to get frame buffer")
			}

			err = bw.pgr.WritePage(currFrame.getKey(), currFrameBuff)
			if err != nil {
				log.Printf("Error writing to page: %s\n", err.Error())
				currFrame = currFrame.GetNextLink()
				continue
			}

			nextFrame = currFrame.next
			currFrame.Unreference()
			currFrame.Release(false)
			bw.writtenPages++

			if bw.writtenPages >= MAX_PAGES {
				break
			}

			currFrame = nextFrame
		}
		time.Sleep(time.Millisecond * BGWRITER_DELAY)
	}
}

// periodically flushes free list to disk if dirty
func (bw *BgWriter) watchFreeList() {
	for {
		bw.cache.flushFreeList()
		time.Sleep(time.Millisecond * FREELIST_WRITER_DELAY)
	}
}

func NewBgWriter(cache *BufferManager, wal *wal.WAL, clockBuffHead *clock, pgr *pager.Pager) *BgWriter {
	return &BgWriter{
		cache:       cache,
		wal:         wal,
		clockBuffer: clockBuffHead,
		pgr:         pgr,
	}
}
