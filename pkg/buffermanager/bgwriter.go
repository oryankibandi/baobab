package buffermanager

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/oryankibandi/baobab/pkg/diskmanager"
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
	cache        *Cache
	wal          *wal.WAL
	dManager     *diskmanager.DiskManager

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

			currFrame.mu.Lock()
			currFrame.Reference()

			wr := make(chan int32)
			// lsnChan := make(chan [diskmanager.LSN_SIZE_BYTE]byte)
			err := bw.dManager.WriteReq(&currFrame.page, currFrame.getKey(), wr, nil)

			if err != nil {
				log.Printf("Error writing to page: %s\n", err.Error())
				currFrame = currFrame.GetNextLink()
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), PAGE_WRITE_TIMEOUT*time.Millisecond)

			select {
			case n := <-wr:
				cancel()
				log.Printf("Written %d bytes\n", n)
				currFrame.MarkClean()
				bw.writtenBytes += uint32(n)
				bw.writtenPages++
			case <-ctx.Done():
				cancel()
				log.Printf("Timeout when writing page.")
			}

			nextFrame = currFrame.next
			currFrame.Unreference()
			currFrame.mu.Unlock()

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

func NewBgWriter(cache *Cache, wal *wal.WAL, clockBuffHead *clock, d *diskmanager.DiskManager) *BgWriter {
	return &BgWriter{
		cache:       cache,
		wal:         wal,
		clockBuffer: clockBuffHead,
	}
}
