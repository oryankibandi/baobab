package buffermanager

import (
	"fmt"
	"sync"
	"time"

	"github.com/oryankibandi/baobab/pkg/helpers"
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
	// exit channel
	exitchan *chan struct{}
}

func (bw *BgWriter) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	go bw.watchFreeList(bw.exitchan, wg)

	var currFrame *Frame
	var nextFrame *Frame
	var currFrameBuff *[]byte

	clockCount := bw.clockBuffer.getCap()
	currFrame = bw.clockBuffer.clockHead()

BgWriterLoop:
	for {
		for range clockCount {
			select {
			case <-*bw.exitchan:
				break BgWriterLoop
			default:

				if currFrame.isPinned() {
					nextFrame = currFrame.GetNextLink()
					currFrame = nextFrame
					continue
				}
				currFrame.Reference()

				if !currFrame.isDirty() {
					nextFrame = currFrame.GetNextLink()
					currFrame.Unreference()
					currFrame = nextFrame
					continue
				}

				err := currFrame.Acquire(false)
				if err != nil {
					helpers.PrintErrorMsg(err.Error())
					currFrame.Unreference()
					currFrame = currFrame.GetNextLink()
					continue
				}
				k := currFrame.getKey()
				helpers.PrintInfoMsg(fmt.Sprintf("(bgwriter) acquired latch for frame id: %d", k))

				currFrameBuff, _, err = currFrame.RawBufferSlice()
				if err != nil {
					panic("Unable to get frame buffer")
				}

				err = bw.pgr.WritePage(currFrame.getKey(), currFrameBuff)
				if err != nil {
					helpers.PrintErrorMsg(fmt.Sprintf("(bgwriter) Error writing to page: %s\n", err.Error()))
					err = currFrame.Release(false)
					if err != nil {
						panic(err.Error())
					}
					currFrame.Unreference()
					currFrame = currFrame.GetNextLink()
					continue
				}

				currFrame.MarkClean()

				nextFrame = currFrame.GetNextLink()
				err = currFrame.Release(false)
				if err != nil {
					panic(err)
				}
				helpers.PrintInfoMsg(fmt.Sprintf("(bgwriter) Released latch on frame %d", k))
				currFrame.Unreference()
				bw.writtenPages++

				if bw.writtenPages >= MAX_PAGES {
					break
				}

				currFrame = nextFrame
			}
		}
		time.Sleep(time.Millisecond * BGWRITER_DELAY)
	}

	helpers.PrintInfoMsg("terminated background writer")
}

// periodically flushes free list to disk if dirty
func (bw *BgWriter) watchFreeList(exitChn *chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	for {
		select {
		case <-*exitChn:
			helpers.PrintInfoMsg("terminating freelist background writer..")
			return
		default:
			err := bw.cache.flushFreeList()
			if err != nil {
				panic(err)
			}

		}
		time.Sleep(time.Millisecond * FREELIST_WRITER_DELAY)

	}
}

func NewBgWriter(cache *BufferManager, wal *wal.WAL, clockBuffHead *clock, pgr *pager.Pager) *BgWriter {
	return &BgWriter{
		cache:       cache,
		wal:         wal,
		clockBuffer: clockBuffHead,
		pgr:         pgr,
		exitchan:    cache.exitchan,
	}
}
