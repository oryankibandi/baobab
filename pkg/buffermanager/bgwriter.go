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
	MAX_PAGES = 5000
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
	cache        *shard
	wal          *wal.WAL
	pgr          *pager.Pager

	// Head of the clock buffer
	clockBuffer   *clock
	clockCapacity uint64
	shardId       uint64
	mu            sync.Mutex
	wg            sync.WaitGroup
	// exit channel
	exitchan *chan struct{}
}

func (bw *BgWriter) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	go bw.watchFreeList(bw.exitchan, wg)

	var currFrame *Frame
	var currEntry *clockentry
	// var currFrameBuff *[]byte
	tmp := make([]byte, 8192)

	// time.Sleep(time.Second * 10)
BgWriterLoop:
	for {
		for i := range bw.clockCapacity {
			select {
			case <-*bw.exitchan:
				break BgWriterLoop
			default:
				currEntry = &bw.clockBuffer.bPool[i]
				if !currEntry.refIfNotReferenced() {
					continue
				}

				currFrame = &currEntry.entry
				if !currFrame.isDirty() {
					currEntry.unref()
					continue
				}

				shouldFlush := bw.writtenPages+1 == MAX_PAGES

				currFrame.Acquire(false)
				k := currFrame.key
				currFrameBuff, _, err := currFrame.RawBufferSlice()
				if err != nil {
					helpers.PrintErrorMsg(fmt.Sprintf("(bgwriter) Error retrieving raw buffer: %s\n", err))
					currFrame.Release(false)
					currEntry.unref()
					continue
				}
				copy(tmp, *currFrameBuff)
				currFrame.MarkClean()
				currFrameBuff = nil
				currFrame.Release(false)

				err = bw.pgr.WritePage(k, &tmp, shouldFlush)
				if err != nil {
					helpers.PrintErrorMsg(fmt.Sprintf("(bgwriter) Error writing to page: %s\n", err.Error()))
					currEntry.unref()
					continue
				}

				currEntry.unref()
				bw.writtenPages++
			}

			if bw.writtenPages >= MAX_PAGES {
				bw.writtenPages = 0
				// bw.pgr.Flush()
				break
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

func NewBgWriter(cache *shard, wal *wal.WAL, clockBuffHead *clock, pgr *pager.Pager) *BgWriter {
	return &BgWriter{
		cache:         cache,
		wal:           wal,
		clockBuffer:   clockBuffHead,
		clockCapacity: clockBuffHead.capacity,
		pgr:           pgr,
		shardId:       cache.shardId,
		exitchan:      cache.exitchan,
	}
}
