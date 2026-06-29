package buffermanager

import (
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/oryankibandi/baobab/internal/manual"
	"github.com/oryankibandi/baobab/internal/zipf"
	diskmanager "github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/logger"
	"github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

func TestNewBufferManager(t *testing.T) {
	// initialize wal, logger and pager
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)

	tests := []struct {
		cacheSize uint64
		w         *wal.WAL
		valid     bool
		hasPager  bool
	}{
		{
			cacheSize: MIN_CACHE_SIZE_KB,
			w:         w,
			hasPager:  true,
			valid:     true,
		},
		{
			cacheSize: 4096, // invalid cache size
			w:         w,
			hasPager:  true,
			valid:     false,
		},
		{
			cacheSize: MIN_CACHE_SIZE_KB,
			w:         nil, // no wal provided
			hasPager:  true,
			valid:     false,
		},
		{
			cacheSize: 81920,
			w:         w,
			hasPager:  true,
			valid:     true,
		},
		{
			cacheSize: MIN_CACHE_SIZE_KB,
			w:         w,
			hasPager:  true,
			valid:     true,
		},
		{
			cacheSize: 65536,
			w:         w,
			hasPager:  false, // no pager
			valid:     false,
		},
	}

	var cConfig CacheConfig
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_test_new_cache", i), func(t *testing.T) {
			var pgr *pager.Pager

			if test.hasPager {
				// initialize pager
				dbFile := filepath.Join(t.TempDir(), fmt.Sprintf("baobab_%d.db", i))
				dman, err := diskmanager.NewDiskManager(diskmanager.DiskManagerConfig{DataFile: dbFile})

				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Could not initialize disk manager: %s", err.Error()), t)
				}

				freelistFile := filepath.Join(t.TempDir(), "baobab")
				pgr, err = pager.NewPager(pager.PagerConfig{DManager: dman, FreeListFile: freelistFile})
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Could not initialize pager: %s", err.Error()), t)
				}
			}

			cConfig = CacheConfig{
				CacheSize: test.cacheSize,
			}

			c, err := NewBufferManager(cConfig, test.w, pgr, true)
			if test.valid {
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
				}

				if c == nil {
					helpers.PrintTestErrorMsg("Expected cache, got nil", t)
				}

				err = c.Close()
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf(" Expected no error while closing cache, got %v", err), t)
				}
			} else {
				if err == nil {
					helpers.PrintTestErrorMsg("Expected error, got nil", t)
				}
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("%d_test_new_cache tests passed"+helpers.RESET, i))
		})
	}
}

func TestNewFrame(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: MIN_CACHE_SIZE_KB,
	}

	cache, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if cache == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	defer cache.Close()

	t.Run("test_createnewframe", func(t *testing.T) {
		f, err := cache.NewFrame(false, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
		}

		if f == nil {
			helpers.PrintTestErrorMsg("Expected new frame, got nil", t)
		}

		if k := f.getKey(); k <= 0 {
			helpers.PrintTestErrorMsg(fmt.Sprintf("expected frame to have an assigned pageId got %d", k), t)
		}

		helpers.PrintSuccessMsg("test_createnewframe passed")
	})
}

func TestGetRootPageId(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: MIN_CACHE_SIZE_KB,
	}

	cache, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if cache == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}
	defer cache.Close()

	t.Run("test_initialrootpageid", func(t *testing.T) {
		if r := cache.GetRootPageId(); r != 0 {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected initialroot page id to be 0, got %d", r), t)
		}

		helpers.PrintSuccessMsg("test_initialrootpageid success")
	})

	t.Run("test_getrootpageid", func(t *testing.T) {
		f, err := cache.NewFrame(true, true)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
		}

		f.Acquire(true)
		key := f.getKey()
		f.Release(true)

		if r := cache.GetRootPageId(); r != key {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected root page Id to be %d, but got %d", key, r), t)
		}

		helpers.PrintSuccessMsg("test_getrootpageid success")
	})
}

func TestMarkFrameDirty(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: MIN_CACHE_SIZE_KB,
	}

	cache, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if cache == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}
	defer cache.Close()

	t.Run("test_markframedirty", func(t *testing.T) {
		p := manual.Alloc(unsafe.Sizeof(Frame{}))
		f := (*Frame)(p)
		if f == nil {
			t.Fatal("Memory not allocated.")
		}

		f.Acquire(false)
		if f.dirty.Load() {
			f.Release(false)
			helpers.PrintTestErrorMsg("Expected new  frame to be clean, new frame was marked as dirty", t)
		}

		defer manual.FreeMem(unsafe.Pointer(f))

		err = cache.MarkFrameDirty(f)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to mark frame as dirty: %s", err.Error()), t)
		}

		helpers.PrintSuccessMsg("test_markframedirty success")
	})
}

func TestPutGet(t *testing.T) {
	var mu sync.Mutex
	var wg sync.WaitGroup

	var evictCandidate *Frame
	var windowCacheFrames []*Frame

	concurrGetReq := 10

	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: MIN_CACHE_SIZE_KB,
		numShards: 1,
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		buffManager.Close()
	})

	// calculate expected frame count in each segment
	totFrames := math.Round(float64((MIN_CACHE_SIZE_KB*1024)/cConfig.numShards) / float64(unsafe.Sizeof(clockentry{})))
	windSize := math.Round(totFrames * WINDOW_CACHE_RATIO)
	mainCacheSize := math.Round(totFrames * (1 - WINDOW_CACHE_RATIO))
	probationSize := uint64(math.Round(float64(mainCacheSize) * MAIN_CACHE_RATIO))
	helpers.PrintInfoMsg(fmt.Sprintf("Total Frames -> %f\n", totFrames))
	helpers.PrintInfoMsg(fmt.Sprintf("Window Frames -> %f\n", windSize))
	helpers.PrintInfoMsg(fmt.Sprintf("Main Cache Frames -> %f\n", mainCacheSize))

	maxPageId := uint64(windSize) + 1
	// do not include reserved frame for metadata page
	//protectedSize := uint64(math.Round(float64(mainCacheSize)*float64(1.0-MAIN_CACHE_RATIO))) - 1

	// 1. test adding an item
	// 2. test retrieving the item
	t.Run("test_putget_simple", func(t *testing.T) {
		fr, err := buffManager.NewFrame(true, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("expected no error, got %s", err), t)
		}

		if fr == nil {
			helpers.PrintTestErrorMsg("expected frame, got nil", t)
		}

		fmt.Println("Done retrieving new frame..")
		fr.Acquire(true)
		frKey := fr.getKey()
		if frKey == 0 {
			helpers.PrintTestErrorMsg("got invalid frame key/pageId 0", t)
		}
		fmt.Println("Done checking key...")
		fr.Release(true)

		fmt.Println("Performing Get()...")
		evictCandidate, _, err = buffManager.Get(frKey)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("expected no error, got %s", err), t)
		}
		fmt.Println("Done performing Get()...")

		if evictCandidate == nil {
			helpers.PrintTestErrorMsg("expected retrieved frame, got nil", t)
		}

		fmt.Println("Unreferencing fr and avict candidate...")
		fr.Unreference()
		evictCandidate.Unreference()
		fmt.Println("Done unreferencing...")

		helpers.PrintSuccessMsg("test_putget_simple success")
	})

	// 3. test eviction, fill window cache and store item to be evicted. Add a new item and see if the predicted eviction candidate has been removed by performing a Get req
	t.Run("test_eviction", func(t *testing.T) {
		for i := range uint64(windSize) {
			j := i
			t.Run(fmt.Sprintf("%d_test_eviction_fillcache", j), func(t *testing.T) {
				fr, err := buffManager.NewFrame(false, false)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("expected no error, got -> %s", err), t)
				}

				frKey := fr.getKey()
				if frKey == 0 {
					helpers.PrintTestErrorMsg("invalid frame key/pageId 0", t)
				}

				if fr == nil {
					helpers.PrintTestErrorMsg("expected frame, got nil", t)
				}

				// reference frame
				// fr.Reference()

				windowCacheFrames = append(windowCacheFrames, fr)
				helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
			})
		}

		t.Run("test_eviction_check_evicted", func(t *testing.T) {
			var actualEvicted *Frame
			evictCandidate.Acquire(false)
			if s := evictCandidate.getEntrySegType(); s != probationSegment {
				frameEvicted := false
				helpers.PrintInfoMsg("Checking window frame items")
				for _, f := range windowCacheFrames {
					if f.getEntrySegType() == probationSegment {
						actualEvicted = f
						frameEvicted = true
					}
					helpers.PrintInfoMsg(fmt.Sprintf("key: %d, segment: %v", f.getKey(), f.getEntrySegType()))
				}

				if !frameEvicted {
					helpers.PrintTestErrorMsg(fmt.Sprintf("expected evicted item to be in probation(2) but is in %d", s), t)
				}
			} else {
				actualEvicted = evictCandidate
			}
			evictCandidate.Release(false)

			// try to retrieve evicted frame
			fr, _, err := buffManager.Get(actualEvicted.getKey())
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("expected no error, got %s", err.Error()), t)
			}

			if fr == nil {
				helpers.PrintTestErrorMsg("expected frame, got nil", t)
			}

			fr.Acquire(true)
			if s := fr.getEntrySegType(); s != protectedSegment {
				fr.Release(true)
				helpers.PrintTestErrorMsg(fmt.Sprintf("expected evicted item to be in protected(3) but is in %d", s), t)
			}
			fr.Release(true)

			helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
		})

		// unreference frames
		for _, f := range windowCacheFrames {
			f.Unreference()
		}
		helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
	})

	// 4. test adding many items concurrently
	// 5. test retrieving many items concurrently
	t.Run("test_concurrent_operations", func(t *testing.T) {
		// set max page id as windowsize+1 given we filled and evicted
		// the window cache in previous test

		t.Run("test_concurrent_put", func(t *testing.T) {
			start := make(chan struct{})
			// add probationsize/2 worth of data
			for i := range probationSize / 2 {
				wg.Add(1)
				go func(j uint64) {
					defer wg.Done()
					<-start
					t.Run(fmt.Sprintf("%d_test_concurrent_put", j), func(t *testing.T) {
						fr, err := buffManager.NewFrame(false, false)
						if err != nil {
							helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %s", err.Error()), t)
						}
						if fr == nil {
							helpers.PrintTestErrorMsg("expected frame, got nil", t)
						}

						mu.Lock()
						maxPageId++
						mu.Unlock()

						fr.Unreference()
						helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
					})
				}(i)

			}

			close(start)
			wg.Wait()

			helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
		})

		t.Run("test_concurrent_get", func(t *testing.T) {
			start := make(chan struct{})
			for i := range concurrGetReq {
				wg.Add(1)
				go func(j int) {
					defer wg.Done()
					<-start
					t.Run(fmt.Sprintf("%d_test_concurrent_get", j), func(t *testing.T) {
						k := helpers.RandomNumber(uint32(maxPageId))

						fr, _, err := buffManager.Get(k)
						if err != nil {
							helpers.PrintTestErrorMsg(fmt.Sprintf("expected no error, got %s", err.Error()), t)
						}

						if fr == nil {
							helpers.PrintTestErrorMsg("Expected frame, got nil", t)
						}

						helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
					})
				}(i)
			}

			close(start)
			wg.Wait()

			helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
		})

		helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
	})

	// 6. test retrieving metadata page

	t.Run("test_get_metadata", func(t *testing.T) {
		metaFr, _, err := buffManager.Get(0)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %s", err.Error()), t)
		}

		if metaFr == nil {
			helpers.PrintTestErrorMsg("Expected frame, got nil", t)
		}

		helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
	})
	// 7. test retrieving items that don't exist
	t.Run("test_get_invalid", func(t *testing.T) {
		invalidFr, _, err := buffManager.Get(uint32(maxPageId + 1))
		if err == nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected error for key %d, got nil and frame %v", maxPageId+1, invalidFr), t)
		}

		if invalidFr != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("expected no frame, got %v", invalidFr), t)
		}

		helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
	})
}

func TestNewFrameConcurrent(t *testing.T) {
	var wg sync.WaitGroup

	newFrameCount := 25000
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: 64 * 1024, // 64MB
		numShards: 4,
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	t.Run("test_newframe_concurrent", func(t *testing.T) {
		start := make(chan struct{})
		for i := range newFrameCount {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				<-start
				t.Run(fmt.Sprintf("%d_test_newframe_concurrent", j), func(t *testing.T) {
					fr, err := buffManager.NewFrame(false, false)
					if err != nil {
						helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %s", err.Error()), t)
					}

					if fr == nil {
						helpers.PrintTestErrorMsg("Expected new frame, got nil", t)
					}
					// fr.Reference()
					err = fr.Acquire(true)
					if err != nil {
						fr.Unreference()
						helpers.PrintTestErrorMsg(err.Error(), t)
					}

					if fr.parentEntry == nil {
						helpers.PrintTestErrorMsg("No parentEntry set", t)
					}

					if k := fr.getKey(); k == 0 {
						fr.Release(true)
						fr.Unreference()
						// fmt.Println("PAGEID -> ", fr.page.PageId)
						helpers.PrintTestErrorMsg(fmt.Sprintf("got new frame key as 0. This is reserved for the metadata page. Frame -> %v", fr), t)
					}

					err = fr.Release(true)
					if err != nil {
						helpers.PrintTestErrorMsg(err.Error(), t)
					}
					fr.Unreference()

					helpers.PrintSuccessMsg(fmt.Sprintf("%s success", fmt.Sprintf("%d_test_newframe_concurrent", j)))
				})
			}(i)
		}
		close(start)
		wg.Wait()

		helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
	})
}

func TestNewFrameSequential(t *testing.T) {
	newFrameCount := 30000
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: 256 * 1024, // 16MB
		numShards: 1,
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	t.Run("test_newframe_sequential", func(t *testing.T) {
		for i := range newFrameCount {
			t.Run(fmt.Sprintf("%d_test_newframe_sequential", i), func(t *testing.T) {
				fr, err := buffManager.NewFrame(false, false)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %s", err.Error()), t)
				}

				if fr == nil {
					helpers.PrintTestErrorMsg("Expected new frame, got nil", t)
				}
				// fr.Reference()
				err = fr.Acquire(true)
				if err != nil {
					fr.Unreference()
					helpers.PrintTestErrorMsg(err.Error(), t)
				}

				if k := fr.getKey(); k == 0 {
					fr.Unreference()
					fr.Release(true)
					helpers.PrintTestErrorMsg(fmt.Sprintf("got new frame key as 0. This is reserved for the metadata page. Frame -> %v", fr), t)
				}

				fr.Unreference()
				fr.Release(true)

				helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
			})
		}

		helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
	})
}

func TestDeleteFrame(t *testing.T) {
	newFrameCount := 30000
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: 128 * 1024, // 16MB
		numShards: 16,
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	for range newFrameCount {
		fr, err := buffManager.NewFrame(false, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %s", err.Error()), t)
		}

		if fr == nil {
			helpers.PrintTestErrorMsg("Expected new frame, got nil", t)
		}
		// fr.Reference()
		err = fr.Acquire(true)
		if err != nil {
			fr.Unreference()
			helpers.PrintTestErrorMsg(err.Error(), t)
		}

		if k := fr.getKey(); k == 0 {
			fr.Unreference()
			fr.Release(true)
			helpers.PrintTestErrorMsg(fmt.Sprintf("got new frame key as 0. This is reserved for the metadata page. Frame -> %v", fr), t)
		}

		fr.Unreference()
		fr.Release(true)

		helpers.PrintSuccessMsg("test_delete_createnewframe success")
	}

	deletePids := make([]uint32, 0)
	for i := range newFrameCount / 100 {
		if i != 0 {
			deletePids = append(deletePids, uint32(i*100))
		}
	}

	// delete entries
	for _, pid := range deletePids {
		err := buffManager.Delete(pid)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to delete frame: %s", err.Error()), t)
		}
		fr, hit, err := buffManager.Get(pid)
		fmt.Printf("CACHE HIT? -> %t\n", hit)
		// if err == nil {
		// 	helpers.PrintTestErrorMsg(fmt.Sprintf("Expected frame to be deleted, got err: %s and frame: %v", err.Error(), fr), t)
		// }

		if fr != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected frame to be deleted, got %v", fr), t)
		}

		helpers.PrintSuccessMsg(fmt.Sprintf("%d_frame_delete successful.", pid))
	}
}

// TestZipfianDistribution Simulates Zipfian pattern.
func TestZipfianDistribution(t *testing.T) {
	iterationCount := 100000
	maxPages := 4600 // ~254MB
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: 128 * 1024, // 48MB
		numShards: 1,
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	// create new frames. Page IDs are assigned monotonically
	for i := range maxPages {
		f, err := buffManager.NewFrame(false, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d unable to create new frames: %s", i, err.Error()), t)
		}

		if f == nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d Expected frame, got nil", i), t)
		}

		f.Acquire(true)
		if k := f.getKey(); k == 0 {
			f.Unreference()
			f.Release(true)
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d got new frame key as 0. This is reserved for the metadata page. Frame -> %v", i, f), t)
		}

		f.Release(true)
		f.Unreference()
	}

	// test zipfian
	var pageId uint64
	var totTime time.Duration
	var st time.Time
	z := zipf.NewZipf(1.1, 5, float64(maxPages))

	for i := range iterationCount {
		pageId = z.GetNext()
		st = time.Now()
		f, _, err := buffManager.Get(uint32(pageId))
		totTime += time.Since(st)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Could not retrieve frame: %s", i, err.Error()), t)
		}

		if f == nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Expected frame, got nil", i), t)
		}

		f.Acquire(true)
		if k := f.getKey(); pageId != uint64(k) {
			f.Release(true)
			f.Unreference()
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Expected retrieved frame to have key: %d, got %d instead", i, pageId, k), t)
		}
		f.Release(true)
		f.Unreference()

		helpers.PrintSuccessMsg(fmt.Sprintf("test_zipfian_%d success.", i))
	}

	avg := totTime / time.Duration(iterationCount)
	helpers.PrintSuccessMsg("--------------------------------------")
	helpers.PrintSuccessMsg("	  ZIPFIAN DISTRIBUTION  	")
	helpers.PrintSuccessMsg(fmt.Sprintf("Total time: %v", totTime))
	helpers.PrintSuccessMsg(fmt.Sprintf("Average GET req time: %v", totTime/time.Duration(iterationCount)))
	helpers.PrintSuccessMsg(fmt.Sprintf("%d Op/sec", (time.Second / avg)))
	helpers.PrintSuccessMsg("--------------------------------------")
}

// TestZipfianDistribution Simulates Zipfian pattern.
func TestZipfianDistributionConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var zipfExp float64 = 1.1
	var shards uint64 = 32
	var threads uint64 = 128
	var cacheSize uint64 = 128 * 1024 // 128MB
	iterationCount := 1000000
	warmUpIterations := int(0.05 * float64(iterationCount))
	maxPages := 30000 // ~254MB

	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: cacheSize,
		numShards: shards,
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			fmt.Println("Closing  buffer manager....")
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
			fmt.Println("Closed buffer manager....")
		}
	})

	// create new frames. Page IDs are assigned monotonically
	for i := range maxPages {
		f, err := buffManager.NewFrame(false, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d unable to create new frames: %s", i, err.Error()), t)
		}

		if f == nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d Expected frame, got nil", i), t)
		}

		f.Acquire(true)
		if k := f.getKey(); k == 0 {
			f.Unreference()
			f.Release(true)
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d got new frame key as 0. This is reserved for the metadata page. Frame -> %v", i, f), t)
		}

		f.Release(true)
		f.Unreference()
	}

	// time.Sleep(time.Second * 5)

	// test zipfian
	// var pageId uint64
	var totTime time.Duration
	// var st time.Time
	times := make([]time.Duration, threads)
	var cacheHits atomic.Uint64
	z := zipf.NewZipf(zipfExp, 5, float64(maxPages))

	fmt.Printf("Done adding entries...\n")
	// WARM UP (50k Req)
	var pid uint64
	for i := range warmUpIterations {
		pid = z.GetNext()
		f, _, err := buffManager.Get(uint32(pid))

		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Could not retrieve frame: %s", i, err.Error()), t)
		}

		if f == nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Expected frame, got nil", i), t)
		}

		if !f.parentEntry.isReferenced() {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Returned frame for pid %d not referenced: %v", pid, f), t)
		}

		f.Acquire(true)
		if k := f.getKey(); pid != uint64(k) {
			f.Release(true)
			f.Unreference()
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Expected retrieved frame to have key: %d, got %d instead", i, pid, k), t)
		}
		f.Release(true)
		f.Unreference()

		helpers.PrintSuccessMsg(fmt.Sprintf("test_zipfian_%d success.", i))
	}
	fmt.Printf("Warm up done...\n")

	for th := range threads {
		wg.Add(1)
		go func(idx uint64) {
			defer wg.Done()
			var endTime time.Duration
			var st time.Time
			var pageId uint64
			maxIter := iterationCount / int(threads)
			for i := range maxIter {
				pageId = z.GetNext()
				st = time.Now()
				fmt.Println("Getting page -> ", pageId)
				f, h, err := buffManager.Get(uint32(pageId))
				endTime += time.Since(st)
				fmt.Printf("time after Get() -> %v\n", endTime)
				if h {
					cacheHits.Add(1)
				}

				if err != nil {
					mu.Lock()
					times[idx] = endTime
					mu.Unlock()
					helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Could not retrieve frame: %s", i, err.Error()), t)
				}

				if f == nil {
					mu.Lock()
					times[idx] = endTime
					mu.Unlock()
					helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Expected frame, got nil", i), t)
				}

				if !f.parentEntry.isReferenced() {
					mu.Lock()
					times[idx] = endTime
					mu.Unlock()
					helpers.PrintTestErrorMsg(fmt.Sprintf("Returned frame for pid %d not referenced: %v", pageId, f), t)
				}

				f.Acquire(true)
				if k := f.getKey(); pageId != uint64(k) {
					f.Release(true)
					f.Unreference()
					mu.Lock()
					times[idx] = endTime
					mu.Unlock()
					helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Expected retrieved frame to have key: %d, got %d instead", i, pageId, k), t)
				}
				f.Release(true)
				f.Unreference()

				helpers.PrintSuccessMsg(fmt.Sprintf("test_zipfian_%d success.", i))
			}

			mu.Lock()
			times[idx] = endTime
			mu.Unlock()
		}(th)
	}
	wg.Wait()

	for _, tt := range times {
		totTime += tt
	}

	executionTime := totTime / time.Duration(threads)
	avg := executionTime / time.Duration(iterationCount)
	hitRate := (float64(cacheHits.Load()) / float64(iterationCount)) * 100
	helpers.PrintSuccessMsg("--------------------------------------")
	helpers.PrintSuccessMsg(fmt.Sprintf("ZIPFIAN DISTRIBUTION - %d shards - %d threads  	", shards, threads))
	helpers.PrintSuccessMsg(fmt.Sprintf("Zipf Exponent: %.2f", zipfExp))
	helpers.PrintSuccessMsg(fmt.Sprintf("Cache Size: %dMB", (cacheSize / 1024)))
	helpers.PrintSuccessMsg(fmt.Sprintf("Total time: %v", executionTime))
	helpers.PrintSuccessMsg(fmt.Sprintf("Average GET req time: %v", totTime/time.Duration(iterationCount)))
	helpers.PrintSuccessMsg(fmt.Sprintf("Total Hits: %v", cacheHits.Load()))
	helpers.PrintSuccessMsg(fmt.Sprintf("Hit Rate: %.2f%%", hitRate))
	helpers.PrintSuccessMsg(fmt.Sprintf("%d Op/sec", (time.Second / avg)))
	helpers.PrintSuccessMsg("--------------------------------------")
}

func TestRandomDistribution(t *testing.T) {
	iterationCount := 1000000
	maxPages := 10000
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: 128 * 1024, // 48MB
		numShards: 4,
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	// create new frames. Page IDs are assigned monotonically
	for i := range maxPages {
		f, err := buffManager.NewFrame(false, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d unable to create new frames: %s", i, err.Error()), t)
		}

		if f == nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d Expected frame, got nil", i), t)
		}

		f.Acquire(true)
		if k := f.getKey(); k == 0 {
			f.Unreference()
			f.Release(true)
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d got new frame key as 0. This is reserved for the metadata page. Frame -> %v", i, f), t)
		}

		f.Release(true)
		f.Unreference()
	}

	// test zipfian
	var pageId uint64
	var totTime time.Duration
	var st time.Time

	for i := range iterationCount {
		pageId = uint64(helpers.RandomNumber(uint32(maxPages)))
		if pageId == 0 {
			pageId++
		}
		st = time.Now()
		f, _, err := buffManager.Get(uint32(pageId))
		totTime += time.Since(st)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Could not retrieve frame: %s", i, err.Error()), t)
		}

		if f == nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Expected frame, got nil", i), t)
		}

		f.Acquire(true)
		if k := f.getKey(); pageId != uint64(k) {
			f.Release(true)
			f.Unreference()
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Expected retrieved frame to have key: %d, got %d instead", i, pageId, k), t)
		}
		f.Release(true)
		f.Unreference()

		helpers.PrintSuccessMsg(fmt.Sprintf("test_zipfian_%d success.", i))
	}

	avg := totTime / time.Duration(iterationCount)
	helpers.PrintSuccessMsg("--------------------------------------")
	helpers.PrintSuccessMsg("	   RANDOM DISTRIBUTION  	")
	helpers.PrintSuccessMsg(fmt.Sprintf("Total time: %v", totTime))
	helpers.PrintSuccessMsg(fmt.Sprintf("Average GET req time: %v", totTime/time.Duration(iterationCount)))
	helpers.PrintSuccessMsg(fmt.Sprintf("%d Op/sec", (time.Second / avg)))
	helpers.PrintSuccessMsg("--------------------------------------")
}

func TestSequentialDistribution(t *testing.T) {
	iterationCount := 1000000
	maxPages := 10000
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	// create new frames. Page IDs are assigned monotonically
	for i := range maxPages {
		f, err := buffManager.NewFrame(false, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d unable to create new frames: %s", i, err.Error()), t)
		}

		if f == nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d Expected frame, got nil", i), t)
		}

		f.Acquire(true)
		if k := f.getKey(); k == 0 {
			f.Unreference()
			f.Release(true)
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_new_frame_%d got new frame key as 0. This is reserved for the metadata page. Frame -> %v", i, f), t)
		}

		f.Release(true)
		f.Unreference()
	}

	// test zipfian
	var pageId uint64
	var totTime time.Duration
	var st time.Time

	pageId = 0
	for i := range iterationCount {
		pageId += 1
		st = time.Now()
		f, _, err := buffManager.Get(uint32(pageId))
		totTime += time.Since(st)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Could not retrieve frame: %s", i, err.Error()), t)
		}

		if f == nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Expected frame, got nil", i), t)
		}

		f.Acquire(true)
		if k := f.getKey(); pageId != uint64(k) {
			f.Release(true)
			f.Unreference()
			helpers.PrintTestErrorMsg(fmt.Sprintf("test_zipfian_%d Expected retrieved frame to have key: %d, got %d instead", i, pageId, k), t)
		}
		f.Release(true)
		f.Unreference()

		if pageId == uint64(maxPages) {
			pageId = 0
		}

		helpers.PrintSuccessMsg(fmt.Sprintf("test_zipfian_%d success.", i))
	}

	avg := totTime / time.Duration(iterationCount)
	helpers.PrintSuccessMsg("--------------------------------------")
	helpers.PrintSuccessMsg("	  SEQUENTIAL DISTRIBUTION  	")
	helpers.PrintSuccessMsg(fmt.Sprintf("Total time: %v", totTime))
	helpers.PrintSuccessMsg(fmt.Sprintf("Average GET req time: %v", totTime/time.Duration(iterationCount)))
	helpers.PrintSuccessMsg(fmt.Sprintf("%d Op/sec", (time.Second / avg)))
	helpers.PrintSuccessMsg("--------------------------------------")
}

func BenchmarkBufferManager(t *testing.B) {
	maxPages := 10000
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPagerBench(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: 256 * 1024, // 48MB
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr, true)
	if err != nil {
		pgr.Close()
		t.Fatalf("Expected no error, got %v", err)
	}

	if buffManager == nil {
		pgr.Close()
		t.Fatalf("Expected cache, got nil")
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				t.Fatalf("Unable to close buffermanager: %s", err.Error())
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	// create new frames. Page IDs are assigned monotonically
	for range maxPages {
		f, err := buffManager.NewFrame(false, false)
		if err != nil {
			t.Fatalf("unable to create new frames: %s", err.Error())
		}

		if f == nil {
			t.Fatalf("Expected frame, got nil")
		}

		f.Acquire(true)

		if k := f.getKey(); k == 0 {
			f.Unreference()
			f.Release(true)
			t.Fatalf("got new frame key as 0. This is reserved for the metadata page.")
		}

		f.Release(true)
		f.Unreference()
	}

	var pageId uint64
	z := zipf.NewZipf(1.1, 5, float64(maxPages))
	for i := 0; t.Loop(); i++ {
		pageId = z.GetNext()
		f, _, err := buffManager.Get(uint32(pageId))
		if err != nil {
			t.Fatalf("bench_zipfian_%d Could not retrieve frame: %s", i, err.Error())
		}

		if f == nil {
			t.Fatalf(fmt.Sprintf("bench_zipfian_%d Expected frame, got nil", i), t)
		}

		f.Acquire(true)
		if k := f.getKey(); pageId != uint64(k) {
			f.Release(true)
			f.Unreference()
			t.Fatalf("test_zipfian_%d Expected retrieved frame to have key: %d, got %d instead", i, pageId, k)
		}
		f.Release(true)
		f.Unreference()
	}
}
