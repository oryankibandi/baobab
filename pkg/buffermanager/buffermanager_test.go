package buffermanager

import (
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"unsafe"

	"github.com/oryankibandi/baobab/internal/manual"
	diskmanager "github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/logger"
	"github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

// TODO:
// 1. Test New Buffer manager
//	- Invalid cache size
//	- nil wal and pager
// 2. Test adding item to cache (CreateNewFrame)
// 3. Test Retrieving items from cache
//	- Existing item
//	- Non-existing item
// 4. Benchmark random and zipfian workloads

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
			cacheSize: 8192,
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
			cacheSize: 8192,
			w:         nil, // no wal provided
			hasPager:  true,
			valid:     false,
		},
		{
			cacheSize: 20480,
			w:         w,
			hasPager:  true,
			valid:     true,
		},
		{
			cacheSize: 8192,
			w:         w,
			hasPager:  true,
			valid:     true,
		},
		{
			cacheSize: 8192,
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

			c, err := NewBufferManager(cConfig, test.w, pgr)
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

	cache, err := NewBufferManager(cConfig, w, pgr)
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

	cache, err := NewBufferManager(cConfig, w, pgr)
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

	cache, err := NewBufferManager(cConfig, w, pgr)
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
		f := NewFrame()
		if f == nil {
			helpers.PrintTestErrorMsg("Expected frame, got nil", t)
		}

		f.Acquire(false)
		if f.dirty.Load() {
			f.Release(false)
			helpers.PrintTestErrorMsg("Expected new  frame to be clean, new frame was marked as dirty", t)
		}

		defer manual.FreeMem(f.CPtr)

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
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}
	defer buffManager.Close()

	// calculate expected frame count in each segment
	totFrames := math.Round(float64(MIN_CACHE_SIZE_KB*1024) / float64(unsafe.Sizeof(Frame{})))
	windSize := math.Round(totFrames * WINDOW_CACHE_RATIO)
	mainCacheSize := math.Round(totFrames * (1 - WINDOW_CACHE_RATIO))
	probationSize := uint64(math.Round(float64(mainCacheSize) * MAIN_CACHE_RATIO))

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
		frKey := fr.getKey()
		if frKey == 0 {
			helpers.PrintTestErrorMsg("got invalid frame key/pageId 0", t)
		}

		if fr == nil {
			helpers.PrintTestErrorMsg("expected frame, got nil", t)
		}

		_, err = buffManager.put(frKey, fr, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("expected no error, got %s", err), t)
		}

		evictCandidate, err = buffManager.Get(frKey)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("expected no error, got %s", err), t)
		}

		if evictCandidate == nil {
			helpers.PrintTestErrorMsg("expected retrieved frame, got nil", t)
		}

		fr.Unreference()

		helpers.PrintSuccessMsg("test_putget_simple success")
	})
	// 3. test eviction, fill window cache and store item to be evicted. Add a new item and see it the predicted eviction candidate has been removed by performing a Get req
	t.Run("test_eviction", func(t *testing.T) {
		for i := range uint64(windSize) {
			t.Run(fmt.Sprintf("%d_test_eviction_fillcache", i), func(t *testing.T) {
				fr, err := buffManager.NewFrame(false, false)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("expected no error, got %s", err), t)
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
			if s := evictCandidate.getSegType(); s != probationSegment {
				helpers.PrintTestErrorMsg(fmt.Sprintf("expected evicted item to be in probation(2) but is in %d", s), t)
			}

			// try to retrieve evicted frame
			fr, err := buffManager.Get(evictCandidate.getKey())
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("expected no error, got %s", err.Error()), t)
			}

			if fr == nil {
				helpers.PrintTestErrorMsg("expected frame, got nil", t)
			}

			if s := fr.getSegType(); s != protectedSegment {
				helpers.PrintTestErrorMsg(fmt.Sprintf("expected evicted item to be in protected(3) but is in %d", s), t)
			}

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

						fr, err := buffManager.Get(k)
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
		metaFr, err := buffManager.Get(0)
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
		invalidFr, err := buffManager.Get(uint32(maxPageId + 1))
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

	newFrameCount := 8000
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: 16 * 1024, // 256MB
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	defer buffManager.Close()

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

					if k := fr.getKey(); k == 0 {
						fr.Unreference()
						fr.Release(true)
						// fmt.Println("PAGEID -> ", fr.page.PageId)
						helpers.PrintTestErrorMsg(fmt.Sprintf("got new frame key as 0. This is reserved for the metadata page. Frame -> %v", fr), t)
					}

					fr.Unreference()
					err = fr.Release(true)
					if err != nil {
						helpers.PrintTestErrorMsg(err.Error(), t)
					}

					helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
				})
			}(i)
		}
		close(start)
		wg.Wait()

		helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
	})
}

func TestNewFrameSequential(t *testing.T) {
	newFrameCount := 10000
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: 16 * 1024, // 128MB
	}

	buffManager, err := NewBufferManager(cConfig, w, pgr)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	defer buffManager.Close()

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
				err = fr.Release(true)
				if err != nil {
					helpers.PrintTestErrorMsg(err.Error(), t)
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
			})
		}

		helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
	})
}
