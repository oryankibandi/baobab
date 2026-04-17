package buffermanager

import (
	"fmt"
	"path/filepath"
	"testing"

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

func TestCreateNewFrame(t *testing.T) {
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

func TestPutGetSimple(t *testing.T) {
	// calculate expected frame count in each segment

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

	// 1. test adding an item
	// 2. test retrieving the item
	// 3. test eviction, fill window cache and store item to be evicted. Add a new item and see it the predicted eviction candidate has been removed by performing a Get req
	// 4. test adding many items concurrently
	// 5. test retrieving many items concurrently
	// 6. test retrieving items that don't exist

}
