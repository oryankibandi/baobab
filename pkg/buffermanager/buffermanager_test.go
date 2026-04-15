package buffermanager

import (
	"fmt"
	"path/filepath"
	"testing"

	diskmanager "github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/logger"
	"github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

// TODO:
// 1. Test New Buffer manager
//	- Invalid cache size
//	- nil wal  and disk manager config
// 2. Test adding item to cache (CreateNewFrame)
// 3. Test Retrieving items from cache
//	- Existing item
//	- Non-existing item
// 4. Benchmark random and zipfian workloads

func TestNewCache(t *testing.T) {
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
					t.Fatalf(helpers.BOLDRED+"Expected no error, got %v"+helpers.RESET, err)
				}

				if c == nil {
					t.Fatalf(helpers.BOLDRED + "Expected cache, got nil" + helpers.RESET)
				}

				err = c.Close()
				if err != nil {
					t.Fatalf(helpers.BOLDRED+"✗ Expected no error while closing cache, got %v"+helpers.RESET, err)
				}
			} else {
				if err == nil {
					t.Fatalf(helpers.BOLDRED + "Expected error, got nil" + helpers.RESET)
				}
			}

			t.Logf(helpers.BOLDGREEN+"  ✓ %d_test_new_cache tests passed"+helpers.RESET, i)
		})

	}
}

func TestCreateNewFrame(t *testing.T) {
	// var keys [][]byte
	// var vals [][]byte

	// numKeys := 3
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	dbFile := filepath.Join(t.TempDir(), "baobab.db")
	dman, err := diskmanager.NewDiskManager(diskmanager.DiskManagerConfig{DataFile: dbFile})

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Could not initialize disk manager: %s", err.Error()), t)
	}

	freelistFile := filepath.Join(t.TempDir(), "baobab")
	pgr, err := pager.NewPager(pager.PagerConfig{DManager: dman, FreeListFile: freelistFile})
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Could not initialize pager: %s", err.Error()), t)
	}

	// initialize buffer manager
	cConfig := CacheConfig{
		CacheSize: MIN_CACHE_SIZE_KB,
	}

	cache, err := NewBufferManager(cConfig, w, pgr)
	if err != nil {
		t.Fatalf(helpers.BOLDRED+"Expected no error, got %v"+helpers.RESET, err)
	}

	if cache == nil {
		t.Fatalf(helpers.BOLDRED + "Expected cache, got nil" + helpers.RESET)
	}

	defer cache.Close()

	//  lsn := []byte("lsn_01")
	//  for i := range numKeys {
	//  	keys = append(keys, fmt.Appendf(nil, "k_%d", i+1))
	//  	vals = append(vals, fmt.Appendf(nil, "v_%d", i+1))
	//  }

	t.Run("test_createnewframe", func(t *testing.T) {
		f, err := cache.NewFrame(false, false)
		if err != nil {
			t.Fatalf(helpers.BOLDRED+"Expected no error, got %v"+helpers.RESET, err)
		}

		if f == nil {
			t.Fatalf(helpers.BOLDRED + "Expected new frame, got nil" + helpers.RESET)
		}

		if k := f.getKey(); k <= 0 {
			helpers.PrintTestErrorMsg(fmt.Sprintf("expected frame to have an assigned pageId got %d", k), t)
		}

		helpers.PrintSuccessMsg("test_createnewframe passed")
	})
}
