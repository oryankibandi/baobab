package buffermanager

import (
	"fmt"
	"path/filepath"
	"testing"

	diskmanager "github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/logger"
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
	// initialize wal and logger
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	dbFile := filepath.Join(t.TempDir(), "baobab.db")

	tests := []struct {
		cacheSize uint64
		w         *wal.WAL
		dbFile    string
		valid     bool
	}{
		{
			cacheSize: 8192,
			w:         w,
			dbFile:    dbFile,
			valid:     true,
		},
		{
			cacheSize: 4096,
			w:         w,
			dbFile:    dbFile,
			valid:     false,
		},
		{
			cacheSize: 8192,
			w:         nil,
			dbFile:    dbFile,
			valid:     false,
		},
		{
			cacheSize: 8192,
			w:         w,
			dbFile:    "",
			valid:     false,
		},
		{
			cacheSize: 20480,
			w:         w,
			dbFile:    dbFile,
			valid:     true,
		},
		{
			cacheSize: 8192,
			w:         w,
			dbFile:    dbFile,
			valid:     true,
		},
	}

	var dmConfig diskmanager.DiskManagerConfig
	var cConfig CacheConfig
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_test_new_cache", i), func(t *testing.T) {
			dmConfig = diskmanager.DiskManagerConfig{
				DataFile: test.dbFile,
			}

			cConfig = CacheConfig{
				CacheSize: test.cacheSize,
			}

			c, err := NewCache(cConfig, test.w, dmConfig)
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
	var keys [][]byte
	var vals [][]byte

	numKeys := 3
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	dmConfig := diskmanager.DiskManagerConfig{
		DataFile: filepath.Join(t.TempDir(), "baobab.db"),
	}

	cConfig := CacheConfig{
		CacheSize: MIN_CACHE_SIZE_KB,
	}

	cache, err := NewCache(cConfig, w, dmConfig)
	if err != nil {
		t.Fatalf(helpers.BOLDRED+"Expected no error, got %v"+helpers.RESET, err)
	}

	if cache == nil {
		t.Fatalf(helpers.BOLDRED + "Expected cache, got nil" + helpers.RESET)
	}

	defer cache.Close()

	lsn := []byte("lsn_01")
	for i := range numKeys {
		keys = append(keys, fmt.Appendf(nil, "k_%d", i+1))
		vals = append(vals, fmt.Appendf(nil, "v_%d", i+1))
	}

	t.Run("test_createnewframe", func(t *testing.T) {
		f, err := cache.CreateNewFrame(lsn, keys, &vals, nil, false)
		if err != nil {
			t.Fatalf(helpers.BOLDRED+"Expected no error, got %v"+helpers.RESET, err)
		}

		if f == nil {
			t.Fatalf(helpers.BOLDRED + "Expected new frame, got nil" + helpers.RESET)
		}

		t.Logf(helpers.BOLDGREEN + "  ✓ test_createnewframe passed" + helpers.RESET)
	})
}
