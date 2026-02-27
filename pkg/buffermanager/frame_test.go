package buffermanager

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/oryankibandi/baobab/internal/manual"
	"github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"

	"github.com/stretchr/testify/assert"
)

func TestMemAlloc(t *testing.T) {
	// t.Parallel()
	var en *Frame

	t.Run("allocate_memory", func(t *testing.T) {
		en = NewFrame()
		if en == nil {
			t.Fatal("Memory not allocated.")
		}

		if en.CPtr == nil {
			t.Fatal("Unsafe pointer not set")
		}
	})

	t.Run("free memory", func(t *testing.T) {
		manual.FreeMem(en.CPtr)

		en = nil

		if en != nil {
			t.Fatal(fmt.Errorf("Memory not freed: %v\n", en))
		}
	})
}

func TestMemAllocMany(t *testing.T) {
	iterations := 5
	buffSizeMB := 24 * (1024 * 1024) // 24MB
	entrySize := unsafe.Sizeof(Frame{})
	entryCount := buffSizeMB / int(entrySize)

	var entries []*Frame
	var m runtime.MemStats
	var e *Frame
	var d [diskmanager.PAGE_SIZE_BYTES]byte

	for i := range diskmanager.PAGE_SIZE_BYTES {
		d[i] = 1
	}

	var currRss uint64

	// allocate and deallocate multiple times
	for i := range iterations {
		t.Run(fmt.Sprintf("%d_mem_allocation", i), func(t *testing.T) {
			runtime.ReadMemStats(&m)
			currRss, err := manual.CurrentRSSBytes()

			if err != nil {
				panic(err)
			}

			fmt.Printf("before: Alloc=%d KB TotalAlloc=%d KB Sys=%d KB RSS=%dMB\n", m.Alloc/1024, m.TotalAlloc/1024, m.Sys/1024, currRss/1024/1024)

			for range entryCount {
				e = NewFrame()

				// add data to page to trigger allocation
				// Since calloc zeros out allocated memory, the OS may
				// not actually assign the physical memory until data is
				// added
				e.page = diskmanager.Page{}
				e.page.SetPageData(&d)

				entries = append(entries, e)
			}

			runtime.ReadMemStats(&m)
			currRss, err = manual.CurrentRSSBytes()

			if err != nil {
				panic(err)
			}

			memAfterAlloc := currRss

			fmt.Printf("After allocation: Alloc=%d KB TotalAlloc=%d KB Sys=%d KB RSS=%dMB\n", m.Alloc/1024, m.TotalAlloc/1024, m.Sys/1024, currRss/1024/1024)
			assert.Less(t, m.Sys, currRss, fmt.Errorf("Expected memory to  be invisible to GC, got Sys: %dKB, RSS: %dKB", m.Sys/1024, currRss/1024))

			runtime.GC()
			runtime.ReadMemStats(&m)

			currRss, err = manual.CurrentRSSBytes()
			if err != nil {
				panic(err)
			}

			fmt.Printf("After GC: Alloc=%d KB TotalAlloc=%d KB Sys=%d KB RSS=%dMB\n", m.Alloc/1024, m.TotalAlloc/1024, m.Sys/1024, currRss/1024/1024)

			// Free
			for i, en := range entries {
				manual.FreeMem(unsafe.Pointer(en))
				en = nil
				entries[i] = nil
			}

			entries = nil

			runtime.ReadMemStats(&m)

			currRss, err = manual.CurrentRSSBytes()

			if err != nil {
				panic(err)
			}

			fmt.Printf("After manual Free: Alloc=%d KB TotalAlloc=%d KB Sys=%d KB RSS=%dMB\n", m.Alloc/1024, m.TotalAlloc/1024, m.Sys/1024, currRss/1024/1024)

			if !ASANEnabled {
				// RSS memory should have gone down after free()
				assert.Less(t, currRss, memAfterAlloc, fmt.Errorf("Expected memory to be freed, got memory of %d, from previous %dKB", currRss/1024, memAfterAlloc/1024))
			}

			fmt.Println("---------------------------------------")

		})
	}

	time.Sleep(1 * time.Second)
	currRss, err := manual.CurrentRSSBytes()

	if err != nil {
		t.Fatal(fmt.Errorf("Could not get current RSS: %v", err))
	}

	fmt.Printf("Current RSS: %dMB\n", currRss/1024/1024)

}

func TestSetData(t *testing.T) {
	var en *Frame

	en = NewFrame()
	if en == nil {
		t.Fatal("Memory not allocated.")
	}

	if en.CPtr == nil {
		t.Fatal("Unsafe pointer not set")
	}

	var d [diskmanager.PAGE_SIZE_BYTES]byte

	key := uint32(25)

	// set key
	binary.LittleEndian.PutUint32(d[1:5], key)

	for i := range diskmanager.PAGE_SIZE_BYTES {
		// skip page id slot
		if i >= 1 && i < 5 {
			continue
		}
		d[i] = 1
	}

	testPage := diskmanager.Page{}
	testPage.SetPageData(&d)

	t.Run("set_data", func(t *testing.T) {
		err := en.SetData(&testPage)
		assert.Nilf(t, err, "Expected no error, got  %v", err)

		d2, err := en.page.GetPageByteData()
		assert.Nilf(t, err, "Expected no error, got  %v", err)
		assert.Equalf(t, d, *d2, "Expected equal data during setData()")

		// check key
		k := en.getKey()
		assert.Equalf(t, key, k, "Expected key to be %d, got %d", key, k)
	})

	t.Cleanup(func() {
		err := FreeFrame(en)
		if err != nil {
			t.Fatalf("Expected no error when freeing memory, got %v", err)
		}

		en = nil
		if en != nil {
			t.Fatal(fmt.Errorf("Memory not freed: %v\n", en))
		}
	})
}

func TestUpdateData(t *testing.T) {
	var en *Frame

	en = NewFrame()
	if en == nil {
		t.Fatal("Memory not allocated.")
	}

	if en.CPtr == nil {
		t.Fatal("Unsafe pointer not set")
	}

	var d [diskmanager.PAGE_SIZE_BYTES]byte

	key := uint32(25)
	binary.LittleEndian.PutUint32(d[1:5], key)
	pge := diskmanager.Page{}
	pge.SetPageData(&d)

	for i := range diskmanager.PAGE_SIZE_BYTES {
		d[i] = 1
	}

	t.Run("update_data", func(t *testing.T) {
		err := en.page.SetPageData(&d)
		assert.Nilf(t, err, "Expected no error, got  %s", err)

		d2, err := en.page.GetPageByteData()
		assert.Nilf(t, err, "Expected no error, got  %s", err)
		assert.Equalf(t, d, *d2, "Expected equal data during setData()")
	})

	t.Cleanup(func() {
		manual.FreeMem(en.CPtr)

		en = nil

		if en != nil {
			t.Fatal(fmt.Errorf("Memory not freed: %v\n", en))
		}
	})
}

func TestClear(t *testing.T) {
	var en *Frame

	en = NewFrame()
	if en == nil {
		t.Fatal("Memory not allocated.")
	}

	if en.CPtr == nil {
		t.Fatal("Unsafe pointer not set")
	}

	var d [diskmanager.PAGE_SIZE_BYTES]byte
	for i := range diskmanager.PAGE_SIZE_BYTES {
		d[i] = 1
	}
	key := uint32(25)

	pge := diskmanager.Page{}
	pge.PageId = key
	pge.SetPageData(&d)
	err := en.SetData(&pge)

	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	t.Run("clear", func(t *testing.T) {
		en.Clear()

		d2, err := en.ByteData()
		assert.Nilf(t, err, "Expected nil, got %v", err)
		assert.NotEqualf(t, d2, d, "Data not cleared")

		k := en.getKey()
		assert.NotEqualf(t, k, key, "Key not cleared")
		assert.Equalf(t, k, uint32(0), "Expected 0, got %d", k)
	})
}

func TestRefAndUnref(t *testing.T) {
	// t.Parallel()
	tests := []struct {
		operation          string // reference | unreference
		expectedPinCount   uint64
		expectedUnpinCount uint64
		expectedAccBit     bool
		expectedRefBit     bool
	}{
		{operation: "reference", expectedPinCount: 1, expectedUnpinCount: 0, expectedAccBit: true, expectedRefBit: true},
		{operation: "reference", expectedPinCount: 2, expectedUnpinCount: 0, expectedAccBit: true, expectedRefBit: true},
		{operation: "reference", expectedPinCount: 3, expectedUnpinCount: 0, expectedAccBit: true, expectedRefBit: true},
		{operation: "unreference", expectedPinCount: 3, expectedUnpinCount: 1, expectedAccBit: true, expectedRefBit: true},
		{operation: "unreference", expectedPinCount: 3, expectedUnpinCount: 2, expectedAccBit: true, expectedRefBit: true},
		{operation: "unreference", expectedPinCount: 3, expectedUnpinCount: 3, expectedAccBit: false, expectedRefBit: true},
	}

	var en *Frame

	t.Run("allocate_memory", func(t *testing.T) {
		en = NewFrame()

		if en == nil {
			t.Fatal("Memory not allocated.")
		}

		if en.CPtr == nil {
			t.Fatal("Unsafe pointer not set")
		}
	})

	for i, test := range tests {
		t.Run(fmt.Sprintf("test_reference_%d", i), func(t *testing.T) {
			if test.operation == "reference" {
				en.Reference()
			} else {
				en.Unreference()
			}
			// check ref bit, acess bit and pin count
			assert.Equal(t, test.expectedAccBit, en.acc.Load(), "access bit not set")
			assert.Equal(t, test.expectedRefBit, en.ref.Load(), "reference bit not set")
			assert.Equal(t, test.expectedPinCount, en.counters.pinCount.Load(), "Invalid pin count")
			assert.Equal(t, test.expectedUnpinCount, en.counters.unpinCount.Load(), "Invalid unpin cunt")
		})
	}

	manual.FreeMem(en.CPtr)
	en = nil
}

func TestRefAndUnrefConcurrent(t *testing.T) {
	// t.Parallel()
	testCount := 1000
	var wg sync.WaitGroup
	type testItem struct {
		operation string // reference | unreference
	}

	tests := make([]testItem, 0)
	for i := range testCount {
		if i%4 == 0 {
			tests = append(tests, testItem{operation: "unreference"})
		} else {
			tests = append(tests, testItem{operation: "reference"})
		}
	}

	var en *Frame

	t.Run("allocate_memory_concurrent", func(t *testing.T) {
		en = NewFrame()

		if en == nil {
			t.Fatal("Memory not allocated.")
		}
		if en.CPtr == nil {
			t.Fatal("Unsafe pointer not set")
		}
	})

	start := make(chan struct{})
	for i, test := range tests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			t.Run(fmt.Sprintf("test_concurrent_%d", i), func(t *testing.T) {
				if test.operation == "reference" {
					en.Reference()

					// check ref bit
					assert.Equal(t, true, en.ref.Load(), "reference bit not set")
				}
			})

		}()
	}

	// start gorutines simultaneously
	close(start)

	wg.Wait()
	manual.FreeMem(en.CPtr)
	en = nil
}

func TestMarkDirty(t *testing.T) {
	var en *Frame
	var d [diskmanager.PAGE_SIZE_BYTES]byte
	en = NewFrame()

	if en == nil {
		t.Fatal("Memory not allocated.")
	}

	if en.CPtr == nil {
		t.Fatal("Unsafe pointer not set")
	}

	// add data
	for i := range diskmanager.PAGE_SIZE_BYTES {
		if i != 0 {
			d[i] = 0x01
		}
	}

	t.Run("Mark Dirty", func(t *testing.T) {
		en.MarkDirty()

		dirty := en.isDirty()
		if !dirty {
			t.Error("Expected frame to be marked as dirty")
		}

		pgeData, err := en.ByteData()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		set := helpers.BitIsSet(&(pgeData[0]), diskmanager.Dirty)
		if !set {
			t.Fatalf("Expected dirty bit to be set")
		}
	})
}

func TestMarkClean(t *testing.T) {
	var en *Frame
	var d [diskmanager.PAGE_SIZE_BYTES]byte
	en = NewFrame()

	if en == nil {
		t.Fatal("Memory not allocated.")
	}

	if en.CPtr == nil {
		t.Fatal("Unsafe pointer not set")
	}

	// add data
	for i := range diskmanager.PAGE_SIZE_BYTES {
		if i != 0 {
			d[i] = 0x01
		}
	}

	t.Run("Mark Clean", func(t *testing.T) {
		en.MarkClean()

		dirty := en.isDirty()
		if dirty {
			t.Error("Expected frame to be marked as clean")
		}

		pgeData, err := en.ByteData()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		set := helpers.BitIsSet(&(pgeData[0]), diskmanager.Dirty)
		if set {
			t.Fatalf("Expected dirty bit to be unset")
		}
	})
}
