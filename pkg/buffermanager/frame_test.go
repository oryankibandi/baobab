package buffermanager

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/oryankibandi/baobab/internal/manual"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/pager"

	"github.com/stretchr/testify/assert"
)

func TestSetData(t *testing.T) {
	var en *Frame

	p := manual.Alloc(unsafe.Sizeof(Frame{}))
	en = (*Frame)(p)
	if en == nil {
		t.Fatal("Memory not allocated.")
	}

	var d [pager.PAGE_SIZE_BYTES]byte

	key := uint32(25)

	// set key
	binary.LittleEndian.PutUint32(d[1:5], key)

	for i := range pager.PAGE_SIZE_BYTES {
		// skip page id slot
		if i >= 1 && i < 5 {
			continue
		}
		d[i] |= 0x01
	}

	pge := en.GetPage()
	if pge == nil {
		assert.NotNilf(t, pge, "Got nil page")
	}
	testPage := pager.Page{
		PageId: key,
	}
	pge.SwapData(&d)

	t.Run("set_data", func(t *testing.T) {
		err := en.SetData(&testPage, true)
		assert.Nilf(t, err, "Expected no error, got  %v", err)

		d2, err := en.ByteData()
		assert.Nilf(t, err, "Expected no error, got  %v", err)
		assert.Equalf(t, d, *d2, "Expected equal data during setData()")

		// check key
		k := en.getKey()
		assert.Equalf(t, key, k, "Expected key to be %d, got %d", key, k)
	})

	t.Cleanup(func() {
		manual.FreeMem(unsafe.Pointer(en))
		en = nil
		if en != nil {
			t.Fatal(fmt.Errorf("Memory not freed: %v\n", en))
		}
	})
}

func TestUpdateData(t *testing.T) {
	var en *Frame

	p := manual.Alloc(unsafe.Sizeof(Frame{}))
	en = (*Frame)(p)
	if en == nil {
		t.Fatal("Memory not allocated.")
	}

	var d [pager.PAGE_SIZE_BYTES]byte

	key := uint32(25)
	binary.LittleEndian.PutUint32(d[1:5], key)
	pge := pager.Page{}
	pge.SwapData(&d)

	for i := range pager.PAGE_SIZE_BYTES {
		d[i] = 1
	}

	t.Run("update_data", func(t *testing.T) {
		err := en.page.SwapData(&d)
		assert.Nilf(t, err, "Expected no error, got  %s", err)

		d2, err := en.page.GetPageByteData()
		assert.Nilf(t, err, "Expected no error, got  %s", err)
		assert.Equalf(t, d, *d2, "Expected equal data during setData()")
	})

	t.Cleanup(func() {
		manual.FreeMem(p)

		en = nil

		if en != nil {
			t.Fatal(fmt.Errorf("Memory not freed: %v\n", en))
		}
	})
}

func TestClear(t *testing.T) {
	var en *Frame

	p := manual.Alloc(unsafe.Sizeof(Frame{}))
	en = (*Frame)(p)
	if en == nil {
		t.Fatal("Memory not allocated.")
	}

	var d [pager.PAGE_SIZE_BYTES]byte
	for i := range pager.PAGE_SIZE_BYTES {
		d[i] = 1
	}
	key := uint32(25)

	pge := pager.Page{}
	pge.PageId = key
	pge.SwapData(&d)
	err := en.SetData(&pge, true)

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

	t.Cleanup(func() {
		manual.FreeMem(p)

		en = nil

		if en != nil {
			t.Fatal(fmt.Errorf("Memory not freed: %v\n", en))
		}
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

	var en *clockentry
	var fr *Frame

	t.Run("allocate_memory", func(t *testing.T) {
		p := manual.Alloc(unsafe.Sizeof(clockentry{}))
		en = (*clockentry)(p)
		if en == nil {
			t.Fatal("Memory not allocated.")
		}

		fr = &en.entry
		fr.parentEntry = en
	})

	t.Cleanup(func() {
		manual.FreeMem(unsafe.Pointer(en))
		en = nil
	})

	for i, test := range tests {
		t.Run(fmt.Sprintf("test_reference_%d", i), func(t *testing.T) {
			if test.operation == "reference" {
				fr.Reference()
			} else {
				fr.Unreference()
			}
			// check ref bit, acess bit and pin count
			assert.Equal(t, test.expectedAccBit, en.acc, "access bit not set")
			assert.Equal(t, test.expectedRefBit, en.ref, "reference bit not set")
			assert.Equal(t, test.expectedPinCount, en.pinCount.Load(), "Invalid pin count")
			assert.Equal(t, test.expectedUnpinCount, en.unpinCount.Load(), "Invalid unpin cunt")
		})
	}

}

func TestGetSegmentType(t *testing.T) {
	var en *clockentry
	var fr *Frame

	t.Run("allocate_memory", func(t *testing.T) {
		p := manual.Alloc(unsafe.Sizeof(clockentry{}))
		en = (*clockentry)(p)
		if en == nil {
			t.Fatal("Memory not allocated.")
		}

		fr = &en.entry
		fr.parentEntry = en
	})

	t.Cleanup(func() {
		manual.FreeMem(unsafe.Pointer(en))
		en = nil
	})

	t.Run("test_getsegmenttype", func(t *testing.T) {
		en.updateSegment(probationSegment)

		if s := fr.getEntrySegType(); s != probationSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected probation segment, got %v", s), t)
		}
	})
}

func TestRefAndUnrefConcurrent(t *testing.T) {
	// t.Parallel()
	testCount := 10
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

	var en *clockentry
	var fr *Frame

	t.Run("allocate_memory_concurrent", func(t *testing.T) {
		p := manual.Alloc(unsafe.Sizeof(clockentry{}))
		en = (*clockentry)(p)
		if en == nil {
			t.Fatal("Memory not allocated.")
		}

		fr = &en.entry
		fr.parentEntry = en
	})

	t.Cleanup(func() {
		manual.FreeMem(unsafe.Pointer(en))
	})

	start := make(chan struct{})
	for i, test := range tests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			t.Run(fmt.Sprintf("test_concurrent_%d", i), func(t *testing.T) {
				if test.operation == "reference" {
					fr.Reference()

					// check ref bit
					assert.Equal(t, true, en.isReferenced(), "reference bit not set")
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
			})

		}()
	}

	// start gorutines simultaneously
	close(start)
	wg.Wait()
}

func TestMarkDirty(t *testing.T) {
	var en *Frame
	var d [pager.PAGE_SIZE_BYTES]byte
	p := manual.Alloc(unsafe.Sizeof(Frame{}))
	en = (*Frame)(p)
	if en == nil {
		t.Fatal("Memory not allocated.")
	}

	t.Cleanup(func() {
		manual.FreeMem(unsafe.Pointer(en))
	})

	// add data
	for i := range pager.PAGE_SIZE_BYTES {
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

		set := helpers.BitIsSet(&(pgeData[0]), pager.Dirty)
		if !set {
			t.Fatalf("Expected dirty bit to be set")
		}
	})
}

func TestMarkClean(t *testing.T) {
	var en *Frame
	var d [pager.PAGE_SIZE_BYTES]byte
	p := manual.Alloc(unsafe.Sizeof(Frame{}))
	en = (*Frame)(p)
	if en == nil {
		t.Fatal("Memory not allocated.")
	}

	t.Cleanup(func() {
		manual.FreeMem(unsafe.Pointer(en))
	})

	// add data
	for i := range pager.PAGE_SIZE_BYTES {
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

		set := helpers.BitIsSet(&(pgeData[0]), pager.Dirty)
		if set {
			t.Fatalf("Expected dirty bit to be unset")
		}
	})

}
