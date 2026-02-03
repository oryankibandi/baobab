package buffermanager

import (
	"fmt"
	"sync"
	"testing"

	diskio "github.com/oryankibandi/baobab/pkg/disk_io"
	"github.com/stretchr/testify/assert"
)

func generateTestPageIds(n int) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = int32(i)
	}
	return out
}

func generateTestFrames(pgeIds []int32) []*Frame {
	var frames []*Frame

	for _, id := range pgeIds {
		page := diskio.NewTestPage(id)

		fr := &Frame{
			page:       page,
			Key:        uint32(id),
			IsInternal: false,
		}

		frames = append(frames, fr)
	}

	return frames

}

func TestAdd(t *testing.T) {
	type testItem struct {
		name  string
		fr    *Frame
		isNil bool
	}

	nonNilCount := 0x14 // 20
	pgeIds := generateTestPageIds(nonNilCount)
	frames := generateTestFrames(pgeIds)

	var tests []testItem
	for _, f := range frames {
		tests = append(tests, testItem{
			name:  "test",
			fr:    f,
			isNil: false,
		})
	}

	// Add nil values
	tests = append(tests, testItem{name: "is_nil", fr: nil, isNil: true})

	var lru ILRU

	lru = NewLru(100, "test_lru")

	assert.NotNil(t, lru, "LRU instance was not created.")

	var err error
	for _, tItem := range tests {
		t.Run(tItem.name, func(t *testing.T) {
			err = lru.Add(tItem.fr)

			if tItem.isNil {
				assert.NotNil(t, err, "Expected error, got nil")
				assert.Error(t, err, "Frame is nil, function should return an error")
			} else {
				assert.Nil(t, err, fmt.Errorf("Expected error to be nil, got %v", err))
				assert.NoError(t, err, fmt.Sprintf("Expected no error: got %v", err))

				head := lru.getHead()

				assert.NotNil(t, head, fmt.Sprintf("Expected head frame: %v, Got nil", head))
				assert.Equalf(t, tItem.fr, head, "Invalid frame  at  head of LRU")

				// Head and Tail have next and prev pointers respectivey
				availableFrames := lru.getCount() - lru.getPinnedFrameCount()

				if availableFrames > 1 {
					h := lru.getHead()
					tail := lru.getTail()
					assert.Nil(t, h.prev, fmt.Errorf("Expected head prev pointer to be nil -> %v", h))
					assert.NotNil(t, h.next, fmt.Errorf("Expected head next pointer to not be nil -> %v\n", h))

					assert.Nil(t, tail.next, fmt.Errorf("Expected tail next pointer to be nil -> %v\n", tail))
					assert.NotNil(t, tail.prev, fmt.Errorf("Expected tail prev to have a value -> %v\n", tail.prev))

					// ensure that added frame has pointer assigned
					assert.NotNil(t, tItem.fr.next, fmt.Errorf("Expected next pointer to have a value, got %v\n", tItem.fr))
				}

				// ensure no cyclic ptrs
				if tItem.fr.prev != nil || tItem.fr.next != nil {
					assert.NotEqual(t, tItem.fr.prev, tItem.fr.next, "Cyclic path detected")
				}
			}

		})
	}

	// check count
	c := lru.getCount()
	assert.Equal(t, uint64(nonNilCount), c, "Invalid count")

	// check tail
	tail := lru.getTail()
	assert.NotNil(t, tail, fmt.Sprintf("Expected tail frame: %v, Got nil", tail))
	assert.NotNil(t, tail.prev, "Tail previous pointer is nil")
	assert.Nil(t, tail.next, "Tail next pointer is not nil")

	// check cyclic pointers
	for _, s := range tests {
		if s.fr != nil {
			t.Run("(add) cyclic pointer", func(t *testing.T) {
				// ensure no cyclic ptrs
				if s.fr.prev != nil || s.fr.next != nil {
					assert.NotEqual(t, s.fr.prev, s.fr.next, "Cyclic path detected")
				}
			})
		}
	}
}

func TestPop(t *testing.T) {
	type testItem struct {
		name string
		fr   *Frame
	}

	nonNilCount := 0x14
	pgeIds := generateTestPageIds(nonNilCount)
	frames := generateTestFrames(pgeIds)

	var tests []testItem
	for i, f := range frames {
		tests = append(tests, testItem{
			name: fmt.Sprintf("%d_test", i),
			fr:   f,
		})
	}

	var lru ILRU

	lru = NewLru(100, "test_lru")

	assert.NotNil(t, lru, "LRU instance was not created.")

	var err error
	for _, tItem := range tests {
		t.Run(tItem.name, func(t *testing.T) {
			err = lru.Add(tItem.fr)
			assert.Nil(t, err, fmt.Errorf("Expected error to be nil, got %v", err))

			head := lru.getHead()

			assert.NotNil(t, head, fmt.Sprintf("Expected head frame: %v, Got nil", head))
			assert.Equalf(t, tItem.fr, head, "Invalid frame  at  head of LRU")

			// ensure no cyclic ptrs
			if tItem.fr.prev != nil || tItem.fr.next != nil {
				assert.NotEqual(t, tItem.fr.prev, tItem.fr.next, "Cyclic path detected")
			}
		})
	}

	// check count
	c := lru.getCount()
	assert.Equal(t, uint64(nonNilCount), c, "Invalid count")

	// pop items
	var tail *Frame
	for i := range nonNilCount {
		t.Run(fmt.Sprintf("%d_pop", i), func(t *testing.T) {
			tail = lru.Pop()

			if len(tests) > 0 {
				expected := tests[0]

				assert.NotNil(t, tail, "No tail from pop() function")
				assert.Equal(t, expected.fr, tail, "Invalid tail received.")

				// ensure pointers are zeroed out
				assert.Nil(t, tail.prev, fmt.Errorf("expected tail previous  pointer to be zeroed out -> %v\n", tail.prev))
				assert.Nil(t, tail.next, fmt.Errorf("expected tail next pointer to be zeroed out -> %v\n", tail.next))

				tests = tests[1:]

			} else {
				assert.Nil(t, tail, "Expected a nil frame.")
			}

			// check count
			c := lru.getCount()

			assert.Equal(t, uint64(len(tests)), c, "Invalid frame count")

			// check tail
			frameCount := lru.getCount()
			pinnedCount := lru.getPinnedFrameCount()
			available := frameCount - pinnedCount

			if available > 1 {
				tail := lru.getTail()
				assert.NotNil(t, tail, fmt.Sprintf("Expected tail frame: %v, Got nil", tail))
				assert.NotNil(t, tail.prev, fmt.Errorf("Tail previous pointer is nil. Available frames -> %d\n", available))
				assert.Nil(t, tail.next, "Tail next pointer is not nil")
			}

		})
	}

	for _, s := range tests {
		t.Run("(pop) cyclic pointer", func(t *testing.T) {
			// ensure no cyclic ptrs
			if s.fr.prev != nil || s.fr.next != nil {
				assert.NotEqual(t, s.fr.prev, s.fr.next, "Cyclic path detected")
			}
		})
	}
}

func TestPinAndUnpin(t *testing.T) {
	type testItem struct {
		name string
		fr   *Frame
	}

	nonNilCount := 0x14
	pgeIds := generateTestPageIds(nonNilCount)
	frames := generateTestFrames(pgeIds)

	var tests []testItem
	for i, f := range frames {
		tests = append(tests, testItem{
			name: fmt.Sprintf("%d_test", i),
			fr:   f,
		})
	}

	var lru ILRU

	lru = NewLru(100, "test_lru")

	assert.NotNil(t, lru, "LRU instance was not created.")

	var err error
	// add items
	for _, tItem := range tests {
		t.Run(tItem.name, func(t *testing.T) {
			err = lru.Add(tItem.fr)
			assert.Nil(t, err, fmt.Errorf("Expected error to be nil, got %v", err))

			head := lru.getHead()

			assert.NotNil(t, head, fmt.Sprintf("Expected head frame: %v, Got nil", head))
			assert.Equalf(t, tItem.fr, head, "Invalid frame at head of LRU")
		})
	}

	// check count
	c := lru.getCount()
	assert.Equal(t, uint64(nonNilCount), c, "Invalid count")

	// pin items
	// Add Nil frame
	nItem := testItem{
		name: "nil frame",
		fr:   nil,
	}

	tests = append(tests, nItem)

	var pCount uint64
	var framePinCount uint32
	var myErr LRUError
	for i, item := range tests {
		t.Run(item.name, func(t *testing.T) {
			err = lru.RemoveFrame(item.fr)

			// assert error
			if item.fr != nil {
				assert.NoError(t, err, "Expected no error, received error from RemoveFrame")

				// check count, should remain constant
				c = lru.getCount()

				assert.Equal(t, uint64(nonNilCount), c, "Invalid count after removing frame")

				// check pinned count, shound increase
				pCount = lru.getPinnedFrameCount()

				assert.Equal(t, uint64(i+1), pCount, "Invalid pinned frames count")

				// Check individual frame pin count
				framePinCount = item.fr.GetPinCount()

				assert.Equal(t, uint32(1), framePinCount, "frame pin count not incremented")

				// Ensure frame pointers are nil after pinning
				assert.Nil(t, item.fr.prev, "Expected previous pointer to be nil")

				assert.Nil(t, item.fr.next, "Expected next pointer to be nil")

				// ensure no cyclic pointers
				for _, s := range tests {
					if s.fr != nil {
						t.Run("(pin) cyclic pointer", func(t *testing.T) {
							// ensure no cyclic ptrs
							if s.fr.prev != nil || s.fr.next != nil {
								assert.NotEqual(t, s.fr.prev, s.fr.next, "Cyclic path detected")
							}
						})
					}
				}

				// check tail
				frameCount := lru.getCount()
				pinnedCount := lru.getPinnedFrameCount()
				available := frameCount - pinnedCount

				if available > 1 {
					tail := lru.getTail()
					assert.NotNil(t, tail, fmt.Sprintf("Expected tail frame: %v, Got nil", tail))
					assert.NotNil(t, tail.prev, fmt.Errorf("Tail previous pointer is nil. Available frames -> %d\n", available))
					assert.Nil(t, tail.next, "Tail next pointer is not nil")
				}

			} else {
				assert.Error(t, err, "expected error from remove frame, received nil")

				assert.ErrorAs(t, err, &myErr, fmt.Errorf("Invalid error type. Expected LRUError, got %v", err))
			}
		})
	}

	// Unpin frames
	for i, item := range tests {
		t.Run(item.name, func(t *testing.T) {
			err = lru.ReAddFrame(item.fr)

			if item.fr != nil {
				assert.NoError(t, err, "Expected no error, received error from RemoveFrame")

				// check count, should remain constant
				c = lru.getCount()

				assert.Equal(t, uint64(nonNilCount), c, "Invalid count after removing frame")

				// check pinned frame count. chould decrease
				pCount = lru.getPinnedFrameCount()

				assert.Equal(t, uint64(nonNilCount-1-i), pCount, "Invalid pin count during unpinning")

				// check frame pin count
				// assert.Equal(t, uint32(0), item.fr.GetPinCount(), "Invalid frame pin count")

				// check that readded frame is head
				h := lru.getHead()

				assert.Equal(t, item.fr, h, "Expected item to be added to head")

				// ensure no cyclic ptrs
				for _, s := range tests {
					if s.fr != nil {
						t.Run("(unpin) cyclic pointer", func(t *testing.T) {
							// ensure no cyclic ptrs
							if s.fr.prev != nil || s.fr.next != nil {
								assert.NotEqual(t, s.fr.prev, s.fr.next, "Cyclic path detected")
							}
						})
					}
				}

				// check tail
				frameCount := lru.getCount()
				pinnedCount := lru.getPinnedFrameCount()
				available := frameCount - pinnedCount

				if available > 1 {
					tail := lru.getTail()
					assert.NotNil(t, tail, fmt.Sprintf("Expected tail frame: %v, Got nil", tail))
					assert.NotNil(t, tail.prev, fmt.Errorf("Tail previous pointer is nil. Available frames -> %d\n", available))
					assert.Nil(t, tail.next, "Tail next pointer is not nil")

					assert.NotNil(t, item.fr.next, fmt.Errorf("Expected next pointer to have a value, got %v\n", item.fr))
				}

			} else {
				assert.Error(t, err, "expected error from remove frame, received nil")

				assert.ErrorAs(t, err, &myErr, "Invalid error type")

			}
		})
	}
}

func TestSetMostRecent(t *testing.T) {
	type testItem struct {
		name string
		fr   *Frame
	}

	nonNilCount := 0x14
	pgeIds := generateTestPageIds(nonNilCount)
	frames := generateTestFrames(pgeIds)

	var tests []testItem
	for i, f := range frames {
		tests = append(tests, testItem{
			name: fmt.Sprintf("%d_test", i),
			fr:   f,
		})
	}

	var lru ILRU

	lru = NewLru(100, "test_lru")

	assert.NotNil(t, lru, "LRU instance was not created.")

	var err error

	// add items to LRU
	for _, tItem := range tests {
		t.Run(tItem.name, func(t *testing.T) {
			err = lru.Add(tItem.fr)
			assert.Nil(t, err, fmt.Errorf("Expected error to be nil, got %v", err))

			head := lru.getHead()

			assert.NotNil(t, head, fmt.Sprintf("Expected head frame: %v, Got nil", head))
			assert.Equalf(t, tItem.fr, head, "Invalid frame  at  head of LRU")
		})
	}

	// check count
	c := lru.getCount()
	assert.Equal(t, uint64(nonNilCount), c, "Invalid count")

	// test nil frame
	setMostRecentTests := []struct {
		name        string
		fr          *Frame
		expectError bool
	}{
		{name: "nil frame", fr: nil, expectError: true},
		{name: "head frame", fr: tests[len(tests)-1].fr, expectError: false},
		{name: "mid frame", fr: tests[len(tests)/2].fr, expectError: false},
	}

	var myErr LRUError
	err = nil
	for _, s := range setMostRecentTests {
		t.Run(s.name, func(t *testing.T) {
			err = lru.SetMostRecent(s.fr)

			if s.expectError {
				assert.Error(t, err, "Expected error, got nil")
				assert.ErrorAs(t, err, &myErr, "Expected LRUError{}")
			} else {
				assert.NoError(t, err, fmt.Errorf("Expected no error, got %v", err))

				// ensure count has not reduced
				c = lru.getCount()
				assert.Equal(t, uint64(nonNilCount), c, "Invalid count")

				// check that item is set as head
				h := lru.getHead()
				assert.Equal(t, s.fr, h, "expected item to be set as head")

				for _, item := range tests {
					t.Run("cyclic pointer", func(t *testing.T) {
						// ensure no cyclic ptrs
						if item.fr.prev != nil || item.fr.next != nil {
							assert.NotEqual(t, item.fr.prev, item.fr.next, "Cyclic path detected")
						}
					})
				}

				// check tail
				frameCount := lru.getCount()
				pinnedCount := lru.getPinnedFrameCount()
				available := frameCount - pinnedCount

				if available > 1 {
					tail := lru.getTail()
					assert.NotNil(t, tail, fmt.Sprintf("Expected tail frame: %v, Got nil", tail))
					assert.NotNil(t, tail.prev, fmt.Errorf("Tail previous pointer is nil. Available frames -> %d\n", available))
					assert.Nil(t, tail.next, "Tail next pointer is not nil")

					assert.NotNil(t, s.fr.next, fmt.Errorf("Expected next pointer to have a value, got %v\n", s.fr))
				}
			}
		})
	}

	for _, s := range tests {
		t.Run("cyclic pointer", func(t *testing.T) {
			// ensure no cyclic ptrs
			if s.fr.prev != nil || s.fr.next != nil {
				assert.NotEqual(t, s.fr.prev, s.fr.next, "Cyclic path detected")
			}
		})
	}
}

func TestDelete(t *testing.T) {
	type testItem struct {
		name string
		fr   *Frame
	}

	nonNilCount := 0x14
	pgeIds := generateTestPageIds(nonNilCount)
	frames := generateTestFrames(pgeIds)

	var tests []testItem
	for i, f := range frames {
		tests = append(tests, testItem{
			name: fmt.Sprintf("%d_test", i),
			fr:   f,
		})
	}

	var lru ILRU

	lru = NewLru(100, "test_lru")

	assert.NotNil(t, lru, "LRU instance was not created.")

	var err error

	// add items to LRU
	for _, tItem := range tests {
		t.Run(tItem.name, func(t *testing.T) {
			err = lru.Add(tItem.fr)
			assert.Nil(t, err, fmt.Errorf("Expected error to be nil, got %v", err))

			head := lru.getHead()

			assert.NotNil(t, head, fmt.Sprintf("Expected head frame: %v, Got nil", head))
			assert.Equalf(t, tItem.fr, head, "Invalid frame  at  head of LRU")
		})
	}

	// check count
	c := lru.getCount()
	assert.Equal(t, uint64(nonNilCount), c, "Invalid count")

	var myErr LRUError
	// delete frames
	tests = append(tests, testItem{name: "nil frame", fr: nil})

	for i, s := range tests {
		err = lru.Delete(s.fr)

		if s.fr == nil {
			// expect error
			assert.Error(t, err, "Expected error, got nil")
			assert.ErrorAs(t, err, &myErr, fmt.Errorf("Expected error of type LRUError, got %v", err))
		} else {
			assert.NoError(t, err, fmt.Errorf("Expected no error, got %v", err))

			// check count
			c = lru.getCount()
			assert.Equal(t, uint64(nonNilCount-(i+1)), c, "Invalid count")

			// check that frame pointers are zeroed out
			assert.Nil(t, s.fr.prev, "expected prev pointer to be nil")
			assert.Nil(t, s.fr.next, "expected next pointer to be nil")

			for _, tItem := range tests {
				if tItem.fr != nil {
					t.Run("cyclic pointer", func(t *testing.T) {
						// ensure no cyclic ptrs
						if tItem.fr.prev != nil || tItem.fr.next != nil {
							assert.NotEqual(t, tItem.fr.prev, tItem.fr.next, "Cyclic path detected")
						}
					})
				}
			}

			// check tail
			frameCount := lru.getCount()
			pinnedCount := lru.getPinnedFrameCount()
			available := frameCount - pinnedCount

			if available > 1 {
				tail := lru.getTail()
				assert.NotNil(t, tail, fmt.Sprintf("Expected tail frame: %v, Got nil", tail))
				assert.NotNil(t, tail.prev, fmt.Errorf("Tail previous pointer is nil. Available frames -> %d\n", available))
				assert.Nil(t, tail.next, "Tail next pointer is not nil")

				assert.Nil(t, s.fr.next, fmt.Errorf("Expected next pointer be nil, got %v\n", s.fr))
				assert.Nil(t, s.fr.prev, fmt.Errorf("Expected prev pointer to be nil, got %v\n", s.fr))
			}

		}
	}
}

func TestConcurrentPin(t *testing.T) {
	type testItem struct {
		name string
		fr   *Frame
	}

	nonNilCount := 0xFA
	pgeIds := generateTestPageIds(nonNilCount)
	frames := generateTestFrames(pgeIds)

	var tests []testItem
	for i, f := range frames {
		tests = append(tests, testItem{
			name: fmt.Sprintf("%d_test", i),
			fr:   f,
		})
	}

	var lru ILRU

	lru = NewLru(1000, "test_lru")

	assert.NotNil(t, lru, "LRU instance was not created.")

	var err error
	// add items
	for _, tItem := range tests {
		t.Run(tItem.name, func(t *testing.T) {
			err = lru.Add(tItem.fr)
			assert.Nil(t, err, fmt.Errorf("Expected error to be nil, got %v", err))

			head := lru.getHead()

			assert.NotNil(t, head, fmt.Sprintf("Expected head frame: %v, Got nil", head))
			assert.Equalf(t, tItem.fr, head, "Invalid frame at head of LRU")
		})
	}

	// check count
	c := lru.getCount()
	assert.Equal(t, uint64(nonNilCount), c, "Invalid count")

	// slice test array
	mid := nonNilCount / 2
	qtr := float32(nonNilCount) * 0.25
	pinSlice := tests[uint32(mid)-uint32(qtr) : uint32(mid)+uint32(qtr)]

	var wg sync.WaitGroup
	start := make(chan struct{})

	// pin concurrently
	for _, item := range pinSlice {
		wg.Add(1)
		go func(fr *Frame) {
			defer wg.Done()
			<-start
			t.Run(item.name, func(t *testing.T) {
				err := lru.RemoveFrame(fr)

				assert.Nil(t, err, fmt.Errorf("Expected no error, got %s\n", err.Error()))

				// check state
				totFrames := lru.getCount()
				pinned := lru.getPinnedFrameCount()
				available := totFrames - pinned
				head := lru.getHead()
				tail := lru.getTail()

				if available == 1 {
					assert.Equal(t, head, tail, fmt.Errorf("Expected head and tail to be equal with a count of one, got:\nHead -> %v\nTail -> %v\n", head, tail))
				}
			})

		}(item.fr)
	}

	// kick start goroutines
	close(start)
}
