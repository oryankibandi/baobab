package buffermanager

import (
	"fmt"
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
			}

			head := lru.getHead()

			if !tItem.isNil {
				assert.NotNil(t, head, fmt.Sprintf("Expected head frame: %v, Got nil", head))
				assert.Equalf(t, tItem.fr, head, "Invalid frame  at  head of LRU")
			}
		})
	}

	// check count
	c := lru.getCount()
	assert.Equal(t, uint64(nonNilCount), c, "Invalid count")
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
				tests = tests[1:]

			} else {
				assert.Nil(t, tail, "Expected a nil frame.")
			}

			// check count
			c := lru.getCount()

			assert.Equal(t, uint64(len(tests)), c, "Invalid frame count")
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
				assert.Equal(t, uint32(0), item.fr.GetPinCount(), "Invalid frame pin count")

				// check that readded frame is head
				h := lru.getHead()

				assert.Equal(t, item.fr, h, "Expected item to be added to head")
			} else {
				assert.Error(t, err, "expected error from remove frame, received nil")

				assert.ErrorAs(t, err, &myErr, "Invalid error type")

			}
		})
	}
}
