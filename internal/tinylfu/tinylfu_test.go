package tinylfu

import (
	"fmt"
	"testing"

	"github.com/oryankibandi/baobab/pkg/helpers"
)

func TestNew(t *testing.T) {
	t.Run("new_tinylfu", func(t *testing.T) {
		tiny, err := NewTinyLFU()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if tiny.Doorkeeper == nil {
			t.Fatalf("Expected doorkeeper to be initialized, got nil")
		}

		if tiny.MainStruct == nil {
			t.Fatalf("Expected the Count-min Sketch main structure to be initialized, got nil")
		}
	})
}

func TestIncrement(t *testing.T) {
	data := []byte("tinylfu")

	tiny, err := NewTinyLFU()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Test invalid data
	t.Run("test_increment_invalid_data", func(t *testing.T) {
		var d []byte

		err = tiny.IncrementItem(d)
		if err == nil {
			t.Fatal("Expected error, got nil")
		}
	})

	// test that first entry is added in doorkeeper and not main structure
	t.Run("test_tinylfu_doorkeeper", func(t *testing.T) {
		err = tiny.IncrementItem(data)
		if err != nil {
			t.Fatalf("Expected no error, got nil")
		}

		count, err := tiny.CheckItemCount(data)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if count > 0 {
			t.Fatalf("Expected first entry to be added to doorkeeper, got a count of %d on main structure", count)
		}
	})

	// Test that an item is indeed incremented
	t.Run("tinylfu_increment_data", func(t *testing.T) {
		// increment item count to just below sample size
		for i := range SAMPLE_SIZE - 1 {
			t.Run(fmt.Sprintf("%d_tinylfu_increment_data", i), func(t *testing.T) {
				err = tiny.IncrementItem(data)
				if err != nil {
					t.Fatalf("Expected no error, got nil")
				}
			})
		}

		count, err := tiny.CheckItemCount(data)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if count <= 0 || count > SAMPLE_SIZE-1 {
			t.Fatalf("Invalid count, expected 0 > count <= actual count, got %v", count)
		}
	})

	// Test that the count is halved after reset operation
	t.Run("tinylfu_reset", func(t *testing.T) {
		initialCount, err := tiny.CheckItemCount(data)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Increment once to trigger reset operation
		err = tiny.IncrementItem(data)
		if err != nil {
			t.Fatalf("Expected no error, got nil")
		}

		secCount, err := tiny.CheckItemCount(data)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// expect secCount < initialCount
		if secCount > initialCount {
			t.Fatalf("Expected reset operation did not occur.")
		}

		// check that counters in CMS  are halved. Cater for rounding off.
		if secCount < (initialCount/2)-1 && secCount > (initialCount/2)+1 {
			t.Fatalf(helpers.BOLDRED+"Expected reset operation to halve counters. Initial count: %d. Count after: %d"+helpers.RESET, initialCount, secCount)
		}

		// check that doorkeepper is cleared
		// incrementing should just add it to doorkeeper and not CMS.
		err = tiny.IncrementItem(data)
		if err != nil {
			t.Fatalf("Expected no error, got nil")
		}

		thirdCount, err := tiny.CheckItemCount(data)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if thirdCount != secCount {
			t.Fatalf("Expected  doorkeepper to be cleared.")
		}
	})
}
