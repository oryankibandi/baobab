package tinylfu

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
)

func TestNewCMS(t *testing.T) {
	tests := []struct {
		errorRate   float64
		probability float64
		h           Hasher
		paramError  bool
	}{
		{errorRate: 0.1, probability: 0.01, h: NewMapHash(), paramError: false},
		{errorRate: 0.0, probability: 0.01, h: NewMapHash(), paramError: true},
		{errorRate: 0.01, probability: 0.0, h: NewMapHash(), paramError: true},
		{errorRate: 0.01, probability: 0.01, h: NewMapHash(), paramError: false},
		{errorRate: 0.23, probability: 0.111, h: NewMapHash(), paramError: false},
		{errorRate: 0.28, probability: 0.04, h: NewMapHash(), paramError: false},
		{errorRate: 0.001, probability: 0.01, h: nil, paramError: true},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_test_newCMS", i), func(t *testing.T) {
			cSketch, err := NewCMS(test.errorRate, test.probability, test.h)

			if test.paramError {
				if err == nil {
					t.Fatal("Expected error, got nil")
				}

				if !errors.As(err, &CountMinSketchError{}) {
					t.Fatalf("Expected error of type CountMinSketchError{}, got %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				// calculate w and d
				w := uint64(math.Ceil(2 / (test.errorRate / 100)))
				d := uint64(math.Ceil(math.Log((test.probability / 100)) / math.Log(0.5)))

				if w != cSketch.width {
					t.Fatalf("Expected width of %d, got %d", w, cSketch.width)
				}

				if d != cSketch.k {
					t.Fatalf("Expected a depth of %d, got %d", d, cSketch.k)
				}
			}
		})

	}
}

func TestGetCount(t *testing.T) {
	tests := []struct {
		key   []byte
		count uint64
	}{
		{key: []byte("john"), count: 25},
		{key: []byte("jane"), count: 54},
		{key: []byte("age"), count: 48},
		{key: []byte("milk"), count: 69},
		{key: []byte("croissant"), count: 96},
		{key: []byte("cheese"), count: 486},
	}

	var errRate float64 = 0.1
	var probability float64 = 0.01

	cSketch, err := NewCMS(errRate, probability, NewMapHash())

	if err != nil {
		t.Fatalf("Expected not error, got %v", err)
	}

	if cSketch == nil {
		t.Fatal("Expected CM Sketch,  got nil")
	}

	for i, test := range tests {
		for j := range test.count {
			t.Run(fmt.Sprintf("%d_test_getcount_increment", j), func(t *testing.T) {
				_, err := cSketch.Increment(test.key)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
			})
		}
		t.Run(fmt.Sprintf("%d_test_getcount", i), func(t *testing.T) {
			c, err := cSketch.GetCount(test.key)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if c <= 0 {
				t.Fatalf("Expected count to be greater than zero")
			}
		})
	}
}

func TestReset(t *testing.T) {
	tests := []struct {
		key   []byte
		count uint64
	}{
		{key: []byte("john"), count: 25},
		{key: []byte("jane"), count: 54},
		{key: []byte("age"), count: 48},
		{key: []byte("milk"), count: 69},
		{key: []byte("croissant"), count: 96},
		{key: []byte("cheese"), count: 486},
	}

	var errRate float64 = 0.1
	var probability float64 = 0.1

	cSketch, err := NewCMS(errRate, probability, NewMapHash())

	if err != nil {
		t.Fatalf("Expected not error, got %v", err)
	}

	if cSketch == nil {
		t.Fatal("Expected CM Sketch,  got nil")
	}

	// add values
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_testreset_increment", i), func(t *testing.T) {
			for j := range test.count {
				t.Run(fmt.Sprintf("%d_testreset_increment_%d", i, j), func(t *testing.T) {
					_, err := cSketch.Increment(test.key)
					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

				})
			}

		})
	}

	// make a copy of the array
	tmp := make([][]uint64, cSketch.k)

	for i := range cSketch.k {
		tmp[i] = make([]uint64, cSketch.width)
		copy(tmp[i], cSketch.arr[i])
	}

	// reset
	t.Run("testreset_reset", func(t *testing.T) {
		cSketch.reset()

		// check array
		for i := range cSketch.k {
			t.Run(fmt.Sprintf("%d_testreset_arraymodified", i), func(t *testing.T) {
				equal := slices.Equal(cSketch.arr[i], tmp[i])
				// ensure original array did not contain zeros
				originalWasZero := slices.Equal(tmp[i], make([]uint64, cSketch.k))

				if equal && !originalWasZero {
					t.Fatalf("Slice at idx %d not modified", i)
				}
			})

			for j, counter := range tmp[i] {
				t.Run(fmt.Sprintf("%d_testreset_counterhalved", j), func(t *testing.T) {
					h := uint64(counter / 2)

					if cSketch.arr[i][j] != h {
						t.Fatalf("Expected halved counter to be %d , but got %d", h, cSketch.arr[i][j])
					}
				})
			}

		}
	})
}

// TODO: ADd test for Uniform and Zipfian distributions
