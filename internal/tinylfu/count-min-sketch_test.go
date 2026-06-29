package tinylfu

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/oryankibandi/baobab/internal/zipf"
	"github.com/oryankibandi/baobab/pkg/helpers"
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

			// count should never be below the actual
			if c < int64(test.count) {
				t.Fatalf("Expected count not to be less than the actual")
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

// Uniform distributions
func TestNormalDistribution(t *testing.T) {
	var wg sync.WaitGroup
	n := 1000 // number of distinct keys
	ε := 0.1  // error rate(ε) 0.1%
	δ := 0.1  // error probability(δ)  0.1%
	totOperations := 0

	type tTest struct {
		key   []byte
		count uint64
	}

	tests := make([]tTest, n)
	var test tTest

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := range n {
		test.key = []byte(helpers.RandomString(5))
		test.count = uint64(r.Intn(80) + 500)
		tests[i] = test
		totOperations += int(test.count)
	}

	cSketch, err := NewCMS(ε, δ, NewMapHash())

	if err != nil {
		t.Fatalf("Expected not error, got %v", err)
	}

	if cSketch == nil {
		t.Fatal("Expected CM Sketch,  got nil")
	}

	// increment count
	for i, test := range tests {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := range test.count {
				t.Run(fmt.Sprintf("%d_%d_test_normaldistribution_increment", i, j), func(t *testing.T) {
					_, err := cSketch.Increment(test.key)

					if err != nil {
						t.Fatalf("Expected no error during incrementing count, got %v", err)
					}
				})
			}
		}(i)
	}
	wg.Wait()

	errCount := 0
	for i, test := range tests {
		// check count
		t.Run(fmt.Sprintf("%d_test_normaldistribution_checker", i), func(t *testing.T) {
			currCount, err := cSketch.GetCount(test.key)
			if err != nil {
				t.Fatalf("Expected not error, got %v", err)
			}

			absoluteErr := math.Abs(float64(currCount - int64(test.count)))
			relativeErr := math.Abs((absoluteErr / float64(test.count)) * 100)
			t.Logf("Relative error: %f", relativeErr)

			// At most δ num of keys should violate this bound.
			exceeded := float64(currCount) > float64(test.count)+((ε/100)*float64(totOperations))
			if exceeded {
				errCount++
			}
		})
	}

	t.Run("test_normaldistribution_probabilitybound", func(t *testing.T) {
		expectedErrorCount := δ * float64(totOperations)
		fmt.Printf("Expected errors: %d, Received  errors: %d\n", uint64(expectedErrorCount), errCount)
		if errCount > int(expectedErrorCount) {
			t.Fatalf("Expected error count <= %d, but got error count of %d", errCount, uint64(expectedErrorCount))
		}
	})
}

// Zipfian/skewed distribution
func TestZipfianDistribution(t *testing.T) {
	n := 10000 // number of distinct keys
	ε := 0.1   // error rate(ε) 0.1%
	δ := 0.1   // error probability(δ)  0.1%
	totOperations := 0

	// zipf parameters
	var offset float64 = 200
	var zipfExponent float64 = 2.3
	var imax float64 = 5000

	type tTest struct {
		key   []byte
		count uint64
	}

	tests := make([]tTest, n)
	var test tTest

	z := zipf.NewZipf(zipfExponent, float64(offset), float64(imax))
	if z == nil {
		panic("Unable to create Zipf generator")
	}

	for i := range n {
		test.key = []byte(helpers.RandomString(5))
		// use rejection-inversion to sample values
		test.count = z.GetNext()
		tests[i] = test
		totOperations += int(test.count)
	}

	cSketch, err := NewCMS(ε, δ, NewMapHash())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if cSketch == nil {
		t.Fatal("Expected CM Sketch,  got nil")
	}

	// increment count
	for i, test := range tests {
		for j := range test.count {
			t.Run(fmt.Sprintf("%d_%d_test_zipfiandistribution_increment", i, j), func(t *testing.T) {
				_, err := cSketch.Increment(test.key)

				if err != nil {
					t.Fatalf("Expected no error during incrementing count, got %v", err)
				}
			})
		}
	}

	errCount := 0
	for i, test := range tests {
		// check count
		t.Run(fmt.Sprintf("%d_test_zipfiandistribution_checker", i), func(t *testing.T) {
			currCount, err := cSketch.GetCount(test.key)
			if err != nil {
				t.Fatalf("Expected not error, got %v", err)
			}

			absoluteErr := math.Abs(float64(currCount - int64(test.count)))
			relativeErr := math.Abs((absoluteErr / float64(test.count)) * 100)
			t.Logf("Relative error: %f", relativeErr)

			// At most δ num of keys should violate this bound.
			exceeded := float64(currCount) > float64(test.count)+((ε/100)*float64(totOperations))
			if exceeded {
				errCount++
			}
		})
	}

	t.Run("test_zipfiandistribution_probabilitybound", func(t *testing.T) {
		expectedErrorCount := δ * float64(totOperations)
		fmt.Printf("Expected errors: %d, Received  errors: %d\n", uint64(expectedErrorCount), errCount)
		if errCount > int(expectedErrorCount) {
			t.Fatalf("Expected error count <= %d, but got error count of %d", errCount, uint64(expectedErrorCount))
		}
	})

}
