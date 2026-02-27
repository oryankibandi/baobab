package tinylfu

import (
	"errors"
	"fmt"
	"math"
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
