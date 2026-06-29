package tinylfu

import (
	"fmt"
	"slices"
	"testing"

	"github.com/oryankibandi/baobab/pkg/helpers"
)

func TestNewDoorkeeper(t *testing.T) {
	tests := []struct {
		bitArrSize    uint64
		hashFuncCount uint64
		hasher        Hasher
		valid         bool
	}{
		{bitArrSize: 0, hashFuncCount: 2, hasher: NewMapHash(), valid: false},
		{bitArrSize: 24, hashFuncCount: 0, hasher: NewMapHash(), valid: false},
		{bitArrSize: 1024, hashFuncCount: 10, hasher: NewMapHash(), valid: true},
		{bitArrSize: 64, hashFuncCount: 4, hasher: nil, valid: false},
		{bitArrSize: 256, hashFuncCount: 5, hasher: NewMapHash(), valid: true},
		{bitArrSize: 8192, hashFuncCount: 10, hasher: NewMapHash(), valid: true},
		{bitArrSize: 4096, hashFuncCount: 50, hasher: nil, valid: false},
		{bitArrSize: 97, hashFuncCount: 50, hasher: NewMapHash(), valid: false},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_test_newdoorkeeper", i), func(t *testing.T) {
			d, err := NewDoorkeeper(test.bitArrSize, test.hashFuncCount, test.hasher)

			if test.valid {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if d == nil {
					t.Fatalf("Expected new doorkeeper, got nil")
				}
			} else {
				if err == nil {
					t.Fatal("Expected error, got nil.")
				}
			}
		})
	}
}

func TestAdd(t *testing.T) {
	type sTest struct {
		key []byte
	}
	testCount := 50
	tests := []sTest{}

	for range testCount {
		tests = append(tests, sTest{key: []byte(helpers.RandomString(10))})
	}

	dk, err := NewDoorkeeper(4096, 10, NewMapHash())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if dk == nil {
		t.Fatal("Expected doorkeeper instance, got nil")
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_test_add", i), func(t *testing.T) {
			err := dk.Add(test.key)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// check for false negatives
			exists := dk.Check(test.key)
			if !exists {
				t.Fatalf("Expected key to be in doorkeeper, gt false")
			}
		})
	}
}

// TODO: Test clearing an item from doorkeeper.

func TestClear(t *testing.T) {
	type sTest struct {
		key []byte
	}
	testCount := 20
	tests := []sTest{}

	for range testCount {
		tests = append(tests, sTest{key: []byte(helpers.RandomString(10))})
	}

	dk, err := NewDoorkeeper(4096, 10, NewMapHash())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if dk == nil {
		t.Fatal("Expected doorkeeper instance, got nil")
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_testclear_add", i), func(t *testing.T) {
			err := dk.Add(test.key)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
		})
	}

	t.Run("testclear_clear", func(t *testing.T) {
		dk.Clear()

		for i, test := range tests {
			t.Run(fmt.Sprintf("%d_testclear_checkcleared", i), func(t *testing.T) {
				exists := dk.Check(test.key)
				if exists {
					t.Fatalf("Expected doorkeeper to be cleared, got a false positive.")
				}

			})
		}
	})
}

func TestSetBit(t *testing.T) {
	tests := []struct {
		bitArrLen      uint64
		indices        []uint64
		expectedResult []byte
	}{
		{bitArrLen: 8, indices: []uint64{1, 3, 5}, expectedResult: []byte{0x54}},
		{bitArrLen: 16, indices: []uint64{2, 5, 6, 10, 13}, expectedResult: []byte{0x26, 0x24}},
		{bitArrLen: 24, indices: []uint64{1, 3, 4, 8, 10, 12, 13, 18, 19}, expectedResult: []byte{0x58, 0xAC, 0x30}},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_test_setbit", i), func(t *testing.T) {
			dk, err := NewDoorkeeper(test.bitArrLen, 10, NewMapHash())
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if dk == nil {
				t.Fatal("Expected doorkeeper instance, got nil")
			}

			err = dk.setBit(test.indices)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			currBitArr := dk.bitArray
			if !slices.Equal(currBitArr, test.expectedResult) {
				t.Fatalf("Expected bit array %b, but got %b", test.expectedResult, currBitArr)
			}
		})

	}
}
