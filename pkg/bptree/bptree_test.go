package bptree

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/oryankibandi/baobab/pkg/buffermanager"
	"github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/logger"
	pgr "github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

type cellEntry struct {
	key []byte
	val []byte
}

func TestFindKeyIndexInternalNode(t *testing.T) {
	// +------+------+------+--------------+
	// |  age   |  name |  country  |      |
	// +--------+-------+-----------+  99  +
	// |  25    |  34   |  89       |      |
	// +--------+-------+-----------+------+

	tests := []struct {
		name        string
		keys        [][]byte
		ptr         []uint32
		expectedIdx int32
		searchKey   []byte
	}{
		{
			name:        "search key in middle of three-key node",
			keys:        [][]byte{[]byte("age"), []byte("country"), []byte("name")},
			ptr:         []uint32{25, 34, 89, 99},
			searchKey:   []byte("country"),
			expectedIdx: 1,
		},
		{
			name:        "search key in single-key node",
			keys:        [][]byte{[]byte("country")},
			ptr:         []uint32{25, 34},
			searchKey:   []byte("country"),
			expectedIdx: 0,
		},
		{
			name:        "search only key in single-key node",
			keys:        [][]byte{[]byte("code")},
			ptr:         []uint32{25, 34},
			searchKey:   []byte("code"),
			expectedIdx: 0,
		},
		{
			name:        "search last key in two-key node",
			keys:        [][]byte{[]byte("age"), []byte("country")},
			ptr:         []uint32{25, 34, 89},
			searchKey:   []byte("country"),
			expectedIdx: 1,
		},
		{
			name:        "search first key in two-key node",
			keys:        [][]byte{[]byte("age"), []byte("country")},
			ptr:         []uint32{25, 34, 89},
			searchKey:   []byte("age"),
			expectedIdx: 0,
		},
		{
			name:        "search second key in two-key node",
			keys:        [][]byte{[]byte("age"), []byte("country")},
			ptr:         []uint32{25, 34, 89},
			searchKey:   []byte("country"),
			expectedIdx: 1,
		},
		{
			name: "search last key in large node",
			keys: [][]byte{
				[]byte("age"),
				[]byte("city"),
				[]byte("code"),
				[]byte("country"),
				[]byte("email"),
				[]byte("gender"),
				[]byte("name"),
				[]byte("phone"),
				[]byte("state"),
			},
			ptr: []uint32{
				25,
				34,
				89,
				99,
				125,
				156,
				178,
				250,
				354,
				456,
			},
			searchKey:   []byte("state"),
			expectedIdx: 8,
		},
		{
			name:        "search last key in three-key node",
			keys:        [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			ptr:         []uint32{25, 88, 99, 105},
			searchKey:   []byte("nationality"),
			expectedIdx: 2,
		},
		{
			name:        "search middle key exact match",
			keys:        [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			ptr:         []uint32{25, 88, 99, 105},
			searchKey:   []byte("country"),
			expectedIdx: 1,
		},
		{
			name:        "search empty node",
			keys:        [][]byte{},
			ptr:         []uint32{},
			searchKey:   []byte("country"),
			expectedIdx: -1,
		},
		{
			name:        "search shared prefix key",
			keys:        [][]byte{[]byte("car"), []byte("cat"), []byte("code")},
			ptr:         []uint32{25, 88, 99, 105},
			searchKey:   []byte("cat"),
			expectedIdx: 1,
		},
		{
			name: "search non-ASCII key",
			keys: [][]byte{
				{0x80},
				{0x80, 0x01},
				{0x81},
				{0xFF},
			},
			ptr: []uint32{
				25,
				88,
				99,
				105,
				250,
			},
			searchKey:   []byte{0x81},
			expectedIdx: 2,
		},
		{
			name: "search missing key between existing keys",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptr: []uint32{
				25,
				34,
				89,
				99,
			},
			searchKey:   []byte("office"),
			expectedIdx: -1,
		},
		{
			name: "search missing key before first key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptr: []uint32{
				25,
				34,
				89,
				99,
			},
			searchKey:   []byte("account"),
			expectedIdx: -1,
		},
		{
			name: "search missing key after last key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptr: []uint32{
				25,
				34,
				89,
				99,
			},
			searchKey:   []byte("zipcode"),
			expectedIdx: -1,
		},
		{
			name:        "search last item",
			keys:        [][]byte{[]byte("age"), []byte("code"), []byte("coutry"), []byte("name")},
			ptr:         []uint32{25, 88, 66, 99, 150},
			searchKey:   []byte("name"),
			expectedIdx: 3,
		},
		{
			name:        "search exact key on single key root node",
			keys:        [][]byte{[]byte("age")},
			ptr:         []uint32{25, 34},
			searchKey:   []byte("age"),
			expectedIdx: 0,
		},
		{
			name:        "search greater key on single key root node",
			keys:        [][]byte{[]byte("age")},
			ptr:         []uint32{25, 34},
			searchKey:   []byte("country"),
			expectedIdx: -1,
		},
		{
			name:        "search lesser key on single key root node",
			keys:        [][]byte{[]byte("code")},
			ptr:         []uint32{25, 34},
			searchKey:   []byte("age"),
			expectedIdx: -1,
		},
	}

	for _, test := range tests {
		internalFrame := createTestInternalNode(35, test.keys, test.ptr)
		if internalFrame == nil {
			t.Fatalf("(%s) No internal frame created", test.name)
		}

		retrievedIdx, err := findKeyIndex(&internalFrame, test.searchKey, 0, uint32(len(test.keys)-1))
		if err != nil {
			t.Fatalf("(%s) Could not find index: %v", test.name, err.Error())
		}

		if int(retrievedIdx) != int(test.expectedIdx) {
			t.Fatalf("(%s) Expected index %d but got %d", test.name, test.expectedIdx, retrievedIdx)
		}

		fmt.Printf("(%s). Done...\n", test.name)
	}
}

func TestFindKeyIndexLeafNode(t *testing.T) {
	// +--------+-----------------+-----------+--------+
	// |  name  |       age       |  country  |  code  |
	// +--------+-----------------+-----------+--------+
	// |  Ben   |  thirty four    |  KENYA    |   KE   |
	// +--------+-----------------+-----------+--------+
	tests := []struct {
		name        string
		keys        [][]byte
		vals        [][]byte
		expectedIdx int32
		searchKey   []byte
	}{
		{
			name: "search missing key in single key node",
			keys: [][]byte{
				[]byte("age"),
			},
			vals: [][]byte{
				[]byte("v1"),
			},
			searchKey:   []byte("account"),
			expectedIdx: -1,
		},
		{
			name:        "search key in middle of three-key node",
			keys:        [][]byte{[]byte("age"), []byte("country"), []byte("name")},
			vals:        [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			searchKey:   []byte("country"),
			expectedIdx: 1,
		},
		{
			name:        "search key in single-key node",
			keys:        [][]byte{[]byte("country")},
			vals:        [][]byte{[]byte("v1")},
			searchKey:   []byte("country"),
			expectedIdx: 0,
		},
		{
			name:        "search only key in single-key node",
			keys:        [][]byte{[]byte("code")},
			vals:        [][]byte{[]byte("v1")},
			searchKey:   []byte("code"),
			expectedIdx: 0,
		},
		{
			name:        "search last key in two-key node",
			keys:        [][]byte{[]byte("age"), []byte("country")},
			vals:        [][]byte{[]byte("v1"), []byte("v2")},
			searchKey:   []byte("country"),
			expectedIdx: 1,
		},
		{
			name:        "search first key in two-key node",
			keys:        [][]byte{[]byte("age"), []byte("country")},
			vals:        [][]byte{[]byte("v1"), []byte("v2")},
			searchKey:   []byte("age"),
			expectedIdx: 0,
		},
		{
			name:        "search second key in two-key node",
			keys:        [][]byte{[]byte("age"), []byte("country")},
			vals:        [][]byte{[]byte("v1"), []byte("v2")},
			searchKey:   []byte("country"),
			expectedIdx: 1,
		},
		{
			name: "search last key in large node",
			keys: [][]byte{
				[]byte("age"),
				[]byte("city"),
				[]byte("code"),
				[]byte("country"),
				[]byte("email"),
				[]byte("gender"),
				[]byte("name"),
				[]byte("phone"),
				[]byte("state"),
			},
			vals: [][]byte{
				[]byte("v1"),
				[]byte("v2"),
				[]byte("v3"),
				[]byte("v4"),
				[]byte("v5"),
				[]byte("v6"),
				[]byte("v7"),
				[]byte("v8"),
				[]byte("v9"),
			},
			searchKey:   []byte("state"),
			expectedIdx: 8,
		},
		{
			name:        "search last key in three-key node",
			keys:        [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			vals:        [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			searchKey:   []byte("nationality"),
			expectedIdx: 2,
		},
		{
			name:        "search middle key exact match",
			keys:        [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			vals:        [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			searchKey:   []byte("country"),
			expectedIdx: 1,
		},
		{
			name:        "search empty node",
			keys:        [][]byte{},
			vals:        [][]byte{},
			searchKey:   []byte("country"),
			expectedIdx: -1,
		},
		{
			name:        "search shared prefix key",
			keys:        [][]byte{[]byte("car"), []byte("cat"), []byte("code")},
			vals:        [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			searchKey:   []byte("cat"),
			expectedIdx: 1,
		},
		{
			name: "search non-ASCII key",
			keys: [][]byte{
				{0x80},
				{0x80, 0x01},
				{0x81},
				{0xFF},
			},
			vals: [][]byte{
				[]byte("v1"),
				[]byte("v2"),
				[]byte("v3"),
				[]byte("v4"),
			},
			searchKey:   []byte{0x81},
			expectedIdx: 2,
		},
		{
			name: "search missing key between existing keys",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			vals: [][]byte{
				[]byte("v1"),
				[]byte("v2"),
				[]byte("v3"),
			},
			searchKey:   []byte("office"),
			expectedIdx: -1,
		},
		{
			name: "search missing key before first key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			vals: [][]byte{
				[]byte("v1"),
				[]byte("v2"),
				[]byte("v3"),
			},
			searchKey:   []byte("account"),
			expectedIdx: -1,
		},
		{
			name: "search missing key after last key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			vals: [][]byte{
				[]byte("v1"),
				[]byte("v2"),
				[]byte("v3"),
			},
			searchKey:   []byte("zipcode"),
			expectedIdx: -1,
		},
	}

	for _, test := range tests {
		leafNode := createTestLeafNode(25, test.keys, test.vals)
		if leafNode == nil {
			t.Fatalf("(%s) No leaf node created", test.name)
		}

		retrievedIdx, err := findKeyIndex(&leafNode, test.searchKey, 0, uint32(len(test.keys)-1))
		if err != nil {
			t.Fatalf("(%s) Could not find index: %v", test.name, err.Error())
		}

		if int(retrievedIdx) != int(test.expectedIdx) {
			t.Fatalf("(%s) Expected index %d but got %d", test.name, test.expectedIdx, retrievedIdx)
		}
	}
}

func TestFindKeyPath(t *testing.T) {
	tests := []struct {
		name             string
		keys             [][]byte
		ptr              []uint32
		expectedPtr      uint32
		expectedPtrIdx   uint32
		expectedSepKey   []byte
		expectedRightKey []byte
		searchKey        []byte
	}{
		{
			name:             "search key in middle of three-key node",
			keys:             [][]byte{[]byte("age"), []byte("country"), []byte("name")},
			ptr:              []uint32{25, 34, 89, 99},
			expectedPtr:      89,
			expectedPtrIdx:   2,
			expectedSepKey:   []byte("country"),
			expectedRightKey: []byte("name"),
			searchKey:        []byte("country"),
		},
		{
			name:             "search key in single-key node",
			keys:             [][]byte{[]byte("country")},
			ptr:              []uint32{25, 34},
			expectedPtr:      34,
			expectedPtrIdx:   1,
			expectedSepKey:   []byte("country"),
			expectedRightKey: nil,
			searchKey:        []byte("country"),
		},
		{
			name:             "search key not in node in a single-key node",
			keys:             [][]byte{[]byte("country")},
			ptr:              []uint32{25, 34},
			expectedPtr:      34,
			expectedPtrIdx:   1,
			expectedSepKey:   []byte("country"),
			expectedRightKey: nil,
			searchKey:        []byte("relationship_status"),
		},
		{
			name:             "search only key in single-key node",
			keys:             [][]byte{[]byte("code")},
			ptr:              []uint32{25, 34},
			expectedPtr:      34,
			expectedPtrIdx:   1,
			expectedSepKey:   []byte("code"),
			expectedRightKey: nil,
			searchKey:        []byte("code"),
		},
		{
			name:             "search last key in two-key node",
			keys:             [][]byte{[]byte("age"), []byte("country")},
			ptr:              []uint32{25, 34, 89},
			expectedPtr:      89,
			expectedPtrIdx:   2,
			expectedSepKey:   []byte("country"),
			expectedRightKey: nil,
			searchKey:        []byte("country"),
		},
		{
			name:             "search least key in two-key node",
			keys:             [][]byte{[]byte("code"), []byte("country")},
			ptr:              []uint32{25, 34, 89},
			expectedPtr:      25,
			expectedPtrIdx:   0,
			expectedSepKey:   []byte("code"),
			expectedRightKey: []byte("code"),
			searchKey:        []byte("age"),
		},
		{
			name:             "search key greater than any key on node",
			keys:             [][]byte{[]byte("code"), []byte("country")},
			ptr:              []uint32{25, 34, 89},
			expectedPtr:      89,
			expectedPtrIdx:   2,
			expectedSepKey:   []byte("country"),
			expectedRightKey: nil,
			searchKey:        []byte("nationality"),
		},
		{
			name:             "search non-existent key in two-key node",
			keys:             [][]byte{[]byte("age"), []byte("country")},
			ptr:              []uint32{25, 34, 89},
			expectedPtr:      34,
			expectedPtrIdx:   1,
			expectedSepKey:   []byte("age"),
			expectedRightKey: []byte("country"),
			searchKey:        []byte("code"),
		},
		{
			name: "search last key in large node",
			keys: [][]byte{
				[]byte("age"),
				[]byte("city"),
				[]byte("code"),
				[]byte("country"),
				[]byte("email"),
				[]byte("gender"),
				[]byte("name"),
				[]byte("phone"),
				[]byte("state"),
			},
			ptr: []uint32{
				25,
				34,
				89,
				99,
				125,
				156,
				178,
				250,
				354,
				456,
			},
			expectedPtr:      456,
			expectedPtrIdx:   9,
			expectedSepKey:   []byte("state"),
			expectedRightKey: nil,
			searchKey:        []byte("state"),
		},
		{
			name:             "search last key in three-key node",
			keys:             [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			ptr:              []uint32{25, 88, 99, 105},
			expectedPtr:      105,
			expectedPtrIdx:   3,
			expectedSepKey:   []byte("nationality"),
			expectedRightKey: nil,
			searchKey:        []byte("nationality"),
		},
		{
			name:             "search middle key exact match",
			keys:             [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			ptr:              []uint32{25, 88, 99, 105},
			expectedPtr:      99,
			expectedPtrIdx:   2,
			expectedSepKey:   []byte("country"),
			expectedRightKey: []byte("nationality"),
			searchKey:        []byte("country"),
		},
		{
			name:             "search empty node",
			keys:             [][]byte{},
			ptr:              []uint32{},
			expectedPtr:      0,
			expectedPtrIdx:   0,
			expectedSepKey:   nil,
			expectedRightKey: nil,
			searchKey:        []byte("country"),
		},
		{
			name:             "search shared prefix key",
			keys:             [][]byte{[]byte("car"), []byte("cat"), []byte("code")},
			ptr:              []uint32{25, 88, 99, 105},
			expectedPtr:      99,
			expectedPtrIdx:   2,
			expectedSepKey:   []byte("cat"),
			expectedRightKey: []byte("code"),
			searchKey:        []byte("cat"),
		},
		{
			name: "search non-ASCII key",
			keys: [][]byte{
				{0x80},
				{0x80, 0x01},
				{0x81},
				{0xFF},
			},
			ptr: []uint32{
				25,
				88,
				99,
				105,
				250,
			},
			expectedPtr:      105,
			expectedPtrIdx:   3,
			expectedSepKey:   []byte{0x81},
			expectedRightKey: []byte{0xFF},
			searchKey:        []byte{0x82},
		},
		{
			name: "search missing key between existing keys",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptr: []uint32{
				25,
				34,
				89,
				99,
			},
			expectedPtr:      99,
			expectedPtrIdx:   3,
			expectedSepKey:   []byte("name"),
			expectedRightKey: nil,
			searchKey:        []byte("office"),
		},
		{
			name: "search missing key before first key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptr: []uint32{
				25,
				34,
				89,
				99,
			},
			expectedPtr:      25,
			expectedPtrIdx:   0,
			expectedSepKey:   []byte("age"),
			expectedRightKey: []byte("age"),
			searchKey:        []byte("account"),
		},
		{
			name: "search missing key after last key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptr: []uint32{
				25,
				34,
				89,
				99,
			},
			expectedPtr:      99,
			expectedPtrIdx:   3,
			expectedSepKey:   []byte("name"),
			expectedRightKey: nil,
			searchKey:        []byte("zipcode"),
		},
		{
			name:             "search last item",
			keys:             [][]byte{[]byte("age"), []byte("code"), []byte("coutry"), []byte("name")},
			ptr:              []uint32{25, 88, 66, 99, 150},
			expectedPtr:      150,
			expectedPtrIdx:   4,
			expectedSepKey:   []byte("name"),
			expectedRightKey: nil,
			searchKey:        []byte("name"),
		},
		{
			name:             "search exact key on single key root node",
			keys:             [][]byte{[]byte("age")},
			ptr:              []uint32{25, 34},
			expectedPtr:      34,
			expectedPtrIdx:   1,
			expectedSepKey:   []byte("age"),
			expectedRightKey: nil,
			searchKey:        []byte("age"),
		},
		{
			name:             "search greater key on single key root node",
			keys:             [][]byte{[]byte("age")},
			ptr:              []uint32{25, 34},
			expectedPtr:      34,
			expectedPtrIdx:   1,
			expectedSepKey:   []byte("age"),
			expectedRightKey: nil,
			searchKey:        []byte("country"),
		},
		{
			name:             "search lesser key on single key root node",
			keys:             [][]byte{[]byte("code")},
			ptr:              []uint32{25, 34},
			expectedPtr:      25,
			expectedPtrIdx:   0,
			expectedSepKey:   []byte("code"),
			expectedRightKey: nil,
			searchKey:        []byte("age"),
		},
		{
			name: "search middle key among long keys",
			keys: [][]byte{
				[]byte("user:435b156ac7aeebb88cd7514604d0df26:profile"),
				[]byte("user:7864e27d26f6b1650ae2e9f398525383:profile"),
				[]byte("user:9fc7949ab99f05494d8e1304f788003c:profile"),
				[]byte("user:db85d83f5692eecc8f6ef0f22c0d8ed7:profile"),
			},
			ptr:              []uint32{1, 2, 4, 5, 6},
			expectedPtr:      4,
			expectedPtrIdx:   2,
			expectedSepKey:   []byte("user:7864e27d26f6b1650ae2e9f398525383:profile"),
			expectedRightKey: []byte("user:9fc7949ab99f05494d8e1304f788003c:profile"),
			searchKey:        []byte("user:8be334382e04bc1182d5ac23f698f6df:profile"),
		},
		{
			name: "search 3rd key among long keys",
			keys: [][]byte{
				[]byte("user:435b156ac7aeebb88cd7514604d0df26:profile"),
				[]byte("user:7864e27d26f6b1650ae2e9f398525383:profile"),
				[]byte("user:9fc7949ab99f05494d8e1304f788003c:profile"),
				[]byte("user:db85d83f5692eecc8f6ef0f22c0d8ed7:profile"),
			},
			ptr:              []uint32{1, 2, 4, 5, 6},
			expectedPtr:      5,
			expectedPtrIdx:   3,
			expectedSepKey:   []byte("user:9fc7949ab99f05494d8e1304f788003c:profile"),
			expectedRightKey: []byte("user:db85d83f5692eecc8f6ef0f22c0d8ed7:profile"),
			searchKey:        []byte("user:9fg7949ab99f05494d8e1304f788003c:profile"),
		},
	}

	var sepKey, rightSepKey []byte
	var retrievedChildPtr, childPtrIdx uint32
	for i, test := range tests {
		fmt.Printf("starting... -> %s\n", test.name)
		internalFrame := createTestInternalNode(35, test.keys, test.ptr)
		if internalFrame == nil {
			t.Fatalf("(%s) No internal frame created", test.name)
		}
		fmt.Println("====================================================")

		sepKeys, childPtrArr, err := findKeyPath(&internalFrame, test.searchKey)
		if err != nil {
			t.Fatalf("(%s) Could not find index: %v", test.name, err.Error())
		}

		if l := len(sepKeys); l == 2 {
			rightSepKey = sepKeys[1]
			sepKey = sepKeys[0]
		} else if l == 1 {
			sepKey = sepKeys[0]
		}

		if len(test.keys) > 0 && childPtrArr == nil {
			t.Fatalf("No childPtrArr returned")
		}

		if l := len(childPtrArr); l != 8 && len(test.keys) > 0 {
			t.Fatalf("Exected length of childPtrArr to be %d,  got %d", 8, l)
		}

		if len(test.keys) > 0 {
			retrievedChildPtr = binary.LittleEndian.Uint32(childPtrArr[4:])
			childPtrIdx = binary.LittleEndian.Uint32(childPtrArr[:4])
		}

		if int(retrievedChildPtr) != int(test.expectedPtr) {
			t.Fatalf("(%s) Expected child ptr %d but got %d", test.name, test.expectedPtr, retrievedChildPtr)
		}

		if childPtrIdx != test.expectedPtrIdx {
			t.Fatalf("(%s) Expected child pointer index to be %d but got %d", test.name, test.expectedPtrIdx, childPtrIdx)
		}

		if sepKey == nil && test.expectedSepKey != nil {
			t.Fatalf("(%s) Expected seperator key to be provided", test.name)
		}

		if test.expectedRightKey != nil && rightSepKey == nil {
			t.Fatalf("(%s) Expected right seperator key to be provided", test.name)
		}

		fmt.Printf("%d. (%s) Received SepKeys --> %v\n", i, test.name, sepKeys)
		if !bytes.Equal(sepKey, test.expectedSepKey) {
			t.Fatalf("(%s) Expected separatorKey to be %v but got %v", test.name, test.expectedSepKey, sepKey)
		}

		if test.expectedRightKey != nil && !bytes.Equal(rightSepKey, test.expectedRightKey) {
			t.Fatalf("(%s) Expected rightKey to be \"%s\" but got \"%s\"", test.name, test.expectedRightKey, rightSepKey)
		}

		sepKey = nil
		rightSepKey = nil
		childPtrArr = nil
		retrievedChildPtr = 0
		childPtrIdx = 0

		fmt.Printf("(%s). Done...\n", test.name)
		fmt.Println("====================================================")
	}
}

func TestReplaceSepKey(t *testing.T) {
	// +------+------------+-----------+------+
	// |  age   |  country |    name   |      |
	// +--------+----------+-----------+  99  +
	// |  25    |     34   |     89    |      |
	// +--------+----------+-----------+------+

	tests := []struct {
		name             string
		keys             [][]byte
		ptrs             []uint32
		vals             [][]byte
		isLeaf           bool
		oldKey           []byte
		newKey           []byte
		expectError      bool
		expectInvalidIdx bool
	}{
		// ------------------------------------------------------------
		// Basic case
		// ------------------------------------------------------------
		{
			name: "replace middle key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("country"),
			newKey:           []byte("location"),
			expectError:      false,
			expectInvalidIdx: false,
		},

		// ------------------------------------------------------------
		// Position of separator key
		// ------------------------------------------------------------
		{
			name: "replace first separator key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("age"),
			newKey:           []byte("birth_year"),
			expectError:      false,
			expectInvalidIdx: false,
		},
		{
			name: "replace last separator key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("name"),
			newKey:           []byte("username"),
			expectError:      false,
			expectInvalidIdx: false,
		},
		{
			name: "replace only separator key",
			keys: [][]byte{
				[]byte("country"),
			},
			ptrs:             []uint32{25, 34},
			isLeaf:           false,
			oldKey:           []byte("country"),
			newKey:           []byte("location"),
			expectError:      false,
			expectInvalidIdx: false,
		},

		// ------------------------------------------------------------
		// Missing keys
		// ------------------------------------------------------------
		{
			name: "replace missing key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("email"),
			newKey:           []byte("phone"),
			expectError:      false,
			expectInvalidIdx: true,
		},
		{
			name: "replace missing key before first separator",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("account"),
			newKey:           []byte("address"),
			expectError:      false,
			expectInvalidIdx: true,
		},
		{
			name: "replace missing key after last separator",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("zipcode"),
			newKey:           []byte("state"),
			expectError:      false,
			expectInvalidIdx: true,
		},

		// ------------------------------------------------------------
		// Key length
		// ------------------------------------------------------------
		{
			name: "replace separator with shorter key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("country"),
			newKey:           []byte("ctry"),
			expectError:      false,
			expectInvalidIdx: false,
		},
		{
			name: "replace separator with longer key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("country"),
			newKey:           []byte("nationality"),
			expectError:      false,
			expectInvalidIdx: false,
		},
		{
			name: "replace separator with empty key",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
				[]byte("name"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("country"),
			newKey:           []byte{},
			expectError:      true,
			expectInvalidIdx: true,
		},

		// ------------------------------------------------------------
		// Exact byte semantics
		// ------------------------------------------------------------
		{
			name: "replace one-byte separator",
			keys: [][]byte{
				[]byte("a"),
				[]byte("b"),
				[]byte("c"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("b"),
			newKey:           []byte("x"),
			expectError:      false,
			expectInvalidIdx: false,
		},
		{
			name: "replace separator with shared prefix",
			keys: [][]byte{
				[]byte("car"),
				[]byte("cat"),
				[]byte("code"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("cat"),
			newKey:           []byte("can"),
			expectError:      false,
			expectInvalidIdx: false,
		},
		{
			name: "replace separator with exact prefix",
			keys: [][]byte{
				[]byte("car"),
				[]byte("cart"),
				[]byte("cat"),
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte("cart"),
			newKey:           []byte("car"),
			expectError:      false,
			expectInvalidIdx: false,
		},

		// ------------------------------------------------------------
		// Empty node
		// ------------------------------------------------------------
		{
			name:             "replace key in empty internal node",
			keys:             [][]byte{},
			ptrs:             []uint32{},
			isLeaf:           false,
			oldKey:           []byte("age"),
			newKey:           []byte("birth_year"),
			expectError:      false,
			expectInvalidIdx: true,
		},

		// ------------------------------------------------------------
		// Single-key internal node
		// ------------------------------------------------------------
		{
			name: "replace key in single-key internal node",
			keys: [][]byte{
				[]byte("country"),
			},
			ptrs:             []uint32{25, 34},
			isLeaf:           false,
			oldKey:           []byte("country"),
			newKey:           []byte("location"),
			expectError:      false,
			expectInvalidIdx: false,
		},
		{
			name: "missing key in single-key internal node",
			keys: [][]byte{
				[]byte("country"),
			},
			ptrs:             []uint32{25, 34},
			isLeaf:           false,
			oldKey:           []byte("name"),
			newKey:           []byte("username"),
			expectError:      false,
			expectInvalidIdx: true,
		},

		// ------------------------------------------------------------
		// Non-ASCII / arbitrary byte keys
		// ------------------------------------------------------------
		{
			name: "replace non-ASCII separator",
			keys: [][]byte{
				{0x80},
				{0x81},
				{0xFF},
			},
			ptrs:             []uint32{25, 34, 89, 99},
			isLeaf:           false,
			oldKey:           []byte{0x81},
			newKey:           []byte{0x82},
			expectError:      false,
			expectInvalidIdx: false,
		},

		// ------------------------------------------------------------
		// Leaf node rejection
		// ------------------------------------------------------------
		{
			name: "return error when replacing key in leaf node",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
			},
			vals: [][]byte{
				[]byte("25"),
				[]byte("United States"),
			},
			ptrs:             []uint32{25, 34, 89},
			isLeaf:           true,
			oldKey:           []byte("country"),
			newKey:           []byte("location"),
			expectError:      true,
			expectInvalidIdx: false,
		},
	}

	var node []byte
	var cellIdx int32
	var originalCellOffset uint32
	var err error
	for i, test := range tests {
		if test.isLeaf {
			node = createTestLeafNode(25, test.keys, test.vals)
		} else {
			node = createTestInternalNode(25, test.keys, test.ptrs)
		}

		fmt.Printf("%d. printing node before replacing=========================\n", i)
		fmt.Println(printNodeContent(&node))
		fmt.Println("======================================================")
		originalItemCount := binary.LittleEndian.Uint32(node[17:21])
		if len(test.newKey) > len(test.oldKey) && !test.expectInvalidIdx {
			cellIdx, err = findKeyIndex(&node, test.oldKey, 0, originalItemCount-1)
			if err != nil {
				t.Fatalf("(%s) %s", test.name, err.Error())
			}
			originalCellOffset = binary.LittleEndian.Uint32(node[pgr.HEADER_SIZE_BYTES+(cellIdx*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(cellIdx*pgr.CELL_POINTER_SIZE_BYTE)+5])
		}

		idx, err := replaceSepKey(&node, test.oldKey, test.newKey)
		if test.expectError {
			if err == nil {
				t.Fatalf("(%s) Expected error, got nil", test.name)
			} else {
				continue
			}
		}

		if test.expectInvalidIdx {
			if idx != -1 {
				t.Fatalf("(%s) Expected idx to be -1, got %d", test.name, idx)
			} else {
				continue
			}
		}

		if err != nil {
			t.Fatalf("Expected no error but got: %s", err.Error())
		}

		if idx < 0 {
			t.Fatalf("Expected valid index, got -1")
		}

		fmt.Printf("(%d) printing node after replacing=========================\n", i)
		fmt.Println(printNodeContent(&node))
		fmt.Println("======================================================")

		finalItemCount := binary.LittleEndian.Uint32(node[17:21])
		if originalItemCount != finalItemCount {
			t.Fatalf("(%s) Expected itemcount to remain %d but got %d", test.name, originalItemCount, finalItemCount)
		}

		// check cell offset. if newKey > oldKey ensure new cell was created
		cOff := binary.LittleEndian.Uint32(node[pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+5])
		if len(test.newKey) > len(test.oldKey) && !test.expectInvalidIdx {
			if cOff == originalCellOffset {
				t.Fatalf("(%s) Expected cell offset to be updated.", test.name)
			}
		}

		// ensure key was updated
		kLen := binary.LittleEndian.Uint32(node[cOff+1 : cOff+5])
		if k := node[cOff+13 : cOff+13+kLen]; !bytes.Equal(test.newKey, k) {
			t.Fatalf("(%s) Expected key to be updated to \"%s\", but got \"%s\"", test.name, test.newKey, k)
		}

		// check key length
		if int(kLen) != len(test.newKey) {
			t.Fatalf("(%s) Expected key length to be %d, got %d", test.name, len(test.newKey), kLen)
		}
	}
}

func TestFindInsertionIdxLeafNode(t *testing.T) {
	// +--------+-----------------+-----------+--------+
	// |  name  |       age       |  country  |  code  |
	// +--------+-----------------+-----------+--------+
	// |  Ben   |  thirty four    |  KENYA    |   KE   |
	// +--------+-----------------+-----------+--------+

	tests := []struct {
		keys         [][]byte
		vals         [][]byte
		expectedIdx  uint32
		insertionKey []byte
		exactMatch   bool
	}{
		{
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("name")},
			vals:         [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			insertionKey: []byte("code"),
			expectedIdx:  1,
			exactMatch:   false,
		},
		// node with only one item
		{
			keys:         [][]byte{[]byte("country")},
			vals:         [][]byte{[]byte("v1")},
			insertionKey: []byte("age"),
			expectedIdx:  0,
			exactMatch:   false,
		},
		{
			keys:         [][]byte{[]byte("code")},
			vals:         [][]byte{[]byte("v1")},
			insertionKey: []byte("continent"),
			expectedIdx:  1,
			exactMatch:   false,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			vals:         [][]byte{[]byte("v1"), []byte("v2")},
			insertionKey: []byte("code"),
			expectedIdx:  1,
			exactMatch:   false,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			vals:         [][]byte{[]byte("v1"), []byte("v2")},
			insertionKey: []byte("cousin"),
			expectedIdx:  2,
			exactMatch:   false,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			vals:         [][]byte{[]byte("v1"), []byte("v2")},
			insertionKey: []byte("account"),
			expectedIdx:  0,
			exactMatch:   false,
		},
		{
			keys: [][]byte{
				[]byte("age"),
				[]byte("city"),
				[]byte("code"),
				[]byte("country"),
				[]byte("email"),
				[]byte("gender"),
				[]byte("name"),
				[]byte("phone"),
				[]byte("state"),
			},
			vals: [][]byte{
				[]byte("v1"),
				[]byte("v2"),
				[]byte("v3"),
				[]byte("v4"),
				[]byte("v5"),
				[]byte("v6"),
				[]byte("v7"),
				[]byte("v8"),
				[]byte("v9"),
			},
			insertionKey: []byte("zipcode"),
			expectedIdx:  9,
			exactMatch:   false,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			vals:         [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			insertionKey: []byte("office"),
			expectedIdx:  3,
			exactMatch:   false,
		},
		{ // exact match
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			vals:         [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			insertionKey: []byte("country"),
			expectedIdx:  1,
			exactMatch:   true,
		},
		{ // empty node
			keys:         [][]byte{},
			vals:         [][]byte{},
			insertionKey: []byte("country"),
			expectedIdx:  0,
			exactMatch:   false,
		},
		{ // shared prefixes
			keys:         [][]byte{[]byte("car"), []byte("cat"), []byte("code")},
			vals:         [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			insertionKey: []byte("can"),
			expectedIdx:  0,
			exactMatch:   false,
		},
		{ // non-ASCII
			keys: [][]byte{
				{0x80},
				{0x80, 0x01},
				{0x81},
				{0xFF},
			},
			vals: [][]byte{
				[]byte("v1"),
				[]byte("v2"),
				[]byte("v3"),
				[]byte("v4"),
			},
			insertionKey: []byte{0x80, 0x02},
			expectedIdx:  2,
			exactMatch:   false,
		},
		{
			keys: [][]byte{
				[]byte("user:b39924f92d739a0bff85da7de6c9245c:profile"),
				[]byte("user:c31b36a64d710f171273b0c4eebf7e66:profile"),
				[]byte("user:dd9a2c24b87be3ffed948eed84dd1a51:profile"),
			},
			vals: [][]byte{
				[]byte("{'id':'b39924f92d739a0bff85da7de6c9245c','name':'User 0','email':'user0@example.com','active':true}"),
				[]byte("{'id':'c31b36a64d710f171273b0c4eebf7e66','name':'User 2','email':'user2@example.com','active':true}"),
				[]byte("{'id':'c31b36a64d710f171273b0c4eebf7e66','name':'User 2','email':'user2@example.com','active':true}"),
			},
			insertionKey: []byte("user:c92b36a2b6b5d834d1b8d59da330fcff:profile"),
			expectedIdx:  2,
			exactMatch:   false,
		},
	}

	for i, test := range tests {
		leafNode := createTestLeafNode(25, test.keys, test.vals)
		if leafNode == nil {
			t.Fatalf("%d. No leaf node created", i)
		}

		idxArr, err := findInsertionIdx(&leafNode, test.insertionKey, 0, uint32(len(test.keys)-1))
		if err != nil {
			t.Fatalf("%d. Expected no error, got %s", i, err.Error())
		}

		if idxArr == nil {
			t.Fatalf("%d. Received nil idxArr", i)
		}

		exactMatch := helpers.BitIsSet(&idxArr[0], exactMatchBitPos)
		if exactMatch != test.exactMatch {
			t.Fatalf("%d. Expected exactMatch:  %t, but got %t", i, test.exactMatch, exactMatch)
		}

		idx := binary.LittleEndian.Uint32(idxArr[1:])

		if idx != test.expectedIdx {
			t.Fatalf("%d. Expected insertion idx %d, got %d", i, test.expectedIdx, idx)
		}

		t.Logf("%d. Done.\n", i)
	}
}

func TestFindInsertionIdxInternalNode(t *testing.T) {
	// +------+------+------+--------------+
	// |  age   |  name |  country  |      |
	// +--------+-------+-----------+  99  +
	// |  25    |  34   |  89       |      |
	// +--------+-------+-----------+------+
	tests := []struct {
		keys         [][]byte
		ptr          []uint32
		expectedIdx  uint32
		insertionKey []byte
		exactMatch   bool
	}{
		{
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("name")},
			ptr:          []uint32{25, 34, 89, 99},
			insertionKey: []byte("code"),
			expectedIdx:  1,
			exactMatch:   false,
		},
		// node with only one item
		{
			keys:         [][]byte{[]byte("country")},
			ptr:          []uint32{25, 34},
			insertionKey: []byte("age"),
			expectedIdx:  0,
			exactMatch:   false,
		},
		{
			keys:         [][]byte{[]byte("code")},
			ptr:          []uint32{25, 34},
			insertionKey: []byte("continent"),
			expectedIdx:  1,
			exactMatch:   false,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			ptr:          []uint32{25, 34, 89},
			insertionKey: []byte("code"),
			expectedIdx:  1,
			exactMatch:   false,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			ptr:          []uint32{25, 34, 89},
			insertionKey: []byte("cousin"),
			expectedIdx:  2,
			exactMatch:   false,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			ptr:          []uint32{25, 34, 89},
			insertionKey: []byte("account"),
			expectedIdx:  0,
			exactMatch:   false,
		},
		{
			keys: [][]byte{
				[]byte("age"),
				[]byte("city"),
				[]byte("code"),
				[]byte("country"),
				[]byte("email"),
				[]byte("gender"),
				[]byte("name"),
				[]byte("phone"),
				[]byte("state"),
			},
			ptr:          []uint32{25, 34, 89, 99, 125, 156, 178, 250, 354, 456},
			insertionKey: []byte("zipcode"),
			expectedIdx:  9,
			exactMatch:   false,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			ptr:          []uint32{25, 88, 99, 0},
			insertionKey: []byte("office"),
			expectedIdx:  3,
			exactMatch:   false,
		},
		{ // exact match
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			ptr:          []uint32{25, 88, 99, 0},
			insertionKey: []byte("country"),
			expectedIdx:  1,
			exactMatch:   true,
		},
		{ // empty node
			keys:         [][]byte{},
			ptr:          []uint32{},
			insertionKey: []byte("country"),
			expectedIdx:  0,
			exactMatch:   false,
		},
		{ // shared prefixes
			keys:         [][]byte{[]byte("car"), []byte("cat"), []byte("code")},
			ptr:          []uint32{25, 88, 99, 0},
			insertionKey: []byte("can"),
			expectedIdx:  0,
			exactMatch:   false,
		},
		{ // non-ASCII
			keys: [][]byte{
				{0x80},
				{0x80, 0x01},
				{0x81},
				{0xFF},
			},
			ptr:          []uint32{25, 88, 99, 105, 250},
			insertionKey: []byte{0x80, 0x02},
			expectedIdx:  2,
			exactMatch:   false,
		},
		{ // insertion key at last index
			keys: [][]byte{
				[]byte("user:180952aef5a9a7ca761063b62b9c5a46:profile"),
				[]byte("user:305c58fbac58336a7e970d2ab709aee8:profile"),
			},
			ptr:          []uint32{1, 7, 4},
			insertionKey: []byte("user:3d7c2d049b5e1ccb1ec7ec0437700375:profile"),
			expectedIdx:  2,
			exactMatch:   false,
		},
		{ // insertion key at last index
			keys: [][]byte{
				[]byte("user:13ec7e79e745d5647ef39bd5a78a6336:profile"),
				[]byte("user:495be42eb757dff30fe85a9944da784a:profile"),
			},
			ptr:          []uint32{1, 4, 2},
			insertionKey: []byte("user:85947b0a02cdfed8fed4ffaef7809ee6:profile"),
			expectedIdx:  2,
			exactMatch:   false,
		},
	}

	for i, test := range tests {
		internalFrame := createTestInternalNode(35, test.keys, test.ptr)
		if internalFrame == nil {
			t.Fatalf("%d. No internal frame created", i)
		}

		idxArr, err := findInsertionIdx(&internalFrame, test.insertionKey, 0, uint32(len(test.keys)-1))
		if err != nil {
			t.Fatalf("%d. Expected no error, got %s", i, err.Error())
		}

		if idxArr == nil {
			t.Fatalf("%d. Got nil idx array.", i)
		}

		exactMatch := helpers.BitIsSet(&idxArr[0], exactMatchBitPos)
		if exactMatch != test.exactMatch {
			t.Fatalf("%d. Expected exactMatch: %t, received %t", i, test.exactMatch, exactMatch)
		}

		idx := binary.LittleEndian.Uint32(idxArr[1:])
		fmt.Printf("Received Insertion IDX --> %d\n", idx)

		if idx != test.expectedIdx {
			t.Fatalf("Expected insertion idx %d, got %d", test.expectedIdx, idx)
		}
		fmt.Printf("%d. Done...\n", i)
	}
}

func TestInsertLeafNode(t *testing.T) {
	tests := []struct {
		name                 string
		keys                 [][]byte
		vals                 [][]byte
		insertKey            []byte
		insertVal            []byte
		expectedInsertionIdx uint32
		isDuplicate          bool
	}{
		{
			name:                 "insert to middle of node.",
			keys:                 [][]byte{[]byte("age"), []byte("country")},
			vals:                 [][]byte{[]byte("thirty"), []byte("U.S.A")},
			insertKey:            []byte("code"),
			insertVal:            []byte("javascript"),
			expectedInsertionIdx: 1,
		},
		{
			name:                 "insert to beginning of node.",
			keys:                 [][]byte{[]byte("code"), []byte("country")},
			vals:                 [][]byte{[]byte("javascript"), []byte("U.S.A")},
			insertKey:            []byte("age"),
			insertVal:            []byte("thirty"),
			expectedInsertionIdx: 0,
		},
		{
			name:                 "insert to end of node.",
			keys:                 [][]byte{[]byte("code"), []byte("country")},
			vals:                 [][]byte{[]byte("java"), []byte("U.S.A")},
			insertKey:            []byte("nationality"),
			insertVal:            []byte("american"),
			expectedInsertionIdx: 2,
		},
		{
			name:                 "insert duplicate key.",
			keys:                 [][]byte{[]byte("age"), []byte("country")},
			vals:                 [][]byte{[]byte("thirty"), []byte("U.S.A")},
			insertKey:            []byte("country"),
			insertVal:            []byte("Canada"),
			expectedInsertionIdx: 1,
			isDuplicate:          true,
		},
		{
			name: "insert middle node edge case",
			keys: [][]byte{
				[]byte("user:017e06fe6b9cc2d148d39b0a16143dbd:profileage"),
				[]byte("user:018a6cf8c0c5034f19720a3abe03dea4:profile"),
				[]byte("user:01ac09b5f7eb1e04fb099ff7b449796c:profile"),
			},
			vals: [][]byte{
				[]byte(`{"id":"017e06fe6b9cc2d148d39b0a16143dbd","name":"User 1725","email":"user1725@example.com","active":true}`),
				[]byte(`{"id":"018a6cf8c0c5034f19720a3abe03dea4","name":"User 411","email":"user411@example.com","active":true}`),
				[]byte(`{"id":"01ac09b5f7eb1e04fb099ff7b449796c","name":"User 3987","email":"user3987@example.com","active":true}`),
			},
			insertKey:            []byte("user:01a9eadffe16c9fcb7aa2ea5228d8ee2:profile"),
			insertVal:            []byte(`{"id":"01a9eadffe16c9fcb7aa2ea5228d8ee2","name":"User 4035","email":"user4035@example.com","active":true}`),
			expectedInsertionIdx: 2,
			isDuplicate:          false,
		},
	}

	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	var expectedLowOff uint32
	var expectedUpperOff uint32
	for _, test := range tests {
		// leaf node
		node := createTestLeafNode(25, test.keys, test.vals)

		fmt.Println("NODE BEFORE INSERT...")
		fmt.Println(printNodeContent(&node))
		currUpperOffset := binary.LittleEndian.Uint32(node[25:29])
		currLowerOffset := binary.LittleEndian.Uint32(node[29:33])
		expectedUpperOff = currUpperOffset - uint32(13+len(test.insertKey)+len(test.insertVal))
		if test.isDuplicate {
			expectedLowOff = currLowerOffset
		} else {
			expectedLowOff = currLowerOffset + pgr.CELL_POINTER_SIZE_BYTE
		}

		err = bp.insertToFrame(&node, test.insertKey, 0, test.insertVal)
		if err != nil {
			t.Fatalf("(%s) Expected no error, got %s", test.name, err.Error())
		}
		fmt.Println("NODE AFTER INSERT...")
		fmt.Println(printNodeContent(&node))

		var expectedItemCount uint32
		if test.isDuplicate {
			expectedItemCount = uint32(len(test.keys))
		} else {
			expectedItemCount = uint32(len(test.keys)) + 1
		}

		idx, err := findKeyIndex(&node, test.insertKey, 0, expectedItemCount-1)
		if err != nil {
			t.Fatalf("(%s) Expected no error, got %s", test.name, err.Error())
		}

		if idx != int32(test.expectedInsertionIdx) {
			t.Fatalf("(%s) Expected key to be inserted at idx %d, got %d", test.name, test.expectedInsertionIdx, idx)
		}

		// check that node is marked dirty
		if !helpers.BitIsSet(&node[0], pgr.Dirty) {
			t.Fatalf("Expected node to be marked dirty")
		}

		// check upper and lower offset
		if l := binary.LittleEndian.Uint32(node[29:33]); l != expectedLowOff {
			t.Fatalf("(%s) Expected updated lower offset to be %d but got %d", test.name, expectedLowOff, l)
		}

		if u := binary.LittleEndian.Uint32(node[25:29]); u != expectedUpperOff {
			t.Fatalf("(%s) Expected updated upper offset to be %d but got %d", test.name, expectedUpperOff, u)
		}

		// check value
		cOff := binary.LittleEndian.Uint32(node[pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+5])
		kLen := binary.LittleEndian.Uint32(node[cOff+1 : cOff+5])
		vLen := binary.LittleEndian.Uint32(node[cOff+5 : cOff+9])
		val := node[cOff+13+kLen : cOff+13+kLen+vLen]

		if !bytes.Equal(val, test.insertVal) {
			t.Fatalf("(%s) Expected new value to be '%s' but got '%s'", test.name, test.insertVal, val)
		}
	}
}

func TestInsertFullLeafNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	// leaf node
	keys := [][]byte{
		[]byte("user:1617d0e6434529cb3cf14b8b3731c069:profile"),
		[]byte("user:6cd13ce83743c1f9199c14bf319d7002:profile"),
		[]byte("user:ac16bba68f0aa11e8c6430abe706c81c:profile"),
		[]byte("user:b328596859c34354066dcf5e4908058f:profile"),
		[]byte("user:c8f6859380e9e97391eaa39ab98f1bb3:profile")}
	vals := [][]byte{
		[]byte(`{"id":"1617d0e6434529cb3cf14b8b3731c069","name":"User 0","email":"user0@example.com","active":true}`),
		[]byte(`{"id":"6cd13ce83743c1f9199c14bf319d7002","name":"User 5","email":"user5@example.com","active":true}`),
		[]byte(`{"id":"ac16bba68f0aa11e8c6430abe706c81c","name":"User 4","email":"user4@example.com","active":true}`),
		[]byte(`{"id":"b328596859c34354066dcf5e4908058f","name":"User 6","email":"user6@example.com","active":true}`),
		[]byte(`{"id":"c8f6859380e9e97391eaa39ab98f1bb3","name":"User 7","email":"user7@example.com","active":true}`)}
	node := createTestLeafNode(25, nil, nil)

	// insertKey := []byte("user:c8f6859380e9e97391eaa39ab98f1bb3:profile")
	// insertVal := []byte("{\"id\":\"c8f6859380e9e97391eaa39ab98f1bb3\",\"name\":\"User 7\",\"email\":\"user7@example.com\",\"active\":true}")

	for i, k := range keys {
		expectedInsertionIdx := i
		err = bp.insertToFrame(&node, k, 0, vals[i])
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}

		idx, err := findKeyIndex(&node, k, 0, uint32(i))
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}

		if idx != int32(expectedInsertionIdx) {
			t.Fatalf("Expected key to be inserted at idx %d, got %d", expectedInsertionIdx, idx)
		}

		// check that node is marked dirty
		if !helpers.BitIsSet(&node[0], pgr.Dirty) {
			t.Fatalf("Expected node to be marked dirty")
		}
	}

}

func TestInsertInternalNode(t *testing.T) {
	tests := []struct {
		name                 string
		keys                 [][]byte
		ptrs                 []uint32
		insertKey            []byte
		insertPtr            uint32
		expectedInsertionIdx uint32
		isDuplicate          bool
	}{
		{
			name:                 "insert to middle of node.",
			keys:                 [][]byte{[]byte("age"), []byte("country")},
			ptrs:                 []uint32{25, 88, 99},
			insertKey:            []byte("code"),
			insertPtr:            66,
			expectedInsertionIdx: 1,
		},
		{
			name:                 "insert to beginning of node.",
			keys:                 [][]byte{[]byte("code"), []byte("country")},
			ptrs:                 []uint32{25, 88, 99},
			insertKey:            []byte("age"),
			insertPtr:            66,
			expectedInsertionIdx: 0,
		},
		{
			name:                 "insert to end of node.",
			keys:                 [][]byte{[]byte("code"), []byte("country")},
			ptrs:                 []uint32{25, 88, 99},
			insertKey:            []byte("nationality"),
			insertPtr:            66,
			expectedInsertionIdx: 2,
		},
		{
			name:                 "insert duplicate key.",
			keys:                 [][]byte{[]byte("age"), []byte("country")},
			ptrs:                 []uint32{25, 88, 99},
			insertKey:            []byte("country"),
			insertPtr:            66,
			expectedInsertionIdx: 1,
			isDuplicate:          true,
		},
		{
			name: "binary key is prefix of existing key.",
			keys: [][]byte{
				{0x01},
				{0x01, 0x00},
			},
			ptrs:                 []uint32{25, 88, 99},
			insertKey:            []byte{0x00},
			insertPtr:            66,
			expectedInsertionIdx: 0,
			isDuplicate:          false,
		},
		{
			name: "insert binary key.",
			keys: [][]byte{
				{0x01, 0x10},
				{0x02, 0x10},
			},
			ptrs:                 []uint32{25, 88, 99},
			insertKey:            []byte{0x01, 0xff},
			insertPtr:            66,
			expectedInsertionIdx: 1,
			isDuplicate:          false,
		},
		{
			name: "insert very long key.",
			keys: [][]byte{
				[]byte("age"),
				[]byte("country"),
			},
			ptrs:                 []uint32{25, 88, 99},
			insertKey:            bytes.Repeat([]byte("x"), 4096),
			insertPtr:            66,
			expectedInsertionIdx: 2,
			isDuplicate:          false,
		},
		{
			name:                 "insert key that is prefix of existing key.",
			keys:                 [][]byte{[]byte("age"), []byte("country")},
			ptrs:                 []uint32{25, 88, 99},
			insertKey:            []byte("coun"),
			insertPtr:            66,
			expectedInsertionIdx: 1,
			isDuplicate:          false,
		},
		{
			name:                 "insert key with existing key as prefix.",
			keys:                 [][]byte{[]byte("age"), []byte("coun")},
			ptrs:                 []uint32{25, 88, 99},
			insertKey:            []byte("country"),
			insertPtr:            66,
			expectedInsertionIdx: 2,
			isDuplicate:          false,
		},
	}

	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	for _, test := range tests {
		fmt.Println("=========================================")
		fmt.Printf("Running test: %s\n", test.name)
		fmt.Println("=========================================")
		// internal node
		node := createTestInternalNode(35, test.keys, test.ptrs)

		err = bp.insertToFrame(&node, test.insertKey, test.insertPtr, nil)
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}

		fmt.Println("NODE JUST AFTER INSERT")
		fmt.Println(printNodeContent(&node))
		var expectedItemCount uint32
		if test.isDuplicate {
			expectedItemCount = uint32(len(test.keys))
		} else {
			expectedItemCount = uint32(len(test.keys) + 1)
		}
		idx, err := findKeyIndex(&node, test.insertKey, 0, expectedItemCount-1)
		if err != nil {
			t.Fatalf("(%s) Expected no error, got %s", test.name, err.Error())
		}

		if idx != int32(test.expectedInsertionIdx) {
			t.Fatalf("Expected key to be inserted at idx %d, got %d", test.expectedInsertionIdx, idx)
		}

		// retreive key
		cellOff := binary.LittleEndian.Uint32(node[(idx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+1 : (idx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+5])
		kLen := binary.LittleEndian.Uint32(node[cellOff+1 : cellOff+5])
		key := node[cellOff+13 : cellOff+13+kLen]
		if !bytes.Equal(key, test.insertKey) {
			t.Fatalf("Expected inserted key to be %v, but got %v", test.insertKey, key)
		}

		// retrieve ptr
		itemCount := binary.LittleEndian.Uint32(node[17:21])
		if itemCount == 0 {
			t.Fatalf("(%s) Invalid itemCount '0'", test.name)
		}

		if test.isDuplicate && itemCount != uint32(len(test.keys)) {
			t.Fatalf("(%s) Expected no new keys to be created while updating a duplicate key", test.name)
		}

		var ptr uint32
		if idx == int32(itemCount-1) {
			// last item
			ptr = binary.LittleEndian.Uint32(node[39:43])
		} else {
			ptrCellOff := binary.LittleEndian.Uint32(node[((idx+1)*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+1 : ((idx+1)*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+5])
			ptr = binary.LittleEndian.Uint32(node[ptrCellOff+9 : ptrCellOff+13])
		}

		if ptr != test.insertPtr {
			fmt.Println(printNodeContent(&node))
			t.Fatalf("(%s) Expected inserted child pointer to be %d, but got %d", test.name, test.insertPtr, ptr)
		}

		// check that node is marked dirty
		if !helpers.BitIsSet(&node[0], pgr.Dirty) {
			t.Fatalf("Expected node to be marked dirty")
		}

		// check that all cell pointers  have 'hasChildPtr' flag set
		for j := range itemCount {
			if !helpers.BitIsSet(&node[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*j)], pgr.HasChildPtr) {
				t.Fatalf("(%s) Expected 'hasChildPPtr' flag to be set in cell pointer at idx %d", test.name, j)
			}
		}

		// printNode
		fmt.Println(printNodeContent(&node))
	}
}

func TestDeleteKeyInternalNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true, lgr)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	tests := []struct {
		name                  string
		keys                  [][]byte
		ptrs                  []uint32
		delKey                []byte
		expectedDelPtr        uint32
		expectedRightChildPtr uint32
		leftMerge             bool
	}{
		{
			name:                  "delete last key in non-left merge",
			keys:                  [][]byte{[]byte("age"), []byte("code"), []byte("country"), []byte("name")},
			ptrs:                  []uint32{25, 66, 88, 99, 150},
			delKey:                []byte("name"),
			expectedDelPtr:        150,
			expectedRightChildPtr: 99,
			leftMerge:             false,
		},
		{
			name:                  "delete last key in left merge",
			keys:                  [][]byte{[]byte("age"), []byte("code"), []byte("country")},
			ptrs:                  []uint32{25, 66, 88, 99},
			delKey:                []byte("country"),
			expectedDelPtr:        88,
			expectedRightChildPtr: 99,
			leftMerge:             true,
		},
		{
			name: "delete key at idx 1 in a full node",
			keys: [][]byte{
				[]byte("user:22d8dc0fdad0109dc5e8c5bd75f0db6a:profile"),
				[]byte("user:6067aa99ef1e892517cfeba388a2841b:profile"),
				[]byte("user:b0d16b34348c90754cc4808eb2ccb395:profile"),
				[]byte("user:c95e067aad777c23cc345738d51b3bff:profile"),
			},
			ptrs:                  []uint32{1, 6, 2, 5, 4},
			delKey:                []byte("user:6067aa99ef1e892517cfeba388a2841b:profile"),
			expectedDelPtr:        2,
			expectedRightChildPtr: 4,
			leftMerge:             false,
		},
		{
			name: "delete key at idx 1 in a full node and left merge",
			keys: [][]byte{
				[]byte("user:22d8dc0fdad0109dc5e8c5bd75f0db6a:profile"),
				[]byte("user:6067aa99ef1e892517cfeba388a2841b:profile"),
				[]byte("user:b0d16b34348c90754cc4808eb2ccb395:profile"),
				[]byte("user:c95e067aad777c23cc345738d51b3bff:profile"),
			},
			ptrs:                  []uint32{1, 6, 2, 5, 4},
			delKey:                []byte("user:6067aa99ef1e892517cfeba388a2841b:profile"),
			expectedDelPtr:        6,
			expectedRightChildPtr: 4,
			leftMerge:             true,
		},
		{
			name: "delete first in a non-full node and non-left merge",
			keys: [][]byte{
				[]byte("user:22d8dc0fdad0109dc5e8c5bd75f0db6a:profile"),
				[]byte("user:6067aa99ef1e892517cfeba388a2841b:profile"),
				[]byte("user:b0d16b34348c90754cc4808eb2ccb395:profile"),
			},
			ptrs:                  []uint32{1, 6, 2, 4},
			delKey:                []byte("user:22d8dc0fdad0109dc5e8c5bd75f0db6a:profile"),
			expectedDelPtr:        6,
			expectedRightChildPtr: 4,
			leftMerge:             false,
		},
		{
			name: "delete first in a non-full node and non-left merge",
			keys: [][]byte{
				[]byte("user:22d8dc0fdad0109dc5e8c5bd75f0db6a:profile"),
				[]byte("user:6067aa99ef1e892517cfeba388a2841b:profile"),
				[]byte("user:b0d16b34348c90754cc4808eb2ccb395:profile"),
			},
			ptrs:                  []uint32{1, 6, 2, 4},
			delKey:                []byte("user:22d8dc0fdad0109dc5e8c5bd75f0db6a:profile"),
			expectedDelPtr:        1,
			expectedRightChildPtr: 4,
			leftMerge:             true,
		},
	}

	// keys := [][]byte{[]byte("age"), []byte("code"), []byte("country"), []byte("name")}
	// ptrs := []uint32{25, 66, 88, 99, 150}

	// example internal node before deletion
	// +--------+------+----------+-------+------+
	// |  age   | code |  country |  name |      |
	// +--------+-------+-----------------+ 150  +
	// |  25    |  66  |     88   |  99   |      |
	// +--------+------+----------+-------+------+

	for _, test := range tests {
		node := createTestInternalNode(35, test.keys, test.ptrs)

		deletedPtr, _, err := bp.deleteFromNode(&node, test.delKey, test.leftMerge)
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}
		fmt.Println(printNodeContent(&node))

		// expected internal node after deletion of the last item in non-left merge
		// +--------+------+----------+-------+------+
		// |  age   | code |  country |       |      |
		// +--------+-------+-----------------+  99  +
		// |  25    |  66  |     88   |       |      |
		// +--------+------+----------+-------+------+

		if deletedPtr != test.expectedDelPtr {
			t.Fatalf("Expected deleted pointer to be %d but got %d", test.expectedDelPtr, deletedPtr)
		}

		// check item count
		itemCount := binary.LittleEndian.Uint32(node[17:21])
		if l := len(test.keys) - 1; itemCount != uint32(l) {
			t.Fatalf("Expected item count to be %d, but got %d", l, itemCount)
		}

		idx, err := findKeyIndex(&node, test.delKey, 0, itemCount-1)
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}

		if idx != -1 {
			t.Fatalf("Expected item's index to be -1, got %d", idx)
		}

		// check right child ptr
		rightPtr := binary.LittleEndian.Uint32(node[39:43])
		if rightPtr != test.expectedRightChildPtr {
			t.Fatalf("Expected right child pointer to be %d, but got %d", test.expectedDelPtr, rightPtr)
		}
	}
}

func TestDeleteKeyLeafNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	keys := [][]byte{[]byte("age"), []byte("code"), []byte("country"), []byte("name")}
	vals := [][]byte{[]byte("thirty four"), []byte("KE"), []byte("united states"), []byte("leonard")}
	node := createTestLeafNode(25, keys, vals)

	// leaf node before deletion
	// +-----------------+------+---------------------+------------+
	// |      age        | code |       country       |  name      |
	// +-----------------+----------------------------+------------+
	// |  thirty four    |  US  |    united states    |  leonard   |
	// +-----------------+------+---------------------+------------+

	deletedPtr, v, err := bp.deleteFromNode(&node, keys[len(keys)-1], false)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if !bytes.Equal(v, vals[len(keys)-1]) {
		t.Fatalf("Expected deleted value to be \"%s\" but got \"%s\"", vals[len(keys)-1], v)
	}

	// expected leaf node after deletion of the last item
	// +-----------------+------+---------------------+------------+
	// |      age        | code |       country       |            |
	// +-----------------+----------------------------+------------+
	// |  thirty four    |  US  |    united states    |            |
	// +-----------------+------+---------------------+------------+

	if deletedPtr != 0 {
		t.Fatalf("Expected deleted pointer to be 0 but got %d", deletedPtr)
	}

	// check item count
	itemCount := binary.LittleEndian.Uint32(node[17:21])
	if l := len(keys) - 1; itemCount != uint32(l) {
		t.Fatalf("Expected item count to be %d, but got %d", l, itemCount)
	}

	idx, err := findKeyIndex(&node, keys[len(keys)-1], 0, itemCount-1)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if idx != -1 {
		t.Fatalf("Expected item's index to be -1, got %d", idx)
	}

	// expected leaf node after leftmerge delete of the key "country"(no difference)
	// +--------+------+----------+-------+
	// |  age   | code |	      |       |
	// +--------+-------+-----------------+
	// |  25    |  66  |          |       |
	// +--------+------+----------+-------+

	deleteKey := []byte("country")
	deleteVal := []byte("united states")
	deletedPtr, v, err = bp.deleteFromNode(&node, deleteKey, true)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	// ensure node is marked dirty
	if !helpers.BitIsSet(&(node[0]), pgr.Dirty) {
		t.Fatalf("Expected node to be marked dirty after deletion.")
	}

	if !bytes.Equal(v, deleteVal) {
		t.Fatalf("Expected deleted value to be \"%s\" but got \"%s\"", deleteVal, v)
	}

	if deletedPtr != 0 {
		t.Fatalf("Expected deleted pointer to be 0 but got %d", deletedPtr)
	}

	// check item count
	itemCount = binary.LittleEndian.Uint32(node[17:21])
	if l := len(keys) - 2; itemCount != uint32(l) {
		t.Fatalf("Expected item count to be %d, but got %d", l, itemCount)
	}
}

func TestGetFirstAndLastKeyInternalNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true, lgr)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	// leaf node
	keys := [][]byte{[]byte("age"), []byte("code"), []byte("country"), []byte("name")}
	ptrs := []uint32{25, 66, 88, 99, 150}
	node := createTestInternalNode(35, keys, ptrs)

	firstKey, err := bp.getFirstKey(&node)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if !bytes.Equal(firstKey, keys[0]) {
		t.Fatalf("Expected to get first key as %v but got %v", keys[0], firstKey)
	}

	lastKey, err := bp.getLastKey(&node)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if !bytes.Equal(lastKey, keys[len(keys)-1]) {
		t.Fatalf("Expected to get last key as %v but got %v", keys[len(keys)-1], lastKey)
	}
}

func TestGetFirstAndLastItemLeafNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true, lgr)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	keys := [][]byte{[]byte("age"), []byte("code"), []byte("country"), []byte("name")}
	vals := [][]byte{[]byte("thirty four"), []byte("US"), []byte("united states"), []byte("leonard")}
	node := createTestLeafNode(25, keys, vals)

	firstKey, err := bp.getFirstKey(&node)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if !bytes.Equal(firstKey, keys[0]) {
		t.Fatalf("Expected firstkey %v, got %v", firstKey, keys[0])
	}

	lastKey, err := bp.getLastKey(&node)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if !bytes.Equal(lastKey, keys[len(keys)-1]) {
		t.Fatalf("Expected lastKey %v, got %v", lastKey, keys[len(keys)-1])
	}
}

func TestSplitLeafNoUnderflow(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true, lgr)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	keys := [][]byte{[]byte("age"), []byte("code"), []byte("country"), []byte("name")}
	vals := [][]byte{[]byte("thirty four"), []byte("US"), []byte("united states"), []byte("leonard")}
	node := createTestLeafNode(25, keys, vals)

	_, _, err = bp.split(&node)
	if err == nil {
		t.Fatalf("Expected error, but got nil.")
	}
}

func TestSplitInternalNoUnderflow(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true, lgr)
	if err != nil {
		pgr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pgr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	keys := [][]byte{[]byte("age"), []byte("code"), []byte("country"), []byte("name")}
	ptrs := []uint32{25, 66, 88, 99, 150}

	node := createTestInternalNode(35, keys, ptrs)
	_, _, err = bp.split(&node)
	if err == nil {
		t.Fatalf("Expected error, but got nil.")
	}
}

func TestSplitInternalNode(t *testing.T) {
	testIterations := 1000
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	tests := []struct {
		name string
		keys [][]byte
		ptrs []uint32
	}{
		{
			name: "personal_info",
			keys: [][]byte{
				[]byte("age"),
				[]byte("code"),
				[]byte("country"),
				[]byte("marital status"),
				[]byte("name"),
			},
			ptrs: []uint32{25, 66, 88, 99, 150, 250},
		},
		{
			name: "geography",
			keys: [][]byte{
				[]byte("city"),
				[]byte("continent"),
				[]byte("country"),
				[]byte("district"),
				[]byte("region"),
			},
			ptrs: []uint32{10, 30, 55, 90, 150, 210},
		},
		{
			name: "technology",
			keys: [][]byte{
				[]byte("algorithm"),
				[]byte("compiler"),
				[]byte("database"),
				[]byte("kernel"),
				[]byte("network"),
			},
			ptrs: []uint32{5, 20, 40, 80, 160, 320},
		},
		{
			name: "animals",
			keys: [][]byte{
				[]byte("ant"),
				[]byte("cat"),
				[]byte("dog"),
				[]byte("elephant"),
				[]byte("zebra"),
			},
			ptrs: []uint32{1, 15, 45, 70, 110, 180},
		},
		{
			name: "fruits",
			keys: [][]byte{
				[]byte("apple"),
				[]byte("banana"),
				[]byte("grape"),
				[]byte("mango"),
				[]byte("orange"),
			},
			ptrs: []uint32{11, 22, 44, 88, 176, 352},
		},
		{
			name: "books",
			keys: [][]byte{
				[]byte("author"),
				[]byte("chapter"),
				[]byte("edition"),
				[]byte("publisher"),
				[]byte("title"),
			},
			ptrs: []uint32{12, 24, 36, 72, 144, 288},
		},
		{
			name: "vehicles",
			keys: [][]byte{
				[]byte("bike"),
				[]byte("bus"),
				[]byte("car"),
				[]byte("truck"),
				[]byte("van"),
			},
			ptrs: []uint32{7, 21, 49, 98, 196, 392},
		},
		{
			name: "filesystem",
			keys: [][]byte{
				[]byte("bin"),
				[]byte("etc"),
				[]byte("home"),
				[]byte("tmp"),
				[]byte("usr"),
			},
			ptrs: []uint32{3, 9, 27, 81, 243, 729},
		},
		{
			name: "programming_languages",
			keys: [][]byte{
				[]byte("c"),
				[]byte("go"),
				[]byte("java"),
				[]byte("python"),
				[]byte("rust"),
			},
			ptrs: []uint32{13, 26, 52, 104, 208, 416},
		},
		{
			name: "months_subset",
			keys: [][]byte{
				[]byte("april"),
				[]byte("august"),
				[]byte("january"),
				[]byte("june"),
				[]byte("march"),
			},
			ptrs: []uint32{8, 16, 32, 64, 128, 256},
		},
		{
			name: "long_keys",
			keys: [][]byte{
				[]byte("user:367c34c436f6547a627b14793d9570ec:profile"),
				[]byte("user:4bc06623c93ccc7801d517b535b4745f:profile"),
				[]byte("user:84d6d51099bc3b16b0bb16bc83befdbf:profile"),
				[]byte("user:974fbf29f6580510f1cbe0af905b031b:profile"),
				[]byte("user:c02ccfb419d665a867e9de8ce92637b3:profile"),
			},
			ptrs: []uint32{1, 4, 6, 2, 7, 5},
		},
	}

	// example overflown internal node with order 2
	// +--------+-------+---------+--------------------+--------+------+
	// |  age   | code  | country |   marital status   |  name  |      |
	// +--------+-------+---------+--------------------+--------+ 250  +
	// |  25    |  66   |    88   |         99         |  150   |      |
	// +--------+-------+---------+--------------------+--------+------+

	for it := range testIterations {
		t.Logf("Starting iteration %d.", it)
		for _, test := range tests {
			t.Logf("----------------------------------\n")
			t.Logf("Running test: %s\n", test.name)
			t.Logf("----------------------------------\n")
			node := createTestInternalNode(uint32(it), test.keys, test.ptrs)

			leftSibling, err := bp.buffermanager.NewFrame(true, false)
			if err != nil {
				t.Fatalf("Unable to create new frame: %s", err)
			}

			rightSibling, err := bp.buffermanager.NewFrame(true, false)
			if err != nil {
				t.Fatalf("Unable to create new frame: %s", err)
			}

			// set sibling pointer
			leftSibling.Acquire(false)
			leftSibFr, _, err := leftSibling.RawBufferSlice()
			if err != nil {
				t.Fatalf("Unable to get frame buffer: %s", err.Error())
			}
			copy(node[47:51], (*leftSibFr)[1:5])
			copy((*leftSibFr)[43:47], node[1:5])
			leftSibling.Release(false)
			leftSibling.Unreference()

			rightSibling.Acquire(false)
			rightSiblingFr, _, err := rightSibling.RawBufferSlice()
			if err != nil {
				t.Fatalf("Unable to get frame buffer: %s", err.Error())
			}
			copy(node[43:47], (*rightSiblingFr)[1:5])
			copy((*rightSiblingFr)[47:51], node[1:5])
			rightSibling.Release(false)
			rightSibling.Unreference()

			fmt.Println("NODE BEFORE SPLIT...")
			fmt.Println(printNodeContent(&node))

			newSepKey, newFramePid, err := bp.split(&node)
			if err != nil {
				t.Fatalf("Expected no error, got %s", err.Error())
			}

			// example expected nodes after split
			//			 +---------+
			//			 | country |
			//			 +---------+
			//			/           \
			//                     /	     \
			//                    /               \
			// +--------+-------+------+    +------------------+--------+---------+
			// |  age   | code  |      |    |  marital status  |  name  |         |
			// +--------+-------+  88  +<-->+------------------+--------+   250   +
			// |  25    |  66   |	   |    |       99         |  150   |         |
			// +--------+-------+------+    +------------------+--------+----------

			if newFramePid == 0 {
				t.Fatalf("Invalid frame pid: %d", newFramePid)
			}

			if !helpers.BitIsSet(&node[0], pgr.Dirty) {
				t.Fatalf("Expected left node to be marked as dirty.")
			}

			// verify seperator key
			if !bytes.Equal(newSepKey, test.keys[pgr.ORDER]) {
				t.Fatalf("Expected separator key to be %v, but got %v", test.keys[pgr.ORDER], newSepKey)
			}

			fmt.Printf("LEFT NODE AFTER SPLIT --> \n")
			fmt.Println(printNodeContent(&node))
			// verify keys on the left node
			leftNodeItemCount := binary.LittleEndian.Uint32(node[17:21])
			if leftNodeItemCount != pgr.ORDER {
				t.Fatalf("Expected itemcount in left node to be %d, but got %d", pgr.ORDER, leftNodeItemCount)
			}

			var leftNodeTotCellSize uint32
			for i := range leftNodeItemCount {
				cellOff := binary.LittleEndian.Uint32(node[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+5])
				cellKeySize := binary.LittleEndian.Uint32(node[cellOff+1 : cellOff+5])
				key := node[cellOff+13 : cellOff+13+cellKeySize]
				ptr := binary.LittleEndian.Uint32(node[cellOff+9 : cellOff+13])

				// check cell offset
				expectedCellOff := pgr.PAGE_SIZE_BYTES - (pgr.LOWER_PADDING_BYTES + leftNodeTotCellSize + (13 + cellKeySize))
				if cellOff != expectedCellOff {
					t.Fatalf("Expected cell offset at idx %d to be %d, got %d\n", i, expectedCellOff, cellOff)
				}

				leftNodeTotCellSize += (13 + cellKeySize)
				// check that hasChildPtr flag is set
				if !helpers.BitIsSet(&node[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)], pgr.HasChildPtr) {
					t.Fatalf("Expected \"hasChildPtr\" flag to be set at idx: %d", i)
				}

				if !bytes.Equal(key, test.keys[i]) {
					t.Fatalf("Expected key at index %d to be %v, but got %v", i, test.keys[i], key)
				}

				if ptr != test.ptrs[i] {
					t.Fatalf("Expected pointer at index %d to be %d, but got %d", i, test.ptrs[i], ptr)
				}
			}

			// check right most child
			leftNodeRightChild := binary.LittleEndian.Uint32(node[39:43])
			if leftNodeRightChild != test.ptrs[leftNodeItemCount] {
				t.Fatalf("Expected left node's right child to be %d, but got %d", test.ptrs[leftNodeItemCount], leftNodeRightChild)
			}

			// verify left node's lower and upper offsets
			expectedLeftLowOff := pgr.HEADER_SIZE_BYTES + (leftNodeItemCount * pgr.CELL_POINTER_SIZE_BYTE)
			if lOff := binary.LittleEndian.Uint32(node[29:33]); lOff != expectedLeftLowOff {
				t.Fatalf("Expected left node's lower offset to be %d, got %d", expectedLeftLowOff, lOff)
			}

			expectedLeftUpperOff := pgr.PAGE_SIZE_BYTES - (pgr.LOWER_PADDING_BYTES + leftNodeTotCellSize)
			t.Logf("ExpectedLeftNode Upper Offset --> %d\n", expectedLeftUpperOff)
			if uOff := binary.LittleEndian.Uint32(node[25:29]); uOff != expectedLeftUpperOff {
				t.Fatalf("Expected left node's upper offset to be %d, got %d", expectedLeftUpperOff, uOff)
			}

			// verify right node
			rightNode, _, err := buffManager.Get(newFramePid)
			if err != nil {
				t.Fatalf("Expected no error, but got %s", err.Error())
			}

			if rightNode == nil {
				t.Fatalf("No right node available")
			}
			rightNodeBuff, _, err := rightNode.RawBufferSlice()
			if err != nil {
				t.Fatalf("Expected no error, but got %s", err.Error())
			}

			fmt.Println("NODES AFTER SPLIT...")
			fmt.Println("LEFT NODE  -> ")
			fmt.Println(printNodeContent(&node))
			fmt.Println("RIGHT NODE -> ")
			fmt.Println(printNodeContent(rightNodeBuff))

			// ensure new node was marked as dirty
			if !helpers.BitIsSet(&(*rightNodeBuff)[0], pgr.Dirty) {
				// NOTE: if test is ran with background writer enabled,
				// the page could already be flushed and marked
				// clean, causing this to fail
				t.Logf("(%s) WARNING Expected new node to be marked as dirty. If test is ran with background writer enabled, the page could already be flushed and marked clean, causing this warning", t.Name())
			}

			rightNodeRightChild := binary.LittleEndian.Uint32((*rightNodeBuff)[39:43])
			if rightNodeRightChild != test.ptrs[len(test.ptrs)-1] {
				t.Fatalf("Expected right node's right child to be %d, but got %d", test.ptrs[len(test.ptrs)-1], rightNodeRightChild)
			}

			rightNodeItemCount := binary.LittleEndian.Uint32((*rightNodeBuff)[17:21])
			expectedRightNodeItemCount := len(test.keys) - pgr.ORDER - 1
			if rightNodeItemCount != uint32(expectedRightNodeItemCount) {
				t.Fatalf("Expected itemcount in right node to be %d, but got %d", expectedRightNodeItemCount, rightNodeItemCount)
			}

			// verify sibling pointers
			if !bytes.Equal(node[43:47], (*rightNodeBuff)[1:5]) {
				t.Fatalf("Expected leftNode's right sibling to be %d, but got %d", binary.LittleEndian.Uint32((*rightNodeBuff)[1:5]), binary.LittleEndian.Uint32(node[43:47]))
			}

			if !bytes.Equal((*rightNodeBuff)[47:51], (node)[1:5]) {
				t.Fatalf("Expected newNode's left sibling to be %d, but got %d", binary.LittleEndian.Uint32((node)[1:5]), binary.LittleEndian.Uint32((*rightNodeBuff)[47:51]))
			}

			if !bytes.Equal((*rightNodeBuff)[43:47], (*rightSiblingFr)[1:5]) {
				t.Fatalf("Expected newNode's right sibling to be %d, but got %d", binary.LittleEndian.Uint32((*rightSiblingFr)[1:5]), binary.LittleEndian.Uint32((*rightNodeBuff)[43:47]))
			}

			if !bytes.Equal((*rightSiblingFr)[47:51], (*rightNodeBuff)[1:5]) {
				t.Fatalf("Expected rightSibling's left sibling to be %d, but got %d", binary.LittleEndian.Uint32((*rightNodeBuff)[1:5]), binary.LittleEndian.Uint32((*rightSiblingFr)[47:51]))
			}

			// ensure all modified nodes are marked dirty
			if !helpers.BitIsSet(&(node[0]), pgr.Dirty) {
				t.Fatalf("Expected leftNode to be marked dirty.")
			}

			if !helpers.BitIsSet(&(*rightNodeBuff)[0], pgr.Dirty) {
				t.Fatalf("Expected newNode to be marked dirty.")
			}

			if !helpers.BitIsSet(&(*rightSiblingFr)[0], pgr.Dirty) {
				t.Fatalf("Expected rightSibling to be marked dirty.")
			}

			var rightNodeTotCellSize uint32
			for i := range rightNodeItemCount {
				cellOff := binary.LittleEndian.Uint32((*rightNodeBuff)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+5])
				cellKeySize := binary.LittleEndian.Uint32((*rightNodeBuff)[cellOff+1 : cellOff+5])
				rightNodeTotCellSize += (13 + cellKeySize)
				key := (*rightNodeBuff)[cellOff+13 : cellOff+13+cellKeySize]
				ptr := binary.LittleEndian.Uint32((*rightNodeBuff)[cellOff+9 : cellOff+13])

				// check that hasChildPtr flag is set
				if !helpers.BitIsSet(&(*rightNodeBuff)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)], pgr.HasChildPtr) {
					t.Fatalf("Expected \"hasChildPtr\" flag to be set at idx: %d", i)
				}

				if !bytes.Equal(key, test.keys[i+1+pgr.ORDER]) {
					t.Fatalf("Expected key at index %d to be %v, but got %v", i, test.keys[i+1+pgr.ORDER], key)
				}

				if ptr != test.ptrs[i+pgr.ORDER+1] {
					t.Fatalf("Expected pointer at index %d to be %d, but got %d", i, test.ptrs[i+pgr.ORDER+i], ptr)
				}
			}

			// verify right node's lower and upper offsets
			expectedRightLowOff := pgr.HEADER_SIZE_BYTES + (rightNodeItemCount * pgr.CELL_POINTER_SIZE_BYTE)
			if lOff := binary.LittleEndian.Uint32((*rightNodeBuff)[29:33]); lOff != expectedRightLowOff {
				t.Fatalf("Expected right node's lower offset to be %d, got %d", expectedRightLowOff, lOff)
			}

			expectedRightUppOff := pgr.PAGE_SIZE_BYTES - (pgr.LOWER_PADDING_BYTES + rightNodeTotCellSize)
			if uOff := binary.LittleEndian.Uint32((*rightNodeBuff)[25:29]); uOff != expectedRightUppOff {
				t.Fatalf("Expected right node's upper offset to be %d, got %d", expectedRightUppOff, uOff)
			}

			rightNode.Unreference()
		}
	}
}

func TestSplitLeafNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	tests := []struct {
		name string
		keys [][]byte
		vals [][]byte
	}{
		{
			name: "personal_info",
			keys: [][]byte{
				[]byte("age"),
				[]byte("code"),
				[]byte("country"),
				[]byte("marital status"),
				[]byte("name"),
			},
			vals: [][]byte{
				[]byte("25"),
				[]byte("66"),
				[]byte("Kenya"),
				[]byte("Single"),
				[]byte("Ian"),
			},
		},
		{
			name: "geography",
			keys: [][]byte{
				[]byte("city"),
				[]byte("continent"),
				[]byte("country"),
				[]byte("district"),
				[]byte("region"),
			},
			vals: [][]byte{
				[]byte("Nairobi"),
				[]byte("Africa"),
				[]byte("Kenya"),
				[]byte("Westlands"),
				[]byte("East Africa"),
			},
		},
		{
			name: "technology",
			keys: [][]byte{
				[]byte("algorithm"),
				[]byte("compiler"),
				[]byte("database"),
				[]byte("kernel"),
				[]byte("network"),
			},
			vals: [][]byte{
				[]byte("B+ Tree"),
				[]byte("Go Compiler"),
				[]byte("PostgreSQL"),
				[]byte("Linux"),
				[]byte("TCP/IP"),
			},
		},
		{
			name: "animals",
			keys: [][]byte{
				[]byte("ant"),
				[]byte("cat"),
				[]byte("dog"),
				[]byte("elephant"),
				[]byte("zebra"),
			},
			vals: [][]byte{
				[]byte("insect"),
				[]byte("mammal"),
				[]byte("mammal"),
				[]byte("largest land animal"),
				[]byte("striped mammal"),
			},
		},
		{
			name: "fruits",
			keys: [][]byte{
				[]byte("apple"),
				[]byte("banana"),
				[]byte("grape"),
				[]byte("mango"),
				[]byte("orange"),
			},
			vals: [][]byte{
				[]byte("red"),
				[]byte("yellow"),
				[]byte("purple"),
				[]byte("sweet"),
				[]byte("citrus"),
			},
		},
		{
			name: "books",
			keys: [][]byte{
				[]byte("author"),
				[]byte("chapter"),
				[]byte("edition"),
				[]byte("publisher"),
				[]byte("title"),
			},
			vals: [][]byte{
				[]byte("Alex Petrov"),
				[]byte("8"),
				[]byte("2"),
				[]byte("O'Reilly"),
				[]byte("Database Internals"),
			},
		},
		{
			name: "vehicles",
			keys: [][]byte{
				[]byte("bike"),
				[]byte("bus"),
				[]byte("car"),
				[]byte("truck"),
				[]byte("van"),
			},
			vals: [][]byte{
				[]byte("2 wheels"),
				[]byte("public transport"),
				[]byte("sedan"),
				[]byte("cargo"),
				[]byte("minivan"),
			},
		},
		{
			name: "filesystem",
			keys: [][]byte{
				[]byte("bin"),
				[]byte("etc"),
				[]byte("home"),
				[]byte("tmp"),
				[]byte("usr"),
			},
			vals: [][]byte{
				[]byte("/bin"),
				[]byte("/etc"),
				[]byte("/home"),
				[]byte("/tmp"),
				[]byte("/usr"),
			},
		},
		{
			name: "programming_languages",
			keys: [][]byte{
				[]byte("c"),
				[]byte("go"),
				[]byte("java"),
				[]byte("python"),
				[]byte("rust"),
			},
			vals: [][]byte{
				[]byte("1972"),
				[]byte("2009"),
				[]byte("1995"),
				[]byte("1991"),
				[]byte("2015"),
			},
		},
		{
			name: "months_subset",
			keys: [][]byte{
				[]byte("april"),
				[]byte("august"),
				[]byte("january"),
				[]byte("june"),
				[]byte("march"),
			},
			vals: [][]byte{
				[]byte("4"),
				[]byte("8"),
				[]byte("1"),
				[]byte("6"),
				[]byte("3"),
			},
		},
		{
			name: "user data",
			keys: [][]byte{
				[]byte("user:22c3085845dc2cee9572fbcef3867bdb:profile"),
				[]byte("user:631902c9da01256f9997ed768805a7e3:profile"),
				[]byte("user:6d8a46ac289ad5fb7e48ab190480939a:profile"),
				[]byte("user:761a8b7861c5ecee91c6132b379fef8c:profile"),
				[]byte("user:ab0d2514b97856c9d32e1418b1072237:profile"),
			},
			vals: [][]byte{
				[]byte(`{"id":"22c3085845dc2cee9572fbcef3867bdb","name":"User 2","email":"user2@example.com","active":true}`),
				[]byte(`{"id":"631902c9da01256f9997ed768805a7e3","name":"User 0","email":"user0@example.com","active":true}`),
				[]byte(`{"id":"6d8a46ac289ad5fb7e48ab190480939a","name":"User 1","email":"user1@example.com","active":true}`),
				[]byte(`{"id":"761a8b7861c5ecee91c6132b379fef8c","name":"User 4","email":"user4@example.com","active":true}`),
				[]byte(`{"id":"ab0d2514b97856c9d32e1418b1072237","name":"User 3","email":"user3@example.com","active":true}`),
			},
		},
	}

	// example overflown internal node with order 2
	// +------------+-------+--------------------+--------------------+--------+
	// |  age	| code  |      country       |   marital status   |  name  |
	// +------------+-------+-----+--------------+-----------------------------+
	// |  thirty    |  US   |    united states   |        single      |  Bob   |
	// +------------+-------+--------------------+--------------------+--------+
	for p, test := range tests {
		t.Logf("----------------------------------\n")
		t.Logf("Running test: %s\n", test.name)
		t.Logf("----------------------------------\n")
		node := createTestLeafNode(uint32(p*20), test.keys, test.vals)

		leftSibling, err := bp.buffermanager.NewFrame(true, false)
		if err != nil {
			t.Fatalf("Unable to create new frame: %s", err)
		}

		rightSibling, err := bp.buffermanager.NewFrame(true, false)
		if err != nil {
			t.Fatalf("Unable to create new frame: %s", err)
		}

		// set sibling pointer
		leftSibling.Acquire(false)
		leftSibFr, _, err := leftSibling.RawBufferSlice()
		if err != nil {
			t.Fatalf("Unable to get frame buffer: %s", err.Error())
		}
		copy(node[47:51], (*leftSibFr)[1:5])
		copy((*leftSibFr)[43:47], node[1:5])
		leftSibling.Release(false)
		leftSibling.Unreference()

		rightSibling.Acquire(false)
		rightSiblingFr, _, err := rightSibling.RawBufferSlice()
		if err != nil {
			t.Fatalf("Unable to get frame buffer: %s", err.Error())
		}
		copy(node[43:47], (*rightSiblingFr)[1:5])
		copy((*rightSiblingFr)[47:51], node[1:5])
		rightSibling.Release(false)
		rightSibling.Unreference()

		newSepKey, newFramePid, err := bp.split(&node)
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}

		// example expected nodes after split
		//			 +---------+
		//			 | country |
		//			 +---------+
		//			/           \
		//                     /	     \
		//                    /               \
		//  +------------+-------+	  +--------------------+--------------------+--------+
		//  |  age	| code   |	  |      country       |   marital status   |  name  |
		//  +-----------+--------+	  +-----+--------------+-----------------------------+
		//  |  thirty   |  US    |	  |    united states   |        single      |  Bob   |
		//  +------------+-------+	  +--------------------+--------------------+--------+

		if newFramePid == 0 {
			t.Fatalf("Invalid frame pid: %d", newFramePid)
		}

		if !helpers.BitIsSet(&node[0], pgr.Dirty) {
			t.Fatalf("Expected left node to be marked as dirty.")
		}

		// verify seperator key
		if !bytes.Equal(newSepKey, test.keys[pgr.ORDER]) {
			t.Fatalf("Expected separator key to be %v, but got %v", test.keys[pgr.ORDER], newSepKey)
		}

		// verify keys on the left node
		leftNodeItemCount := binary.LittleEndian.Uint32(node[17:21])
		if leftNodeItemCount != pgr.ORDER {
			t.Fatalf("Expected itemcount in left node to be %d, but got %d", pgr.ORDER, leftNodeItemCount)
		}

		var totLeftNodeCellSize uint32 = 0
		var lastKey []byte
		for i := range leftNodeItemCount {
			cellOff := binary.LittleEndian.Uint32(node[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+5])
			cellKeySize := binary.LittleEndian.Uint32(node[cellOff+1 : cellOff+5])
			key := node[cellOff+13 : cellOff+13+cellKeySize]
			valLen := binary.LittleEndian.Uint32(node[cellOff+5 : cellOff+9])
			val := node[cellOff+13+cellKeySize : cellOff+13+cellKeySize+valLen]

			if !bytes.Equal(key, test.keys[i]) {
				t.Fatalf("Expected key at index %d to be %v, but got %v", i, test.keys[i], key)
			}

			if !bytes.Equal(val, test.vals[i]) {
				t.Fatalf("Expected value at index %d to be %v, but got %v", i, test.vals[i], val)
			}

			if i > 0 {
				if bytes.Compare(lastKey, key) > -1 {
					t.Fatalf("Expected current key: %s, to be greater than previous key: %s", key, lastKey)
				}
			}

			lastKey = make([]byte, cellKeySize)
			copy(lastKey, key)

			totLeftNodeCellSize += 13 + cellKeySize + valLen
		}

		// verify left node lower offset
		expectedLeftNodeLowerOffset := pgr.HEADER_SIZE_BYTES + (leftNodeItemCount * pgr.CELL_POINTER_SIZE_BYTE)
		if l := binary.LittleEndian.Uint32(node[29:33]); l != expectedLeftNodeLowerOffset {
			t.Fatalf("Expected left node's lower offset to be %d but got %d", expectedLeftNodeLowerOffset, l)
		}

		// verfiy left node's upper offset

		// for i := range leftNodeItemCount {
		// 	kLen := len(test.keys[i])
		// 	vLen := len(test.vals[i])
		// 	totLeftNodeCellSize += 13 + kLen + vLen
		// }

		expectedLeftNodeUppOff := (pgr.PAGE_SIZE_BYTES - pgr.LOWER_PADDING_BYTES) - (totLeftNodeCellSize)
		if lUpp := binary.LittleEndian.Uint32(node[25:29]); lUpp != uint32(expectedLeftNodeUppOff) {
			t.Fatalf("Expected left node's upper offset to be %d but got %d", expectedLeftNodeUppOff, lUpp)
		}

		fmt.Printf("(%s) PRINTING LEFT NODE's CELL AFTER UPPER OFFSET -->\n", test.name)
		fmt.Printf("%v\n", node[expectedLeftNodeUppOff-157:expectedLeftNodeUppOff])
		fmt.Printf("(%s) PRINTING LEFT NODE's CELL AT UPPER OFFSET -->\n", test.name)
		fmt.Printf("%v\n", node[expectedLeftNodeUppOff:8176])

		// verify right node
		rightNode, _, err := buffManager.Get(newFramePid)
		if err != nil {
			t.Fatalf("Expected no error, but got %s", err.Error())
		}

		if rightNode == nil {
			t.Fatalf("No right node available")
		}
		rightNodeBuff, _, err := rightNode.RawBufferSlice()
		if err != nil {
			t.Fatalf("Expected no error, but got %s", err.Error())
		}

		// ensure node was marked as dirty
		if !helpers.BitIsSet(&(*rightNodeBuff)[0], pgr.Dirty) {
			t.Fatalf("Expected new node to be marked as dirty.")
		}

		rightNodeItemCount := binary.LittleEndian.Uint32((*rightNodeBuff)[17:21])
		expectedRightNodeItemCount := len(test.keys) - pgr.ORDER
		if rightNodeItemCount != uint32(expectedRightNodeItemCount) {
			t.Fatalf("Expected itemcount in right node to be %d, but got %d", expectedRightNodeItemCount, rightNodeItemCount)
		}

		// verify sibling pointers
		if !bytes.Equal(node[43:47], (*rightNodeBuff)[1:5]) {
			t.Fatalf("Expected leftNode's right sibling to be %d, but got %d", binary.LittleEndian.Uint32((*rightNodeBuff)[1:5]), binary.LittleEndian.Uint32(node[43:47]))
		}

		if !bytes.Equal((*rightNodeBuff)[47:51], (node)[1:5]) {
			t.Fatalf("Expected newNode's left sibling to be %d, but got %d", binary.LittleEndian.Uint32((node)[1:5]), binary.LittleEndian.Uint32((*rightNodeBuff)[47:51]))
		}

		if !bytes.Equal((*rightNodeBuff)[43:47], (*rightSiblingFr)[1:5]) {
			t.Fatalf("Expected newNode's right sibling to be %d, but got %d", binary.LittleEndian.Uint32((*rightSiblingFr)[1:5]), binary.LittleEndian.Uint32((*rightNodeBuff)[43:47]))
		}

		if !bytes.Equal((*rightSiblingFr)[47:51], (*rightNodeBuff)[1:5]) {
			t.Fatalf("Expected rightSibling's left sibling to be %d, but got %d", binary.LittleEndian.Uint32((*rightNodeBuff)[1:5]), binary.LittleEndian.Uint32((*rightSiblingFr)[47:51]))
		}

		var totRightNodeCellSize uint32 = 0
		for i := range rightNodeItemCount {
			cellOff := binary.LittleEndian.Uint32((*rightNodeBuff)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+5])
			cellKeySize := binary.LittleEndian.Uint32((*rightNodeBuff)[cellOff+1 : cellOff+5])
			key := (*rightNodeBuff)[cellOff+13 : cellOff+13+cellKeySize]
			valSize := binary.LittleEndian.Uint32((*rightNodeBuff)[cellOff+5 : cellOff+9])
			val := (*rightNodeBuff)[cellOff+13+cellKeySize : cellOff+13+cellKeySize+valSize]

			if !bytes.Equal(key, test.keys[i+pgr.ORDER]) {
				t.Fatalf("Expected key at index %d to be %v, but got %v", i, test.keys[i+pgr.ORDER], key)
			}

			if !bytes.Equal(val, test.vals[i+pgr.ORDER]) {
				t.Fatalf("Expected val at index %d to be %v, but got %v", i, test.vals[i+pgr.ORDER], val)
			}

			if bytes.Compare(lastKey, key) > -1 {
				t.Fatalf("Expected current key: %s to be greater than previous key: %s", key, lastKey)
			}

			lastKey = make([]byte, cellKeySize)
			copy(lastKey, key)

			totRightNodeCellSize += 13 + cellKeySize + valSize
		}

		// verify right node lower offset
		expectedRightNodeLowerOffset := pgr.HEADER_SIZE_BYTES + (rightNodeItemCount * pgr.CELL_POINTER_SIZE_BYTE)
		if r := binary.LittleEndian.Uint32((*rightNodeBuff)[29:33]); r != expectedRightNodeLowerOffset {
			t.Fatalf("Expected right node's lower offset to be %d but got %d", expectedLeftNodeLowerOffset, r)
		}

		// verfiy right node's upper offset

		// for i := pgr.ORDER; i < len(test.keys); i++ {
		// 	kLen := len(test.keys[i])
		// 	vLen := len(test.vals[i])
		// 	totRightNodeCellSize += 13 + kLen + vLen
		// 	fmt.Printf("CELL SIZE --> %d\n", (13 + kLen + vLen))
		// }

		expectedRightNodeUppOff := (pgr.PAGE_SIZE_BYTES - pgr.LOWER_PADDING_BYTES) - (totRightNodeCellSize)
		if rUpp := binary.LittleEndian.Uint32((*rightNodeBuff)[25:29]); rUpp != uint32(expectedRightNodeUppOff) {
			t.Fatalf("Expected right node's upper offset to be %d but got %d", expectedLeftNodeUppOff, rUpp)
		}
	}
}

func TestMergeNoUnderflowInternalNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	sepKey := []byte("nationality")
	leftKeys := [][]byte{[]byte("age"), []byte("country"), []byte("name")}
	leftPtrs := []uint32{25, 88, 99, 150}
	leftNode := createTestInternalNode(35, leftKeys, leftPtrs)

	rightKeys := [][]byte{
		[]byte("office"),
		[]byte("organization"),
		[]byte("owner"),
		[]byte("passport"),
	}
	rightPtrs := []uint32{10, 30, 55, 90, 150}
	rightNode := createTestInternalNode(45, rightKeys, rightPtrs)

	// set sibling pointers
	binary.LittleEndian.PutUint32(leftNode[43:47], binary.LittleEndian.Uint32(rightNode[1:5]))
	binary.LittleEndian.PutUint32(rightNode[47:51], binary.LittleEndian.Uint32(leftNode[1:5]))

	_, err = bp.merge(&leftNode, &rightNode, sepKey, false)
	if err == nil {
		t.Fatalf("Expected error but got nil.")
	}
}

func TestMergeNoUnderflowLeafNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	sepKey := []byte("nationality")
	leftKeys := [][]byte{[]byte("age"), []byte("country"), []byte("name")}
	leftVals := [][]byte{
		[]byte("age"),
		[]byte("code"),
		[]byte("country"),
	}
	leftNode := createTestLeafNode(35, leftKeys, leftVals)

	rightKeys := [][]byte{
		[]byte("office"),
		[]byte("organization"),
		[]byte("owner"),
		[]byte("passport"),
	}
	rightVals := [][]byte{[]byte("Nairobi"),
		[]byte("Africa"),
		[]byte("marital status"),
		[]byte("Kenya")}
	rightNode := createTestLeafNode(45, rightKeys, rightVals)

	// set sibling pointers
	binary.LittleEndian.PutUint32(leftNode[43:47], binary.LittleEndian.Uint32(rightNode[1:5]))
	binary.LittleEndian.PutUint32(rightNode[47:51], binary.LittleEndian.Uint32(leftNode[1:5]))

	_, err = bp.merge(&leftNode, &rightNode, sepKey, false)
	if err == nil {
		t.Fatalf("Expected error but got nil.")
	}
}

func TestMergeInternalRightToLeft(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	tests := []struct {
		name                   string
		leftNodeKeys           [][]byte
		leftNodePtrs           []uint32
		leftNodePid            uint32
		rightNodeKeys          [][]byte
		rightNodePtrs          []uint32
		rightNodePid           uint32
		sepKey                 []byte
		expectedMergedNodeKeys [][]byte
		expectedMergedNodePtrs []uint32
	}{
		{
			name:                   "normal underflowed right node",
			leftNodeKeys:           [][]byte{[]byte("age"), []byte("country")},
			leftNodePtrs:           []uint32{25, 88, 99},
			leftNodePid:            35,
			rightNodeKeys:          [][]byte{[]byte("office")},
			rightNodePtrs:          []uint32{10, 30},
			rightNodePid:           55,
			sepKey:                 []byte("nationality"),
			expectedMergedNodeKeys: [][]byte{[]byte("age"), []byte("country"), []byte("nationality"), []byte("office")},
			expectedMergedNodePtrs: []uint32{25, 88, 99, 10, 30},
		},
	}

	// example nodes before merge (order=2)
	//		           +-------------+
	//		           | nationality |
	//		           +-------------+
	//	                  /               \
	//                       /	           \
	//                      /                   \
	// +--------+----------+------+         +----------+-------+
	// |  age   | country  |      |         |  office  |       |
	// +--------+----------+  99  +<------->+----------+   30  +
	// |  25    |   88     |      |         |    10    |       |
	// +--------+----------+------+         +----------+-------+

	for _, test := range tests {
		leftNode := createTestInternalNode(test.leftNodePid, test.leftNodeKeys, test.leftNodePtrs)

		leftNodeLeftSibling, err := bp.buffermanager.NewFrame(true, false)
		if err != nil {
			t.Fatalf("Unable to create new frame: %s", err)
		}

		rightNode := createTestInternalNode(test.rightNodePid, test.rightNodeKeys, test.rightNodePtrs)
		rightNodeRightSibling, err := bp.buffermanager.NewFrame(true, false)
		if err != nil {
			t.Fatalf("Unable to create new frame: %s", err)
		}

		// set sibling pointers
		binary.LittleEndian.PutUint32(leftNode[43:47], test.rightNodePid)
		binary.LittleEndian.PutUint32(rightNode[47:51], test.leftNodePid)

		rightNodeRightSibling.Acquire(false)
		rightNodeRightSibFr, _, err := rightNodeRightSibling.RawBufferSlice()
		if err != nil {
			t.Fatalf("Unable to get frame buffer: %s", err)
		}
		binary.LittleEndian.PutUint32((*rightNodeRightSibFr)[47:51], test.rightNodePid)
		copy(rightNode[43:47], (*rightNodeRightSibFr)[1:5])
		rightNodeRightSibling.Release(false)
		rightNodeRightSibling.Unreference()

		leftNodeLeftSibling.Acquire(false)
		leftNodeLeftSibFr, _, err := leftNodeLeftSibling.RawBufferSlice()
		if err != nil {
			t.Fatalf("Unable to get frame buffer: %s", err)
		}
		binary.LittleEndian.PutUint32((*leftNodeLeftSibFr)[43:47], test.leftNodePid)
		copy(leftNode[47:51], (*leftNodeLeftSibFr)[1:5])
		leftNodeLeftSibling.Release(false)
		leftNodeLeftSibling.Unreference()

		newSepKey, err := bp.merge(&leftNode, &rightNode, test.sepKey, false)
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}

		if newSepKey != nil {
			t.Fatalf("Expected no new separator key, got %v", newSepKey)
		}

		// example expected node after merge; (order=2)

		// +--------+----------+-----------------+------------------+
		// |  age   | country  |  nationality    |  office  |       |
		// +--------+----------+-----------------+----------+   30  +
		// |  25    |   88     |      99         |    10    |       |
		// +--------+----------+-----------------+----------+-------+

		// check that right node is marked for deletion
		markedDead := helpers.BitIsSet(&rightNode[0], pgr.Dead)
		if !markedDead {
			t.Fatalf("Expected merged node to be marked as dead.")
		}

		leftNodeDirty := helpers.BitIsSet(&leftNode[0], pgr.Dirty)
		if !leftNodeDirty {
			t.Fatalf("Expected left node to be marked dirty")
		}

		itemCount := binary.LittleEndian.Uint32(leftNode[17:21])
		expectedItemCount := len(test.leftNodeKeys) + len(test.rightNodeKeys) + 1
		if itemCount != uint32(expectedItemCount) {
			t.Fatalf("Expected number of items in new node to be %d but got %d", expectedItemCount, itemCount)
		}

		// check right most child
		expectedRightChildPtr := test.rightNodePtrs[len(test.rightNodePtrs)-1]
		if ch := binary.LittleEndian.Uint32(leftNode[39:43]); ch != expectedRightChildPtr {
			t.Fatalf("Expected right most child to be %d, but got %d", expectedRightChildPtr, ch)
		}

		// ensure sibling pointer is updated
		if binary.LittleEndian.Uint32(leftNode[43:47]) != binary.LittleEndian.Uint32((*rightNodeRightSibFr)[1:5]) {
			t.Fatalf("Expected left node's right sibling pointer to be %d but got %d.", binary.LittleEndian.Uint32((*rightNodeRightSibFr)[1:5]), binary.LittleEndian.Uint32(leftNode[43:47]))
		}

		if binary.LittleEndian.Uint32((*rightNodeRightSibFr)[47:51]) != binary.LittleEndian.Uint32(leftNode[1:5]) {
			t.Fatalf("Expected rightNodeRightSibling's left sibling to be %d, but got %d", binary.LittleEndian.Uint32(leftNode[1:5]), binary.LittleEndian.Uint32((*rightNodeRightSibFr)[47:51]))
		}

		// ensure items in left node are ordered and none is missing
		var cellOff uint32
		var prevKey []byte
		for i := range itemCount {
			cellOff = binary.LittleEndian.Uint32(leftNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+1 : pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+5])
			kLen := binary.LittleEndian.Uint32(leftNode[cellOff+1 : cellOff+5])
			currKey := leftNode[cellOff+13 : cellOff+13+kLen]
			currPtr := binary.LittleEndian.Uint32(leftNode[cellOff+9 : cellOff+13])

			if bytes.Compare(currKey, prevKey) == -1 {
				t.Fatalf("Expected curr key %v to be greater than previous key %v", currKey, prevKey)
			}

			if currPtr != test.expectedMergedNodePtrs[i] {
				t.Fatalf("Expected child pointer at idx %d to be %d but got %d", i, test.expectedMergedNodePtrs[i], currPtr)
			}

			if !helpers.BitIsSet(&leftNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)], pgr.HasChildPtr) {
				t.Fatalf("Expected \"hasChildPtr\" flag in item at idx %d to be set", i)
			}

			prevKey = make([]byte, kLen)
			copy(prevKey, currKey)
		}
	}
}

func TestMergeLeafRightToLeft(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	// example nodes before merge (order=2)
	//		           +-------------+
	//		           | nationality |
	//		           +-------------+
	//	                  /               \
	//                       /	           \
	//                      /                   \
	// +------------+----------+              +----------+
	// |  age       | country  |              |  office  |
	// +------------+----------+<------------>+----------+
	// |  twenty    |  U.S.A   |              |    aws   |
	// +------------+----------+              +----------+
	sepKey := []byte("nationality")
	leftKeys := [][]byte{[]byte("age"), []byte("country")}
	leftVals := [][]byte{[]byte("twenty"), []byte("U.S.A")}
	leftNode := createTestLeafNode(35, leftKeys, leftVals)

	leftNodeLeftSibling, err := bp.buffermanager.NewFrame(true, false)
	if err != nil {
		t.Fatalf("Unable to create new frame: %s", err)
	}

	rightKeys := [][]byte{
		[]byte("office"),
	}
	rightVals := [][]byte{[]byte("aws")}
	rightNode := createTestLeafNode(45, rightKeys, rightVals)
	rightNodeRightSibling, err := bp.buffermanager.NewFrame(true, false)
	if err != nil {
		t.Fatalf("Unable to create new frame: %s", err)
	}

	// set sibling pointers
	binary.LittleEndian.PutUint32(leftNode[43:47], binary.LittleEndian.Uint32(rightNode[1:5]))
	binary.LittleEndian.PutUint32(rightNode[47:51], binary.LittleEndian.Uint32(leftNode[1:5]))

	rightNodeRightSibling.Acquire(false)
	rightNodeRightSibFr, _, err := rightNodeRightSibling.RawBufferSlice()
	if err != nil {
		t.Fatalf("Unable to get frame buffer: %s", err)
	}
	binary.LittleEndian.PutUint32((*rightNodeRightSibFr)[47:51], binary.LittleEndian.Uint32(rightNode[1:5]))
	copy(rightNode[43:47], (*rightNodeRightSibFr)[1:5])
	rightNodeRightSibling.Release(false)
	rightNodeRightSibling.Unreference()

	leftNodeLeftSibling.Acquire(false)
	leftNodeLeftSibFr, _, err := leftNodeLeftSibling.RawBufferSlice()
	if err != nil {
		t.Fatalf("Unable to get frame buffer: %s", err)
	}
	binary.LittleEndian.PutUint32((*leftNodeLeftSibFr)[43:47], binary.LittleEndian.Uint32(leftNode[1:5]))
	copy(leftNode[47:51], (*leftNodeLeftSibFr)[1:5])
	leftNodeLeftSibling.Release(false)
	leftNodeLeftSibling.Unreference()

	newSepKey, err := bp.merge(&leftNode, &rightNode, sepKey, false)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if newSepKey != nil {
		t.Fatalf("Expected no new separator key, got %v", newSepKey)
	}

	// example expected node after merge; (order=2)

	// +------------+----------+----------+
	// |  age       | country  |  office  |
	// +------------+----------+----------+
	// |  twenty    |  U.S.A   |   aws    |
	// +------------+----------+----------+

	// check that right node is marked for deletion and dirty
	markedDead := helpers.BitIsSet(&rightNode[0], pgr.Dead)
	if !markedDead {
		t.Fatalf("Expected merged node to be marked as dead.")
	}

	rightNodeDirty := helpers.BitIsSet(&rightNode[0], pgr.Dirty)
	if !rightNodeDirty {
		t.Fatalf("Expected right node to be marked dirty")
	}

	leftNodeDirty := helpers.BitIsSet(&leftNode[0], pgr.Dirty)
	if !leftNodeDirty {
		t.Fatalf("Expected left node to be marked dirty")
	}

	itemCount := binary.LittleEndian.Uint32(leftNode[17:21])
	expectedItemCount := len(leftKeys) + len(rightKeys)
	if itemCount != uint32(expectedItemCount) {
		t.Fatalf("Expected number of items in new node to be %d but got %d", expectedItemCount, itemCount)
	}

	// ensure sibling pointer is updated on the left node
	if binary.LittleEndian.Uint32(leftNode[43:47]) != binary.LittleEndian.Uint32((*rightNodeRightSibFr)[1:5]) {
		t.Fatalf("Expected left node's right sibling pointer to be %d but got %d.", binary.LittleEndian.Uint32((*rightNodeRightSibFr)[1:5]), binary.LittleEndian.Uint32(leftNode[43:47]))
	}

	if binary.LittleEndian.Uint32((*rightNodeRightSibFr)[47:51]) != binary.LittleEndian.Uint32(leftNode[1:5]) {
		t.Fatalf("Expected rightNodeRightSibling's left sibling to be %d, but got %d", binary.LittleEndian.Uint32(leftNode[1:5]), binary.LittleEndian.Uint32((*rightNodeRightSibFr)[47:51]))
	}

	// ensure items in left node are ordered and none is missing
	var cellOff uint32
	var prevKey []byte
	for i := range itemCount {
		cellOff = binary.LittleEndian.Uint32(leftNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+1 : pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+5])
		kLen := binary.LittleEndian.Uint32(leftNode[cellOff+1 : cellOff+5])
		currKey := leftNode[cellOff+13 : cellOff+13+kLen]

		if bytes.Compare(currKey, prevKey) == -1 {
			t.Fatalf("Expected curr key %v to be greater than previous key %v", currKey, prevKey)
		}

		prevKey = make([]byte, kLen)
		copy(prevKey, currKey)
	}
}

func TestMergeInternalLeftToRight(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	tests := []struct {
		name                   string
		leftNodeKeys           [][]byte
		leftNodePtrs           []uint32
		leftNodePid            uint32
		rightNodeKeys          [][]byte
		rightNodePtrs          []uint32
		rightNodePid           uint32
		sepKey                 []byte
		expectedMergedNodeKeys [][]byte
		expectedMergedNodePtrs []uint32
	}{
		{
			name:                   "normal underflowed node",
			leftNodeKeys:           [][]byte{[]byte("age")},
			leftNodePtrs:           []uint32{25, 99},
			leftNodePid:            35,
			rightNodeKeys:          [][]byte{[]byte("nationality"), []byte("office")},
			rightNodePtrs:          []uint32{510, 10, 30},
			rightNodePid:           55,
			sepKey:                 []byte("country"),
			expectedMergedNodeKeys: [][]byte{[]byte("age"), []byte("country"), []byte("nationality"), []byte("office")},
			expectedMergedNodePtrs: []uint32{25, 99, 510, 10, 30},
		},
	}

	for _, test := range tests {
		// example nodes before merge (order=2)
		//		           +-------------+
		//		           |   country   |
		//		           +-------------+
		//	                  /               \
		//                       /	           \
		//                      /                   \
		// +--------+----------+------+         +---------------+--------+------+
		// |  age   |          |      |         |  nationality  | office |      |
		// +--------+----------+  99  +<------->+---------------+--------+  30  +
		// |  25    |          |      |         |    510        |   10   |      |
		// +--------+----------+------+         +---------------+--------+------+
		leftNode := createTestInternalNode(test.leftNodePid, test.leftNodeKeys, test.leftNodePtrs)

		leftNodeLeftSibling, err := bp.buffermanager.NewFrame(true, false)
		if err != nil {
			t.Fatalf("Unable to create new frame: %s", err)
		}

		rightNode := createTestInternalNode(test.rightNodePid, test.rightNodeKeys, test.rightNodePtrs)
		rightNodeRightSibling, err := bp.buffermanager.NewFrame(true, false)
		if err != nil {
			t.Fatalf("Unable to create new frame: %s", err)
		}

		// set sibling pointers
		binary.LittleEndian.PutUint32(leftNode[43:47], test.rightNodePid)
		binary.LittleEndian.PutUint32(rightNode[47:51], test.leftNodePid)

		rightNodeRightSibling.Acquire(false)
		rightNodeRightSibFr, _, err := rightNodeRightSibling.RawBufferSlice()
		if err != nil {
			t.Fatalf("Unable to get frame buffer: %s", err)
		}
		binary.LittleEndian.PutUint32((*rightNodeRightSibFr)[47:51], test.rightNodePid)
		copy(rightNode[43:47], (*rightNodeRightSibFr)[1:5])
		rightNodeRightSibling.Release(false)
		rightNodeRightSibling.Unreference()

		leftNodeLeftSibling.Acquire(false)
		leftNodeLeftSibFr, _, err := leftNodeLeftSibling.RawBufferSlice()
		if err != nil {
			t.Fatalf("Unable to get frame buffer: %s", err)
		}
		binary.LittleEndian.PutUint32((*leftNodeLeftSibFr)[43:47], test.leftNodePid)
		copy(leftNode[47:51], (*leftNodeLeftSibFr)[1:5])
		leftNodeLeftSibling.Release(false)
		leftNodeLeftSibling.Unreference()

		// merge nodes
		newSepKey, err := bp.merge(&leftNode, &rightNode, test.sepKey, true)
		if err != nil {
			t.Fatalf("Expected no error, got %s", err.Error())
		}

		if newSepKey != nil {
			t.Fatalf("Expected no new separator key, got %v", newSepKey)
		}

		// example expected node after merge; (order=2)
		// +--------+----------+-----------------+------------------+
		// |  age   | country  |  nationality    |  office  |       |
		// +--------+----------+-----------------+----------+   30  +
		// |  25    |   99     |     510         |    10    |       |
		// +--------+----------+-----------------+----------+-------+

		// check that left node is marked for deletion
		markedDead := helpers.BitIsSet(&leftNode[0], pgr.Dead)
		if !markedDead {
			t.Fatalf("Expected merged node to be marked as dead.")
		}

		// check that right node is marked dirty
		rightNodeDirty := helpers.BitIsSet(&rightNode[0], pgr.Dirty)
		if !rightNodeDirty {
			t.Fatalf("Expected right node to be marked dirty")
		}

		// ensure itemcount is correct
		itemCount := binary.LittleEndian.Uint32(rightNode[17:21])
		expectedItemCount := len(test.leftNodeKeys) + len(test.rightNodeKeys) + 1
		if itemCount != uint32(expectedItemCount) {
			t.Fatalf("Expected number of items in right node to be %d but got %d", expectedItemCount, itemCount)
		}

		// check right most child
		expectedRightChildPtr := test.rightNodePtrs[len(test.rightNodePtrs)-1]
		if ch := binary.LittleEndian.Uint32(rightNode[39:43]); ch != expectedRightChildPtr {
			t.Fatalf("Expected right most child to be %d, but got %d", expectedRightChildPtr, ch)
		}

		// ensure sibling pointer is updated
		if binary.LittleEndian.Uint32(rightNode[47:51]) != binary.LittleEndian.Uint32((*leftNodeLeftSibFr)[1:5]) {
			t.Fatalf("Expected right node's left sibling pointer to be %d but got %d.", binary.LittleEndian.Uint32((*leftNodeLeftSibFr)[1:5]), binary.LittleEndian.Uint32(rightNode[47:51]))
		}

		if binary.LittleEndian.Uint32((*leftNodeLeftSibFr)[43:47]) != binary.LittleEndian.Uint32(rightNode[1:5]) {
			t.Fatalf("Expected leftNodeLeftSibling's right sibling to be %d, but got %d", binary.LittleEndian.Uint32(rightNode[1:5]), binary.LittleEndian.Uint32((*leftNodeLeftSibFr)[47:51]))
		}

		// ensure items in right node are ordered and none is missing
		var cellOff uint32
		var prevKey []byte
		for i := range itemCount {
			cellOff = binary.LittleEndian.Uint32(rightNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+1 : pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+5])
			kLen := binary.LittleEndian.Uint32(rightNode[cellOff+1 : cellOff+5])
			currKey := rightNode[cellOff+13 : cellOff+13+kLen]
			currPtr := binary.LittleEndian.Uint32(rightNode[cellOff+9 : cellOff+13])

			if bytes.Compare(currKey, prevKey) == -1 {
				t.Fatalf("Expected curr key %v to be greater than previous key %v", currKey, prevKey)
			}

			if currPtr != test.expectedMergedNodePtrs[i] {
				t.Fatalf("Expected child pointer at idx %d to be %d but got %d", i, test.expectedMergedNodePtrs[i], currPtr)
			}

			if !helpers.BitIsSet(&rightNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)], pgr.HasChildPtr) {
				t.Fatalf("Expected \"hasChildPtr\" flag to be set at index %d", i)
			}

			prevKey = make([]byte, kLen)
			copy(prevKey, currKey)
		}
	}
}

func TestMergeLeafLeftToRight(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	// example nodes before merge (order=2)
	//              +-------------+
	//              | nationality |
	//              +-------------+
	//             /               \
	//            /                 \
	//           /                   \
	// +------------+              +-------------+----------+
	// |  age       |              | nationality |  office  |
	// +------------+<------------>+-------------+----------+
	// |  twenty    |              | american    |    aws   |
	// +------------+              +-------------+----------+
	sepKey := []byte("nationality")
	leftKeys := [][]byte{[]byte("age")}
	leftVals := [][]byte{[]byte("twenty")}
	leftNode := createTestLeafNode(35, leftKeys, leftVals)

	leftNodeLeftSibling, err := bp.buffermanager.NewFrame(true, false)
	if err != nil {
		t.Fatalf("Unable to create new frame: %s", err)
	}

	rightKeys := [][]byte{
		[]byte("nationality"),
		[]byte("office"),
	}
	rightVals := [][]byte{[]byte("american"), []byte("aws")}
	rightNode := createTestLeafNode(45, rightKeys, rightVals)
	rightNodeRightSibling, err := bp.buffermanager.NewFrame(true, false)
	if err != nil {
		t.Fatalf("Unable to create new frame: %s", err)
	}

	// set sibling pointers
	binary.LittleEndian.PutUint32(leftNode[43:47], binary.LittleEndian.Uint32(rightNode[1:5]))
	binary.LittleEndian.PutUint32(rightNode[47:51], binary.LittleEndian.Uint32(leftNode[1:5]))

	rightNodeRightSibling.Acquire(false)
	rightNodeRightSibFr, _, err := rightNodeRightSibling.RawBufferSlice()
	if err != nil {
		t.Fatalf("Unable to get frame buffer: %s", err)
	}
	binary.LittleEndian.PutUint32((*rightNodeRightSibFr)[47:51], binary.LittleEndian.Uint32(rightNode[1:5]))
	copy(rightNode[43:47], (*rightNodeRightSibFr)[1:5])
	rightNodeRightSibling.Release(false)
	rightNodeRightSibling.Unreference()

	leftNodeLeftSibling.Acquire(false)
	leftNodeLeftSibFr, _, err := leftNodeLeftSibling.RawBufferSlice()
	if err != nil {
		t.Fatalf("Unable to get frame buffer: %s", err)
	}
	binary.LittleEndian.PutUint32((*leftNodeLeftSibFr)[43:47], binary.LittleEndian.Uint32(leftNode[1:5]))
	copy(leftNode[47:51], (*leftNodeLeftSibFr)[1:5])
	leftNodeLeftSibling.Release(false)
	leftNodeLeftSibling.Unreference()

	newSepKey, err := bp.merge(&leftNode, &rightNode, sepKey, true)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if newSepKey != nil {
		t.Fatalf("Expected no new separator key, got %v", newSepKey)
	}

	// example expected node after merge; (order=2)

	// +------------+-------------+----------+
	// |  age       | nationality  |  office  |
	// +------------+-------------+----------+
	// |  twenty    |  american   |   aws    |
	// +------------+-------------+----------+

	// check that left node is marked for deletion
	markedDead := helpers.BitIsSet(&leftNode[0], pgr.Dead)
	if !markedDead {
		t.Fatalf("Expected merged node to be marked as dead.")
	}

	// check that the riht node is marked dirty
	rightNodeDirty := helpers.BitIsSet(&rightNode[0], pgr.Dirty)
	if !rightNodeDirty {
		t.Fatalf("Expected right node to be marked dirty")
	}

	itemCount := binary.LittleEndian.Uint32(rightNode[17:21])
	expectedItemCount := len(leftKeys) + len(rightKeys)
	if itemCount != uint32(expectedItemCount) {
		t.Fatalf("Expected number of items in right node to be %d but got %d", expectedItemCount, itemCount)
	}

	// ensure sibling pointers are updated
	if binary.LittleEndian.Uint32(rightNode[47:51]) != binary.LittleEndian.Uint32((*leftNodeLeftSibFr)[1:5]) {
		t.Fatalf("Expected right node's left sibling pointer to be %d but got %d.", binary.LittleEndian.Uint32((*leftNodeLeftSibFr)[1:5]), binary.LittleEndian.Uint32(rightNode[47:51]))
	}

	if binary.LittleEndian.Uint32((*leftNodeLeftSibFr)[43:47]) != binary.LittleEndian.Uint32(rightNode[1:5]) {
		t.Fatalf("Expected leftNodeLeftSibling's right sibling to be %d, but got %d", binary.LittleEndian.Uint32(rightNode[1:5]), binary.LittleEndian.Uint32((*leftNodeLeftSibFr)[47:51]))
	}

	// ensure items in right node are ordered and none is missing
	var cellOff uint32
	var prevKey []byte
	for i := range itemCount {
		cellOff = binary.LittleEndian.Uint32(rightNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+1 : pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+5])
		kLen := binary.LittleEndian.Uint32(rightNode[cellOff+1 : cellOff+5])
		currKey := rightNode[cellOff+13 : cellOff+13+kLen]

		if bytes.Compare(currKey, prevKey) == -1 {
			t.Fatalf("Expected curr key %v to be greater than previous key %v", currKey, prevKey)
		}

		prevKey = make([]byte, kLen)
		copy(prevKey, currKey)
	}
}

func TestRebalanceInternalNode(t *testing.T) {
	tests := []struct {
		name          string
		leftNodeKeys  [][]byte
		leftNodePtrs  []uint32
		rightNodeKeys [][]byte
		rightNodePtrs []uint32
		seperatorKey  []byte
		leftMerge     bool
	}{
		{
			name: "right_node_underflow",
			leftNodeKeys: [][]byte{
				[]byte("age"),
				[]byte("code"),
				[]byte("country"),
			},
			leftNodePtrs: []uint32{25, 66, 88, 99},
			rightNodeKeys: [][]byte{
				[]byte("name"),
			},
			rightNodePtrs: []uint32{150, 250},
			seperatorKey:  []byte("marital status"),
			leftMerge:     false,
		},
		{
			name: "left_node_underflow",
			leftNodeKeys: [][]byte{
				[]byte("age"),
			},
			leftNodePtrs: []uint32{25, 66},
			rightNodeKeys: [][]byte{
				[]byte("code"),
				[]byte("country"),
				[]byte("email"),
			},
			rightNodePtrs: []uint32{178, 250, 354, 456},
			seperatorKey:  []byte("city"),
			leftMerge:     true,
		},
		{
			name: "right_node_underflow_edge_case",
			rightNodeKeys: [][]byte{
				[]byte("user:3492fa9c941a17684360f1691b9e83f6:profile"),
			},
			rightNodePtrs: []uint32{27, 29},
			leftNodeKeys: [][]byte{
				[]byte("user:0bea13f848993c239eae636f630caeef:profile"),
				[]byte("user:1023f73dc1137bba14f4a61b05536f92:profile"),
				[]byte("user:15abbb159b0112d6b17a4f30984bce87:profile"),
				[]byte("user:18208b6baed1be771e0f498bffaabdc5:profile"),
			},
			leftNodePtrs: []uint32{43, 11, 56, 16, 50},
			seperatorKey: []byte("user:1e3189549cf15336c6cc8cc020884999:profile"),
			leftMerge:    true,
		},
		{
			name: "left_node_underflow_edge_case",
			leftNodeKeys: [][]byte{
				[]byte("user:72a096195421dd8d3e7ac940b0623231:profile"),
			},
			leftNodePtrs: []uint32{19, 34},
			rightNodeKeys: [][]byte{
				[]byte("user:823ba7b74c0f6687789a9a8b04bdcd54:profile"),
				[]byte("user:888f1185deae8949e380ed925d47a7e4:profile"),
				[]byte("user:8d82934ba30c43fc6f0edc65ab3ce23b:profile"),
			},
			rightNodePtrs: []uint32{6, 46, 22, 55},
			seperatorKey:  []byte("user:7f51fcbb33d64c61c734604c935fd420:profile"),
			leftMerge:     true,
		},
	}

	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	for _, test := range tests {
		fmt.Printf("RUNNING -=======> %s\n", test.name)
		leftNode := createTestInternalNode(25, test.leftNodeKeys, test.leftNodePtrs)
		rightNode := createTestInternalNode(45, test.rightNodeKeys, test.rightNodePtrs)

		fmt.Printf("BEFORE REBALANCE----\n")
		fmt.Printf("LEFT NODE\n")
		fmt.Println(printNodeContent(&leftNode))
		fmt.Printf("RIGHT NODE\n")
		fmt.Println(printNodeContent(&rightNode))

		leftSibling, err := bp.buffermanager.NewFrame(true, false)
		if err != nil {
			t.Fatalf("Unable to create new frame: %s", err)
		}

		rightSibling, err := bp.buffermanager.NewFrame(true, false)
		if err != nil {
			t.Fatalf("Unable to create new frame: %s", err)
		}

		// set sibling pointers
		copy(leftNode[43:47], rightNode[1:5])
		copy(rightNode[47:51], leftNode[1:5])

		leftSibling.Acquire(false)
		leftSibFr, _, err := leftSibling.RawBufferSlice()
		if err != nil {
			t.Fatalf("Unable to get frame buffer: %s", err.Error())
		}
		copy(leftNode[47:51], (*leftSibFr)[1:5])
		copy((*leftSibFr)[43:47], leftNode[1:5])
		leftSibling.Release(false)
		leftSibling.Unreference()

		rightSibling.Acquire(false)
		rightSiblingFr, _, err := rightSibling.RawBufferSlice()
		if err != nil {
			t.Fatalf("Unable to get frame buffer: %s", err.Error())
		}
		copy(rightNode[43:47], (*rightSiblingFr)[1:5])
		copy((*rightSiblingFr)[47:51], rightNode[1:5])
		rightSibling.Release(false)
		rightSibling.Unreference()

		newSepKey, err := bp.merge(&leftNode, &rightNode, test.seperatorKey, test.leftMerge)
		if err != nil {
			t.Fatalf("(%s) merge failed: %s", test.name, err.Error())
		}

		fmt.Printf("AFTER REBALANCE----\n")
		fmt.Printf("LEFT NODE\n")
		fmt.Println(printNodeContent(&leftNode))
		fmt.Printf("RIGHT NODE\n")
		fmt.Println(printNodeContent(&rightNode))

		fmt.Printf("NEW SEPERATOR KEY --> %s\n", newSepKey)

		if newSepKey == nil {
			t.Fatalf("(%s) Expected nodes to be rebalanced, got nil new seperator key", test.name)
		}

		leftNodeItemCount := binary.LittleEndian.Uint32(leftNode[17:21])
		rightNodeItemCount := binary.LittleEndian.Uint32(rightNode[17:21])

		if leftNodeItemCount < pgr.ORDER {
			t.Fatalf("(%s) left node still underflown after rebalancing - %d", test.name, leftNodeItemCount)
		}

		if rightNodeItemCount < pgr.ORDER {
			t.Fatalf("(%s) right node still underflown after rebalancing - %d", test.name, rightNodeItemCount)
		}

		if s := rightNodeItemCount + leftNodeItemCount; s > (pgr.ORDER*2)*2 {
			t.Fatalf("(%s) Number of items on both nodes is %d exceeding allowable max.", test.name, s)
		}

		// check separator key
		if bytes.Equal(newSepKey, test.seperatorKey) {
			t.Fatalf("(%s) Expected different separator key, got %s", test.name, newSepKey)
		}

		var cellOff uint32
		var prevKey []byte
		// check items in left node
		for i := range leftNodeItemCount {
			// ensure  'hasChildPtr' flag is set
			if fl := helpers.BitIsSet(&leftNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)], pgr.HasChildPtr); !fl {
				t.Fatalf("(%s) Expected 'hasChildPtr' flag to be set on left node at index: %d", test.name, i)
			}

			cellOff = binary.LittleEndian.Uint32(leftNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+1 : pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+5])
			kLen := binary.LittleEndian.Uint32(leftNode[cellOff+1 : cellOff+5])
			currKey := leftNode[cellOff+13 : cellOff+13+kLen]

			if bytes.Compare(currKey, prevKey) == -1 {
				t.Fatalf("(%s) Expected curr key %v to be greater than previous key %v", test.name, currKey, prevKey)
			}

			if c := bytes.Compare(newSepKey, currKey); c <= 0 {
				t.Fatalf("(%s) key \"%s\" in left node is greater than or equal to seperator key \"%s\"", test.name, currKey, newSepKey)
			}

			prevKey = make([]byte, kLen)
			copy(prevKey, currKey)
		}

		// check items in right node
		cellOff = 0
		clear(prevKey)
		for i := range rightNodeItemCount {
			if fl := helpers.BitIsSet(&rightNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)], pgr.HasChildPtr); !fl {
				t.Fatalf("(%s) Expected 'hasChildPtr' flag to be set on right node at index: %d", test.name, i)
			}

			cellOff = binary.LittleEndian.Uint32(rightNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+1 : pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+5])
			kLen := binary.LittleEndian.Uint32(rightNode[cellOff+1 : cellOff+5])
			currKey := rightNode[cellOff+13 : cellOff+13+kLen]

			if bytes.Compare(currKey, prevKey) == -1 {
				t.Fatalf("(%s) Expected curr key %v to be greater than previous key %v", test.name, currKey, prevKey)
			}

			if c := bytes.Compare(newSepKey, currKey); c == 1 {
				t.Fatalf("(%s) key \"%s\" in right node is less than the seperator key \"%s\"", test.name, currKey, newSepKey)
			}

			prevKey = make([]byte, kLen)
			copy(prevKey, currKey)
		}

		// ensure sibling pointers remain the same
		// left node
		if !bytes.Equal(leftNode[43:47], rightNode[1:5]) {
			t.Fatalf("(%s) Expected left node's right sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32(rightNode[1:5]), binary.LittleEndian.Uint32(leftNode[43:47]))
		}

		if !bytes.Equal(leftNode[47:51], (*leftSibFr)[1:5]) {
			t.Fatalf("(%s) Expected left node's left sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32((*leftSibFr)[1:5]), binary.LittleEndian.Uint32(leftNode[47:51]))
		}

		// left sibling
		if !bytes.Equal((*leftSibFr)[43:47], leftNode[1:5]) {
			t.Fatalf("(%s) Expected leftSibling node's right sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32(leftNode[1:5]), binary.LittleEndian.Uint32((*leftSibFr)[43:47]))
		}

		// right node
		if !bytes.Equal(rightNode[47:51], leftNode[1:5]) {
			t.Fatalf("(%s) Expected right node's left sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32(leftNode[1:5]), binary.LittleEndian.Uint32(rightNode[47:51]))
		}

		if !bytes.Equal(rightNode[43:47], (*rightSiblingFr)[1:5]) {
			t.Fatalf("(%s) Expected right node's right sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32((*rightSiblingFr)[1:5]), binary.LittleEndian.Uint32(rightNode[43:47]))
		}

		//  right sibling
		if !bytes.Equal((*rightSiblingFr)[47:51], rightNode[1:5]) {
			t.Fatalf("(%s) Expected rightSibling node's left sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32(rightNode[1:5]), binary.LittleEndian.Uint32((*rightSiblingFr)[47:51]))
		}
	}
}

func TestRebalanceLeafNode(t *testing.T) {
	tests := []struct {
		name          string
		leftNodeKeys  [][]byte
		leftNodeVals  [][]byte
		rightNodeKeys [][]byte
		rightNodeVals [][]byte
		seperatorKey  []byte
	}{
		{
			name: "right_node_underflow",
			leftNodeKeys: [][]byte{
				[]byte("age"),
				[]byte("code"),
				[]byte("country"),
			},
			leftNodeVals: [][]byte{
				[]byte("twenty"),
				[]byte("U.S.A"),
				[]byte("united states"),
			},
			rightNodeKeys: [][]byte{
				[]byte("name"),
			},
			rightNodeVals: [][]byte{
				[]byte("Ben"),
			},
			seperatorKey: []byte("marital status"),
		},
		{
			name: "left_node_underflow",
			leftNodeKeys: [][]byte{
				[]byte("age"),
			},
			leftNodeVals: [][]byte{
				[]byte("twenty"),
			},
			rightNodeKeys: [][]byte{
				[]byte("code"),
				[]byte("country"),
				[]byte("email"),
			},
			rightNodeVals: [][]byte{
				[]byte("U.S.A"),
				[]byte("unites states"),
				[]byte("ian@db.com"),
			},
			seperatorKey: []byte("city"),
		},
	}

	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	for _, test := range tests {
		leftNode := createTestLeafNode(25, test.leftNodeKeys, test.leftNodeVals)
		rightNode := createTestLeafNode(45, test.rightNodeKeys, test.rightNodeVals)

		fmt.Printf("BEFORE REBALANCE----\n")
		fmt.Printf("LEFT NODE\n")
		fmt.Println(printNodeContent(&leftNode))
		fmt.Printf("RIGHT NODE\n")
		fmt.Println(printNodeContent(&rightNode))

		leftSibling, err := bp.buffermanager.NewFrame(true, false)
		if err != nil {
			t.Fatalf("Unable to create new frame: %s", err)
		}

		rightSibling, err := bp.buffermanager.NewFrame(true, false)
		if err != nil {
			t.Fatalf("Unable to create new frame: %s", err)
		}

		// set sibling pointers
		copy(leftNode[43:47], rightNode[1:5])
		copy(rightNode[47:51], leftNode[1:5])

		leftSibling.Acquire(false)
		leftSibFr, _, err := leftSibling.RawBufferSlice()
		if err != nil {
			t.Fatalf("Unable to get frame buffer: %s", err.Error())
		}
		copy(leftNode[47:51], (*leftSibFr)[1:5])
		copy((*leftSibFr)[43:47], leftNode[1:5])
		leftSibling.Release(false)
		leftSibling.Unreference()

		rightSibling.Acquire(false)
		rightSiblingFr, _, err := rightSibling.RawBufferSlice()
		if err != nil {
			t.Fatalf("Unable to get frame buffer: %s", err.Error())
		}
		copy(rightNode[43:47], (*rightSiblingFr)[1:5])
		copy((*rightSiblingFr)[47:51], rightNode[1:5])
		rightSibling.Release(false)
		rightSibling.Unreference()

		newSepKey, err := bp.merge(&leftNode, &rightNode, test.seperatorKey, false)
		if err != nil {
			t.Fatalf("(%s) merge failed: %s", test.name, err.Error())
		}

		fmt.Printf("AFTER REBALANCE----\n")
		fmt.Printf("LEFT NODE\n")
		fmt.Println(printNodeContent(&leftNode))
		fmt.Printf("RIGHT NODE\n")
		fmt.Println(printNodeContent(&rightNode))

		fmt.Printf("NEW SEPERATOR KEY --> %s\n", newSepKey)

		if newSepKey == nil {
			t.Fatalf("(%s) Expected nodes to be rebalanced, got nil new seperator key", test.name)
		}

		leftNodeItemCount := binary.LittleEndian.Uint32(leftNode[17:21])
		rightNodeItemCount := binary.LittleEndian.Uint32(rightNode[17:21])

		if leftNodeItemCount < pgr.ORDER {
			t.Fatalf("(%s) left node still underflown after rebalancing - %d", test.name, leftNodeItemCount)
		}

		if rightNodeItemCount < pgr.ORDER {
			t.Fatalf("(%s) right node still underflown after rebalancing - %d", test.name, rightNodeItemCount)
		}

		if s := rightNodeItemCount + leftNodeItemCount; s > (pgr.ORDER*2)*2 {
			t.Fatalf("(%s) Number of items on both nodes is %d exceeding allowable max.", test.name, s)
		}

		// check separator key
		if bytes.Equal(newSepKey, test.seperatorKey) {
			t.Fatalf("(%s) Expected different separator key, got %s", test.name, newSepKey)
		}

		var cellOff uint32
		var prevKey []byte
		// check items in left node
		for i := range leftNodeItemCount {
			cellOff = binary.LittleEndian.Uint32(leftNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+1 : pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+5])
			kLen := binary.LittleEndian.Uint32(leftNode[cellOff+1 : cellOff+5])
			currKey := leftNode[cellOff+13 : cellOff+13+kLen]

			if bytes.Compare(currKey, prevKey) == -1 {
				t.Fatalf("(%s) Expected curr key %v to be greater than previous key %v", test.name, currKey, prevKey)
			}

			if c := bytes.Compare(newSepKey, currKey); c <= 0 {
				t.Fatalf("(%s) key \"%s\" in left node is greater than or equal to seperator key \"%s\"", test.name, currKey, newSepKey)
			}

			prevKey = make([]byte, kLen)
			copy(prevKey, currKey)
		}

		// check items in right node
		cellOff = 0
		clear(prevKey)
		for i := range rightNodeItemCount {
			cellOff = binary.LittleEndian.Uint32(rightNode[pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+1 : pgr.HEADER_SIZE_BYTES+(pgr.CELL_POINTER_SIZE_BYTE*i)+5])
			kLen := binary.LittleEndian.Uint32(rightNode[cellOff+1 : cellOff+5])
			currKey := rightNode[cellOff+13 : cellOff+13+kLen]

			if bytes.Compare(currKey, prevKey) == -1 {
				t.Fatalf("(%s) Expected curr key %v to be greater than previous key %v", test.name, currKey, prevKey)
			}

			if c := bytes.Compare(newSepKey, currKey); c == 1 {
				t.Fatalf("(%s) key \"%s\" in right node is less than the seperator key \"%s\"", test.name, currKey, newSepKey)
			}

			prevKey = make([]byte, kLen)
			copy(prevKey, currKey)
		}

		// ensure sibling pointers remain the same
		// left node
		if !bytes.Equal(leftNode[43:47], rightNode[1:5]) {
			t.Fatalf("(%s) Expected left node's right sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32(rightNode[1:5]), binary.LittleEndian.Uint32(leftNode[43:47]))
		}

		if !bytes.Equal(leftNode[47:51], (*leftSibFr)[1:5]) {
			t.Fatalf("(%s) Expected left node's left sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32((*leftSibFr)[1:5]), binary.LittleEndian.Uint32(leftNode[47:51]))
		}

		// left sibling
		if !bytes.Equal((*leftSibFr)[43:47], leftNode[1:5]) {
			t.Fatalf("(%s) Expected leftSibling node's right sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32(leftNode[1:5]), binary.LittleEndian.Uint32((*leftSibFr)[43:47]))
		}

		// right node
		if !bytes.Equal(rightNode[47:51], leftNode[1:5]) {
			t.Fatalf("(%s) Expected right node's left sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32(leftNode[1:5]), binary.LittleEndian.Uint32(rightNode[47:51]))
		}

		if !bytes.Equal(rightNode[43:47], (*rightSiblingFr)[1:5]) {
			t.Fatalf("(%s) Expected right node's right sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32((*rightSiblingFr)[1:5]), binary.LittleEndian.Uint32(rightNode[43:47]))
		}

		//  right sibling
		if !bytes.Equal((*rightSiblingFr)[47:51], rightNode[1:5]) {
			t.Fatalf("(%s) Expected rightSibling node's left sibling pointer to remain %d, instead got %d", test.name, binary.LittleEndian.Uint32(rightNode[1:5]), binary.LittleEndian.Uint32((*rightSiblingFr)[47:51]))
		}

	}
}

func TestInsertToBpTree(t *testing.T) {
	lgr := logger.NewLogger(tempLogFile(t), logger.DEBUG, 100)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	tests, err := generateKV((pgr.ORDER * 2) * 25000) // 100k values
	if err != nil {
		panic(err.Error())
	}

	// fmt.Printf("generated keys and vals=======\n")
	// for i, k := range tests {
	// 	fmt.Printf("%d. %s: %s\n", i, k.key, k.val)
	// }

	// add keys
	for _, test := range tests {
		fmt.Println("============================================================================================================================================================================================")
		t.Logf("Inserting key: \"%s\"\n", test.key)
		err := bp.Insert(test.key, test.val)
		if err != nil {
			t.Fatalf("Error while inserting key \"%s\": %v", test.key, err.Error())
		}
	}

	// retrieve keys
	var retrievedVal []byte
	for _, test := range tests {
		retrievedVal, err = bp.Get(test.key)
		if err != nil {
			t.Fatalf("Error while retrieving key \"%s\": %v", test.key, err.Error())
		}

		if !bytes.Equal(retrievedVal, test.val) {
			t.Fatalf("Expected for key \"%s\" to get value \"%s\" but instead got \"%s\"", test.key, test.val, retrievedVal)
		}

		fmt.Printf("SUCCESS: retrieved key \"%s\" successfully\n", test.key)
	}
}

func TestSingleDeleteFromBpTree(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	tests, err := generateKV((pgr.ORDER * 2) * 10) // 128 values
	if err != nil {
		panic(err.Error())
	}

	// fmt.Printf("generated keys and vals=======\n")
	// for i, k := range tests {
	// 	fmt.Printf("%d. %s: %s\n", i, k.key, k.val)
	// }

	// add keys
	for _, test := range tests {
		fmt.Println("============================================================================================================================================================================================")
		t.Logf("Inserting key: \"%s\"\n", test.key)
		err := bp.Insert(test.key, test.val)
		if err != nil {
			t.Fatalf("Error while inserting key \"%s\": %v", test.key, err.Error())
		}
	}

	// retrieve keys and ensure they were added
	var retrievedVal []byte
	for _, test := range tests {
		retrievedVal, err = bp.Get(test.key)
		if err != nil {
			t.Fatalf("Error while retrieving key \"%s\": %v", test.key, err.Error())
		}

		if !bytes.Equal(retrievedVal, test.val) {
			t.Fatalf("Expected for key \"%s\" to get value \"%s\" but instead got \"%s\"", test.key, test.val, retrievedVal)
		}
	}

	deleteIdx := rand.Intn(len(tests))

	val, err := bp.Delete(tests[int(deleteIdx)].key)
	if err != nil {
		t.Fatalf("Expected no error while deleting key \"%s\", got \"%s\"", tests[int(deleteIdx)].key, err.Error())
	}

	if !bytes.Equal(val, tests[int(deleteIdx)].val) {
		t.Fatalf("Expected deleted val to be %s but got %v", tests[int(deleteIdx)].val, val)
	}

	// try retrieving the node
	fmt.Printf("RETRIEVING KEY AFTER DELETE...\n")
	val, err = bp.Get(tests[int(deleteIdx)].key)
	fmt.Printf("RETRIEVED KEY AFTER DELETE -> %s\n", val)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if val != nil {
		t.Fatalf("Expected no value for deleted key, got %s", val)
	}
}

func TestDeleteFromBpTree(t *testing.T) {
	testIterations := 1
	keysPerTest := 32768
	for i := range testIterations {
		ok := t.Run(fmt.Sprintf("TestDeleteFromBpTree: iteration_%d", i), func(t *testing.T) {
			t.Logf("-------------------------------------")
			t.Logf("Running %s", t.Name())
			t.Logf("-------------------------------------")
			lgr := logger.NewLogger(tempLogFile(t), logger.DEBUG, 50)
			w := wal.NewWal(lgr)
			// initialize pager
			pagr := InitPager(t)

			// initialize buffer manager
			cConfig := buffermanager.CacheConfig{
				CacheSize: 16 * 1024, // 16MB
			}

			buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
			if err != nil {
				pagr.Close()
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
			}

			if buffManager == nil {
				pagr.Close()
				helpers.PrintTestErrorMsg("Expected cache, got nil", t)
			}

			t.Cleanup(func() {
				if buffManager != nil {
					err := buffManager.Close()
					if err != nil {
						helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
					}
					helpers.PrintSuccessMsg("successfully closed buffermanager")
				}
			})

			bp, err := NewBpTree(buffManager, w, lgr)
			if err != nil {
				t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
			}

			if bp == nil {
				t.Fatalf("expected B+ index got nil")
			}

			tests, err := generateKV(keysPerTest)
			if err != nil {
				panic(err.Error())
			}

			fmt.Printf("generated keys and vals=======\n")
			for i, k := range tests {
				fmt.Printf("%d. %s: %s\n", i, k.key, k.val)
			}

			// add keys
			for _, test := range tests {
				fmt.Println("============================================================================================================================================================================================")
				t.Logf("Inserting key: \"%s\"\n", test.key)
				err := bp.Insert(test.key, test.val)
				if err != nil {
					t.Fatalf("Error while inserting key \"%s\": %v", test.key, err.Error())
				}
			}

			fmt.Println("INSERTED ALL KEYS==============================================")

			// retrieve keys and ensure they were added
			var retrievedVal []byte
			for i, test := range tests {
				retrievedVal, err = bp.Get(test.key)
				if err != nil {
					t.Fatalf("Error while retrieving key \"%s\": %v", test.key, err.Error())
				}

				if !bytes.Equal(retrievedVal, test.val) {
					t.Fatalf("%d. Expected for key \"%s\" to get value \"%s\" but instead got \"%s\"", i, test.key, test.val, retrievedVal)
				}
			}

			fmt.Println("ALL KEYS INSERTED SUCCESSFULY===================================================================================")

			midPt := len(tests) / 2

			for i := len(tests) - 1; i > int(midPt); i-- {
				val, err := bp.Delete(tests[i].key)
				if err != nil {
					t.Fatalf("%d. Expected no error while deleting key \"%s\", got \"%s\"", i, tests[i].key, err.Error())
				}

				if !bytes.Equal(val, tests[i].val) {
					t.Fatalf("%d. Expected deleted val to be %s but got %v", i, tests[i].val, val)
				}

				// try retrieving the node
				fmt.Printf("%d. RETRIEVING KEY AFTER DELETE...\n", i)
				val, err = bp.Get(tests[i].key)
				fmt.Printf("%d. RETRIEVED KEY AFTER DELETE -> %s\n", i, val)
				if err != nil {
					t.Fatalf("Expected no error, got %s", err.Error())
				}

				if val != nil {
					t.Fatalf("Expected no value for deleted key, got %s", val)
				}
			}
		})

		if !ok {
			t.FailNow()
		}
	}
}

func TestDefragmentNode(t *testing.T) {
	entries, err := generateSortedKV(5)
	if err != nil {
		t.Fatal(err.Error())
	}

	var keys [][]byte
	var vals [][]byte

	for _, e := range entries {
		keys = append(keys, e.key)
		vals = append(vals, e.val)
	}

	node := createTestFragmentedLeafNode(25, keys, vals)
	// set left sibling, right sibling
	leftSib := 24
	rightSib := 27
	binary.LittleEndian.PutUint32(node[47:51], uint32(leftSib))
	binary.LittleEndian.PutUint32(node[43:47], uint32(rightSib))

	err = defragmentNode(&node)
	if err != nil {
		t.Fatalf("Expected no error, intead got:: %s", err.Error())
	}

	fmt.Println("PRINTING DEFRAGMENTED NODE-->")
	fmt.Println(printNodeContent(&node))

	var expectedCellOff uint32 = pgr.PAGE_SIZE_BYTES - pgr.LOWER_PADDING_BYTES
	for i := range len(keys) {
		cOff := binary.LittleEndian.Uint32(node[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+5])
		kLen := binary.LittleEndian.Uint32(node[cOff+1 : cOff+5])
		vLen := binary.LittleEndian.Uint32(node[cOff+5 : cOff+9])
		cellSize := 13 + kLen + vLen
		expectedCellOff -= cellSize

		if cOff != expectedCellOff {
			t.Fatalf("%d. Expected cell offset of %d but got %d", i, expectedCellOff, cOff)
		}
	}

	if uOff := binary.LittleEndian.Uint32(node[25:29]); uOff != expectedCellOff {
		t.Fatalf("Expected upperoffset to be %d but instead got %d", expectedCellOff, uOff)
	}

	expectedLowOff := pgr.HEADER_SIZE_BYTES + (len(entries) * pgr.CELL_POINTER_SIZE_BYTE)
	if lOff := binary.LittleEndian.Uint32(node[29:33]); lOff != uint32(expectedLowOff) {
		t.Fatalf("Expected lowerOffset to be %d but instead got %d", expectedLowOff, lOff)
	}

	// ensure siblings are unchanged
	if l := binary.LittleEndian.Uint32(node[47:51]); l != uint32(leftSib) {
		t.Fatalf("Expected left sibling to be %d, got %d", leftSib, l)
	}

	if r := binary.LittleEndian.Uint32(node[43:47]); r != uint32(rightSib) {
		t.Fatalf("Expected left sibling to be %d, got %d", rightSib, r)
	}
}

func TestDefragmentNoneFragmentedNode(t *testing.T) {
	entries, err := generateKV(3)
	if err != nil {
		t.Fatal(err.Error())
	}

	var keys [][]byte
	var vals [][]byte

	for _, e := range entries {
		keys = append(keys, e.key)
		vals = append(vals, e.val)
	}

	node := createTestLeafNode(25, keys, vals)
	// set left sibling, right sibling
	leftSib := 24
	rightSib := 27
	binary.LittleEndian.PutUint32(node[47:51], uint32(leftSib))
	binary.LittleEndian.PutUint32(node[43:47], uint32(rightSib))

	err = defragmentNode(&node)
	if err != nil {
		t.Fatalf("Expected no error, intead got:: %s", err.Error())
	}

	var expectedCellOff uint32 = pgr.PAGE_SIZE_BYTES - pgr.LOWER_PADDING_BYTES
	for i := range len(keys) {
		cOff := binary.LittleEndian.Uint32(node[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+5])
		kLen := binary.LittleEndian.Uint32(node[cOff+1 : cOff+5])
		vLen := binary.LittleEndian.Uint32(node[cOff+5 : cOff+9])
		cellSize := 13 + kLen + vLen
		expectedCellOff -= cellSize

		if cOff != expectedCellOff {
			t.Fatalf("%d. Expected cell offset of %d but got %d", i, expectedCellOff, cOff)
		}
	}

	if uOff := binary.LittleEndian.Uint32(node[25:29]); uOff != expectedCellOff {
		t.Fatalf("Expected upperoffset to be %d but instead got %d", expectedCellOff, uOff)
	}

	expectedLowOff := pgr.HEADER_SIZE_BYTES + (len(entries) * pgr.CELL_POINTER_SIZE_BYTE)
	if lOff := binary.LittleEndian.Uint32(node[29:33]); lOff != uint32(expectedLowOff) {
		t.Fatalf("Expected lowerOffset to be %d but instead got %d", expectedLowOff, lOff)
	}

	// ensure siblings are unchanged
	if l := binary.LittleEndian.Uint32(node[47:51]); l != uint32(leftSib) {
		t.Fatalf("Expected left sibling to be %d, got %d", leftSib, l)
	}

	if r := binary.LittleEndian.Uint32(node[43:47]); r != uint32(rightSib) {
		t.Fatalf("Expected left sibling to be %d, got %d", rightSib, r)
	}
}

func TestDefragmentInternalNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true, lgr)
	if err != nil {
		pagr.Close()
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if buffManager == nil {
		pagr.Close()
		helpers.PrintTestErrorMsg("Expected cache, got nil", t)
	}

	t.Cleanup(func() {
		if buffManager != nil {
			err := buffManager.Close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to close buffermanager: %s", err.Error()), t)
			}
			helpers.PrintSuccessMsg("successfully closed buffermanager")
		}
	})

	bp, err := NewBpTree(buffManager, w, lgr)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	keys := [][]byte{
		[]byte("age"),
		[]byte("code"),
		[]byte("country"),
		[]byte("relation"),
	}
	ptrs := []uint32{5, 12, 14, 19, 23}

	node := createTestInternalNode(25, keys, ptrs)

	// insert key to middle of node
	insertKey := []byte("color")
	insertPtr := 99
	err = bp.insertToFrame(&node, insertKey, uint32(insertPtr), nil)
	if err != nil {
		t.Fatalf("Unable to insert to to frame: %s", err.Error())
	}

	// set left sibling, right sibling
	leftSib := 24
	rightSib := 27
	binary.LittleEndian.PutUint32(node[47:51], uint32(leftSib))
	binary.LittleEndian.PutUint32(node[43:47], uint32(rightSib))

	err = defragmentNode(&node)
	if err != nil {
		t.Fatalf("Expected no error, intead got: %s", err.Error())
	}

	fmt.Println("PRINTING DEFRAGMENTED NODE-->")
	fmt.Println(printNodeContent(&node))

	var expectedCellOff uint32 = pgr.PAGE_SIZE_BYTES - pgr.LOWER_PADDING_BYTES
	var prevKey []byte
	for i := range len(keys) + 1 {
		cOff := binary.LittleEndian.Uint32(node[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+5])
		kLen := binary.LittleEndian.Uint32(node[cOff+1 : cOff+5])
		vLen := binary.LittleEndian.Uint32(node[cOff+5 : cOff+9])
		cellSize := 13 + kLen + vLen
		expectedCellOff -= cellSize

		if cOff != expectedCellOff {
			t.Fatalf("%d. Expected cell offset of %d but got %d", i, expectedCellOff, cOff)
		}

		currKey := node[13 : 13+kLen]
		if bytes.Compare(currKey, prevKey) != 1 {
			t.Fatalf("%d. Expected currKey: %s, to be greater than previous key: %v", i, currKey, prevKey)
		}

		prevKey = make([]byte, kLen)
		copy(prevKey, currKey)
	}

	if uOff := binary.LittleEndian.Uint32(node[25:29]); uOff != expectedCellOff {
		t.Fatalf("Expected upperoffset to be %d but instead got %d", expectedCellOff, uOff)
	}

	expectedLowOff := pgr.HEADER_SIZE_BYTES + ((len(keys) + 1) * pgr.CELL_POINTER_SIZE_BYTE)
	if lOff := binary.LittleEndian.Uint32(node[29:33]); lOff != uint32(expectedLowOff) {
		t.Fatalf("Expected lowerOffset to be %d but instead got %d", expectedLowOff, lOff)
	}

	// ensure siblings are unchanged
	if l := binary.LittleEndian.Uint32(node[47:51]); l != uint32(leftSib) {
		t.Fatalf("Expected left sibling to be %d, got %d", leftSib, l)
	}

	if r := binary.LittleEndian.Uint32(node[43:47]); r != uint32(rightSib) {
		t.Fatalf("Expected left sibling to be %d, got %d", rightSib, r)
	}
}

func generateKV(count int) ([]cellEntry, error) {
	if count < 0 {
		return nil, fmt.Errorf("count must be non-negative")
	}

	items := make([]cellEntry, count)

	for i := range count {
		id := make([]byte, 16)
		if _, err := rand.Read(id); err != nil {
			return nil, err
		}

		idStr := hex.EncodeToString(id)

		items[i] = cellEntry{
			key: fmt.Appendf(nil, "user:%s:profile", idStr),
			val: fmt.Appendf(nil,
				`{"id":"%s","name":"User %d","email":"user%d@example.com","active":true}`,
				idStr, i, i,
			),
		}
	}

	return items, nil
}

func generateSortedKV(count int) ([]cellEntry, error) {
	items, err := generateKV(count)
	if err != nil {
		return nil, err
	}

	slices.SortFunc(items, func(a, b cellEntry) int {
		return bytes.Compare(a.key, b.key)
	})

	return items, nil
}

func createTestInternalNode(pid uint32, keys [][]byte, ptrs []uint32) []byte {
	if len(keys) != len(ptrs)-1 && len(keys) != 0 {
		panic("Invalid number of keys and pointers")
	}
	internalFr := make([]byte, pgr.PAGE_SIZE_BYTES)

	// set header
	helpers.SetFlag(&internalFr[0], []int{pgr.IsInternal})

	binary.LittleEndian.PutUint32(internalFr[1:5], pid) // pid=25
	binary.LittleEndian.PutUint32(internalFr[17:21], uint32(len(keys)))
	binary.LittleEndian.PutUint32(internalFr[25:29], uint32(pgr.PAGE_SIZE_BYTES-pgr.LOWER_PADDING_BYTES)) // upper offset
	binary.LittleEndian.PutUint32(internalFr[29:33], uint32(pgr.HEADER_SIZE_BYTES))                       // lower offset

	if len(keys) == 0 {
		return internalFr
	}

	// right child
	binary.LittleEndian.PutUint32(internalFr[39:43], ptrs[len(keys)])

	for i, k := range keys {
		upperOff := binary.LittleEndian.Uint32(internalFr[25:29])
		kLen := len(k)
		cSize := 13 + kLen
		newUpperOffset := upperOff - uint32(cSize)
		binary.LittleEndian.PutUint32(internalFr[25:29], newUpperOffset)

		binary.LittleEndian.PutUint32(internalFr[newUpperOffset+1:newUpperOffset+5], uint32(kLen))
		binary.LittleEndian.PutUint32(internalFr[newUpperOffset+9:newUpperOffset+13], ptrs[i])
		copy(internalFr[newUpperOffset+13:newUpperOffset+13+uint32(kLen)], k)

		// cell pointer
		lowOff := binary.LittleEndian.Uint32(internalFr[29:33])
		binary.LittleEndian.PutUint32(internalFr[lowOff+1:lowOff+5], newUpperOffset)
		if ptrs[i] != 0 {
			helpers.SetFlag(&internalFr[lowOff], []int{pgr.HasChildPtr})
		}
		binary.LittleEndian.PutUint32(internalFr[29:33], lowOff+pgr.CELL_POINTER_SIZE_BYTE)
	}

	return internalFr
}

func createTestLeafNode(pid uint32, keys [][]byte, vals [][]byte) []byte {
	if len(keys) != len(vals) && len(keys) != 0 {
		panic("Invalid number of keys and values")
	}

	leafNode := make([]byte, pgr.PAGE_SIZE_BYTES)

	binary.LittleEndian.PutUint32(leafNode[1:5], pid) // pid=25
	binary.LittleEndian.PutUint32(leafNode[17:21], uint32(len(keys)))
	binary.LittleEndian.PutUint32(leafNode[25:29], uint32(pgr.PAGE_SIZE_BYTES-pgr.LOWER_PADDING_BYTES)) // upper offset
	binary.LittleEndian.PutUint32(leafNode[29:33], uint32(pgr.HEADER_SIZE_BYTES))                       // lower offset

	for i, k := range keys {
		upperOff := binary.LittleEndian.Uint32(leafNode[25:29])
		kLen := len(k)
		vLen := len(vals[i])
		cSize := 13 + kLen + vLen
		newUpperOffset := upperOff - uint32(cSize)
		binary.LittleEndian.PutUint32(leafNode[25:29], newUpperOffset)

		binary.LittleEndian.PutUint32(leafNode[newUpperOffset+1:newUpperOffset+5], uint32(kLen))
		binary.LittleEndian.PutUint32(leafNode[newUpperOffset+5:newUpperOffset+9], uint32(vLen))
		copy(leafNode[newUpperOffset+13:newUpperOffset+13+uint32(kLen)], k)
		copy(leafNode[newUpperOffset+13+uint32(kLen):newUpperOffset+13+uint32(kLen)+uint32(vLen)], vals[i])

		// cell pointer
		lowOff := binary.LittleEndian.Uint32(leafNode[29:33])
		binary.LittleEndian.PutUint32(leafNode[lowOff+1:lowOff+5], newUpperOffset)
		binary.LittleEndian.PutUint32(leafNode[29:33], lowOff+pgr.CELL_POINTER_SIZE_BYTE)
	}

	return leafNode
}

func createTestFragmentedLeafNode(pid uint32, keys [][]byte, vals [][]byte) []byte {
	if len(keys) != len(vals) && len(keys) != 0 {
		panic("Invalid number of keys and values")
	}

	leafNode := make([]byte, pgr.PAGE_SIZE_BYTES)

	binary.LittleEndian.PutUint32(leafNode[1:5], pid) // pid=25
	binary.LittleEndian.PutUint32(leafNode[17:21], uint32(len(keys)))
	binary.LittleEndian.PutUint32(leafNode[25:29], uint32(pgr.PAGE_SIZE_BYTES-pgr.LOWER_PADDING_BYTES)) // upper offset
	binary.LittleEndian.PutUint32(leafNode[29:33], uint32(pgr.HEADER_SIZE_BYTES))                       // lower offset

	for i, k := range keys {
		upperOff := binary.LittleEndian.Uint32(leafNode[25:29])
		kLen := len(k)
		vLen := len(vals[i])
		cSize := 13 + kLen + vLen
		newUpperOffset := upperOff - uint32(cSize) - 20 // add gaps
		binary.LittleEndian.PutUint32(leafNode[25:29], newUpperOffset)

		binary.LittleEndian.PutUint32(leafNode[newUpperOffset+1:newUpperOffset+5], uint32(kLen))
		binary.LittleEndian.PutUint32(leafNode[newUpperOffset+5:newUpperOffset+9], uint32(vLen))
		copy(leafNode[newUpperOffset+13:newUpperOffset+13+uint32(kLen)], k)
		copy(leafNode[newUpperOffset+13+uint32(kLen):newUpperOffset+13+uint32(kLen)+uint32(vLen)], vals[i])

		// cell pointer
		lowOff := binary.LittleEndian.Uint32(leafNode[29:33])
		binary.LittleEndian.PutUint32(leafNode[lowOff+1:lowOff+5], newUpperOffset)
		binary.LittleEndian.PutUint32(leafNode[29:33], lowOff+pgr.CELL_POINTER_SIZE_BYTE)
	}

	return leafNode
}

func InitPager(t *testing.T) *pgr.Pager {
	dir := "baobab"
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, dir)
	os.Mkdir(dataDir, 0750)
	dbFile := filepath.Join(dataDir, "baobab.db")
	dman, err := diskmanager.NewDiskManager(diskmanager.DiskManagerConfig{DataFile: dbFile})

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Could not initialize disk manager: %s", err.Error()), t)
	}

	freelistFile := filepath.Join(dataDir, "baobab")
	pgr, err := pgr.NewPager(pgr.PagerConfig{DManager: dman, FreeListFile: freelistFile, WorkerSize: 1500})
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Could not initialize pager: %s", err.Error()), t)
	}

	return pgr
}

func tempLogFile(t *testing.T) string {
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "baobab.log")
	return dbFile
}
