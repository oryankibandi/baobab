package bptree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/oryankibandi/baobab/pkg/buffermanager"
	"github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/logger"
	pgr "github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

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
	}{
		{
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("name")},
			vals:         [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			insertionKey: []byte("code"),
			expectedIdx:  1,
		},
		// node with only one item
		{
			keys:         [][]byte{[]byte("country")},
			vals:         [][]byte{[]byte("v1")},
			insertionKey: []byte("age"),
			expectedIdx:  0,
		},
		{
			keys:         [][]byte{[]byte("code")},
			vals:         [][]byte{[]byte("v1")},
			insertionKey: []byte("continent"),
			expectedIdx:  1,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			vals:         [][]byte{[]byte("v1"), []byte("v2")},
			insertionKey: []byte("code"),
			expectedIdx:  1,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			vals:         [][]byte{[]byte("v1"), []byte("v2")},
			insertionKey: []byte("cousin"),
			expectedIdx:  2,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			vals:         [][]byte{[]byte("v1"), []byte("v2")},
			insertionKey: []byte("account"),
			expectedIdx:  0,
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
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			vals:         [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			insertionKey: []byte("office"),
			expectedIdx:  3,
		},
		{ // exact match
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			vals:         [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			insertionKey: []byte("country"),
			expectedIdx:  1,
		},
		{ // empty node
			keys:         [][]byte{},
			vals:         [][]byte{},
			insertionKey: []byte("country"),
			expectedIdx:  0,
		},
		{ // shared prefixes
			keys:         [][]byte{[]byte("car"), []byte("cat"), []byte("code")},
			vals:         [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
			insertionKey: []byte("can"),
			expectedIdx:  0,
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
		},
	}

	for i, test := range tests {
		leafNode := createTestLeafNode(25, test.keys, test.vals)
		if leafNode == nil {
			t.Fatalf("%d. No leaf node created", i)
		}

		idx, err := findInsertionIdx(&leafNode, test.insertionKey, 0, uint32(len(test.keys)-1))
		if err != nil {
			t.Fatalf("%d. Expected no error, got %s", i, err.Error())
		}

		if idx != int32(test.expectedIdx) {
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
	}{
		{
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("name")},
			ptr:          []uint32{25, 34, 89, 99},
			insertionKey: []byte("code"),
			expectedIdx:  1,
		},
		// node with only one item
		{
			keys:         [][]byte{[]byte("country")},
			ptr:          []uint32{25, 34},
			insertionKey: []byte("age"),
			expectedIdx:  0,
		},
		{
			keys:         [][]byte{[]byte("code")},
			ptr:          []uint32{25, 34},
			insertionKey: []byte("continent"),
			expectedIdx:  1,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			ptr:          []uint32{25, 34, 89},
			insertionKey: []byte("code"),
			expectedIdx:  1,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			ptr:          []uint32{25, 34, 89},
			insertionKey: []byte("cousin"),
			expectedIdx:  2,
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country")},
			ptr:          []uint32{25, 34, 89},
			insertionKey: []byte("account"),
			expectedIdx:  0,
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
		},
		{
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			ptr:          []uint32{25, 88, 99, 0},
			insertionKey: []byte("office"),
			expectedIdx:  3,
		},
		{ // exact match
			keys:         [][]byte{[]byte("age"), []byte("country"), []byte("nationality")},
			ptr:          []uint32{25, 88, 99, 0},
			insertionKey: []byte("country"),
			expectedIdx:  1,
		},
		{ // empty node
			keys:         [][]byte{},
			ptr:          []uint32{},
			insertionKey: []byte("country"),
			expectedIdx:  0,
		},
		{ // shared prefixes
			keys:         [][]byte{[]byte("car"), []byte("cat"), []byte("code")},
			ptr:          []uint32{25, 88, 99, 0},
			insertionKey: []byte("can"),
			expectedIdx:  0,
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
		},
	}

	for i, test := range tests {
		internalFrame := createTestInternalNode(35, test.keys, test.ptr)
		if internalFrame == nil {
			t.Fatalf("%d. No internal frame created", i)
		}

		idx, err := findInsertionIdx(&internalFrame, test.insertionKey, 0, uint32(len(test.keys)-1))
		if err != nil {
			t.Fatalf("%d. Expected no error, got %s", i, err.Error())
		}

		if idx != int32(test.expectedIdx) {
			t.Fatalf("Expected insertion idx %d, got %d", test.expectedIdx, idx)
		}
		fmt.Printf("%d. Done...\n", i)
	}
}

func TestInsertLeafNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true)
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

	bp, err := NewBpTree(buffManager, w)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	// leaf node
	keys := [][]byte{[]byte("age"), []byte("country"), []byte("name")}
	vals := [][]byte{[]byte("thirty four"), []byte("united states"), []byte("leonard")}
	node := createTestLeafNode(25, keys, vals)

	insertKey := []byte("code")
	insertVal := []byte("US")
	expectedInsertionIdx := 1

	err = bp.insertToFrame(&node, insertKey, 0, insertVal)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	idx, err := findKeyIndex(&node, insertKey, 0, uint32(len(keys)))
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if idx != int32(expectedInsertionIdx) {
		t.Fatalf("Expected key to be inserted at idx %d, got %d", expectedInsertionIdx, idx)
	}
}

func TestInsertInternalNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	// internal node
	keys := [][]byte{[]byte("age"), []byte("country")}
	ptrs := []uint32{25, 88, 99}
	node := createTestInternalNode(35, keys, ptrs)

	insertKey := []byte("code")
	insertPtr := 66
	expectedInsertionIdx := 1

	err = bp.insertToFrame(&node, insertKey, uint32(insertPtr), nil)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	idx, err := findKeyIndex(&node, insertKey, 0, uint32(len(keys)))
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if idx != int32(expectedInsertionIdx) {
		t.Fatalf("Expected key to be inserted at idx %d, got %d", expectedInsertionIdx, idx)
	}

	// retreive key
	cellOff := binary.LittleEndian.Uint32(node[(idx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+1 : (idx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+5])
	kLen := binary.LittleEndian.Uint32(node[cellOff+1 : cellOff+5])
	key := node[cellOff+13 : cellOff+13+kLen]
	if !bytes.Equal(key, insertKey) {
		t.Fatalf("Expected inserted key to be %v, but got %v", insertKey, key)
	}

	// printNode
	fmt.Println(printNodeContent(&node))

	// insert second key
	insertKey = []byte("name")
	insertPtr = 150
	expectedInsertionIdx = 3

	err = bp.insertToFrame(&node, insertKey, uint32(insertPtr), nil)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}
	// printNode
	fmt.Println(printNodeContent(&node))

	idx, err = findKeyIndex(&node, insertKey, 0, uint32(len(keys)+1))
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if idx != int32(expectedInsertionIdx) {
		t.Fatalf("Expected key %s to be inserted at idx %d, got %d", insertKey, expectedInsertionIdx, idx)
	}

	// retreive key
	cellOff = binary.LittleEndian.Uint32(node[(idx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+1 : (idx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+5])
	kLen = binary.LittleEndian.Uint32(node[cellOff+1 : cellOff+5])
	key = node[cellOff+13 : cellOff+13+kLen]
	if !bytes.Equal(key, insertKey) {
		t.Fatalf("Expected inserted key to be %v, but got %v", insertKey, key)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true)
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

	bp, err := NewBpTree(buffManager, w)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	keys := [][]byte{[]byte("age"), []byte("code"), []byte("country"), []byte("name")}
	ptrs := []uint32{25, 66, 88, 99, 150}

	// internal node before deletion
	// +--------+------+----------+-------+------+
	// |  age   | code |  country |  name |      |
	// +--------+-------+-----------------+ 150  +
	// |  25    |  66  |     88   |  99   |      |
	// +--------+------+----------+-------+------+
	node := createTestInternalNode(35, keys, ptrs)

	deletedPtr, _, err := bp.deleteFromNode(&node, keys[len(keys)-1], false)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	// expected internal node after deletion of the last item
	// +--------+------+----------+-------+------+
	// |  age   | code |  country |       |      |
	// +--------+-------+-----------------+  99  +
	// |  25    |  66  |     88   |       |      |
	// +--------+------+----------+-------+------+

	if d := ptrs[len(ptrs)-1]; deletedPtr != d {
		t.Fatalf("Expected deleted pointer to be %d but got %d", d, deletedPtr)
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

	// check right child ptr
	rightPtr := binary.LittleEndian.Uint32(node[39:43])
	if expected := ptrs[len(ptrs)-2]; rightPtr != expected {
		t.Fatalf("Expected right child pointer to be %d, but got %d", expected, rightPtr)
	}

	// expected internal node after leftmerge delete of the key "country"
	// +--------+------+----------+-------+------+
	// |  age   | code |	      |       |      |
	// +--------+-------+-----------------+  99  +
	// |  25    |  66  |          |       |      |
	// +--------+------+----------+-------+------+

	deleteKey := []byte("country")
	deletedPtr, _, err = bp.deleteFromNode(&node, deleteKey, true)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if deletedPtr != ptrs[len(ptrs)-3] {
		t.Fatalf("Expected deleted pointer to be %d, got %d", ptrs[len(ptrs)-2], deletedPtr)
	}

	// check item count
	itemCount = binary.LittleEndian.Uint32(node[17:21])
	if l := len(keys) - 2; itemCount != uint32(l) {
		t.Fatalf("Expected item count to be %d, but got %d", l, itemCount)
	}

	// check right child ptr
	rightPtr = binary.LittleEndian.Uint32(node[39:43])
	if expected := ptrs[len(ptrs)-2]; rightPtr != expected {
		t.Fatalf("Expected right child pointer to be %d, but got %d", expected, rightPtr)
	}
}

func TestDeleteKeyLeafNode(t *testing.T) {
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pgr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 128 * 1024, // 128MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true)
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

	bp, err := NewBpTree(buffManager, w)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true)
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

	bp, err := NewBpTree(buffManager, w)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true)
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

	bp, err := NewBpTree(buffManager, w)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true)
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

	bp, err := NewBpTree(buffManager, w)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pgr, true)
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

	bp, err := NewBpTree(buffManager, w)
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
	lgr := logger.NewLogger("", logger.DEBUG, 1)
	w := wal.NewWal(lgr)
	// initialize pager
	pagr := InitPager(t)

	// initialize buffer manager
	cConfig := buffermanager.CacheConfig{
		CacheSize: 16 * 1024, // 16MB
	}

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
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
	}

	// example overflown internal node with order 2
	// +--------+-------+---------+--------------------+--------+------+
	// |  age   | code  | country |   marital status   |  name  |      |
	// +--------+-------+---------+--------------------+--------+ 250  +
	// |  25    |  66   |    88   |         99         |  150   |      |
	// +--------+-------+---------+--------------------+--------+------+
	for _, test := range tests {
		t.Logf("----------------------------------\n")
		t.Logf("Running test: %s\n", test.name)
		t.Logf("----------------------------------\n")
		node := createTestInternalNode(35, test.keys, test.ptrs)

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
		// +--------+-------+------+    +------------------+--------+-----------+
		// |  age   | code  |      |    |  marital status  |  name  |           |
		// +--------+-------+  88  +<-->+------------------+--------+   250     +
		// |  25    |  66   |	   |    |       99         |  150   |           |
		// +--------+-------+------+    +------------------+--------+------------

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

		for i := range leftNodeItemCount {
			cellOff := binary.LittleEndian.Uint32(node[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+5])
			cellKeySize := binary.LittleEndian.Uint32(node[cellOff+1 : cellOff+5])
			key := node[cellOff+13 : cellOff+13+cellKeySize]
			ptr := binary.LittleEndian.Uint32(node[cellOff+9 : cellOff+13])

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

		for i := range rightNodeItemCount {
			cellOff := binary.LittleEndian.Uint32((*rightNodeBuff)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+5])
			cellKeySize := binary.LittleEndian.Uint32((*rightNodeBuff)[cellOff+1 : cellOff+5])
			key := (*rightNodeBuff)[cellOff+13 : cellOff+13+cellKeySize]
			ptr := binary.LittleEndian.Uint32((*rightNodeBuff)[cellOff+9 : cellOff+13])

			if !bytes.Equal(key, test.keys[i+1+pgr.ORDER]) {
				t.Fatalf("Expected key at index %d to be %v, but got %v", i, test.keys[i+1+pgr.ORDER], key)
			}

			if ptr != test.ptrs[i+pgr.ORDER+1] {
				t.Fatalf("Expected pointer at index %d to be %d, but got %d", i, test.ptrs[i+pgr.ORDER+i], ptr)
			}
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
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
	// +--------+----------+------+         +----------+-------+
	// |  age   | country  |      |         |  office  |       |
	// +--------+----------+  99  +<------->+----------+   30  +
	// |  25    |   88     |      |         |    10    |       |
	// +--------+----------+------+         +----------+-------+
	sepKey := []byte("nationality")
	leftKeys := [][]byte{[]byte("age"), []byte("country")}
	leftNodePid := 35
	leftPtrs := []uint32{25, 88, 99}
	leftNode := createTestInternalNode(uint32(leftNodePid), leftKeys, leftPtrs)

	leftNodeLeftSibling, err := bp.buffermanager.NewFrame(true, false)
	if err != nil {
		t.Fatalf("Unable to create new frame: %s", err)
	}

	rightKeys := [][]byte{
		[]byte("office"),
	}
	rightPtrs := []uint32{10, 30}
	rightNodePid := 45
	rightNode := createTestInternalNode(uint32(rightNodePid), rightKeys, rightPtrs)
	rightNodeRightSibling, err := bp.buffermanager.NewFrame(true, false)
	if err != nil {
		t.Fatalf("Unable to create new frame: %s", err)
	}

	// set sibling pointers
	binary.LittleEndian.PutUint32(leftNode[43:47], uint32(rightNodePid))
	binary.LittleEndian.PutUint32(rightNode[47:51], uint32(leftNodePid))

	rightNodeRightSibling.Acquire(false)
	rightNodeRightSibFr, _, err := rightNodeRightSibling.RawBufferSlice()
	if err != nil {
		t.Fatalf("Unable to get frame buffer: %s", err)
	}
	binary.LittleEndian.PutUint32((*rightNodeRightSibFr)[47:51], uint32(rightNodePid))
	copy(rightNode[43:47], (*rightNodeRightSibFr)[1:5])
	rightNodeRightSibling.Release(false)
	rightNodeRightSibling.Unreference()

	leftNodeLeftSibling.Acquire(false)
	leftNodeLeftSibFr, _, err := leftNodeLeftSibling.RawBufferSlice()
	if err != nil {
		t.Fatalf("Unable to get frame buffer: %s", err)
	}
	binary.LittleEndian.PutUint32((*leftNodeLeftSibFr)[43:47], uint32(leftNodePid))
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
	expectedItemCount := len(leftKeys) + len(rightKeys) + 1
	if itemCount != uint32(expectedItemCount) {
		t.Fatalf("Expected number of items in new node to be %d but got %d", expectedItemCount, itemCount)
	}

	// check right most child
	expectedRightChildPtr := rightPtrs[len(rightPtrs)-1]
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

		if bytes.Compare(currKey, prevKey) == -1 {
			t.Fatalf("Expected curr key %v to be greater than previous key %v", currKey, prevKey)
		}

		prevKey = make([]byte, kLen)
		copy(prevKey, currKey)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

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
	sepKey := []byte("country")
	leftKeys := [][]byte{[]byte("age")}
	leftNodePid := 35
	leftPtrs := []uint32{25, 99}
	leftNode := createTestInternalNode(uint32(leftNodePid), leftKeys, leftPtrs)

	leftNodeLeftSibling, err := bp.buffermanager.NewFrame(true, false)
	if err != nil {
		t.Fatalf("Unable to create new frame: %s", err)
	}

	rightKeys := [][]byte{
		[]byte("nationality"),
		[]byte("office"),
	}
	rightPtrs := []uint32{510, 10, 30}
	rightNodePid := 45
	rightNode := createTestInternalNode(uint32(rightNodePid), rightKeys, rightPtrs)
	rightNodeRightSibling, err := bp.buffermanager.NewFrame(true, false)
	if err != nil {
		t.Fatalf("Unable to create new frame: %s", err)
	}

	// set sibling pointers
	binary.LittleEndian.PutUint32(leftNode[43:47], uint32(rightNodePid))
	binary.LittleEndian.PutUint32(rightNode[47:51], uint32(leftNodePid))

	rightNodeRightSibling.Acquire(false)
	rightNodeRightSibFr, _, err := rightNodeRightSibling.RawBufferSlice()
	if err != nil {
		t.Fatalf("Unable to get frame buffer: %s", err)
	}
	binary.LittleEndian.PutUint32((*rightNodeRightSibFr)[47:51], uint32(rightNodePid))
	copy(rightNode[43:47], (*rightNodeRightSibFr)[1:5])
	rightNodeRightSibling.Release(false)
	rightNodeRightSibling.Unreference()

	leftNodeLeftSibling.Acquire(false)
	leftNodeLeftSibFr, _, err := leftNodeLeftSibling.RawBufferSlice()
	if err != nil {
		t.Fatalf("Unable to get frame buffer: %s", err)
	}
	binary.LittleEndian.PutUint32((*leftNodeLeftSibFr)[43:47], uint32(leftNodePid))
	copy(leftNode[47:51], (*leftNodeLeftSibFr)[1:5])
	leftNodeLeftSibling.Release(false)
	leftNodeLeftSibling.Unreference()

	// merge nodes
	newSepKey, err := bp.merge(&leftNode, &rightNode, sepKey, true)
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
	expectedItemCount := len(leftKeys) + len(rightKeys) + 1
	if itemCount != uint32(expectedItemCount) {
		t.Fatalf("Expected number of items in right node to be %d but got %d", expectedItemCount, itemCount)
	}

	// check right most child
	expectedRightChildPtr := rightPtrs[len(rightPtrs)-1]
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

		if bytes.Compare(currKey, prevKey) == -1 {
			t.Fatalf("Expected curr key %v to be greater than previous key %v", currKey, prevKey)
		}

		prevKey = make([]byte, kLen)
		copy(prevKey, currKey)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
	if err != nil {
		t.Fatalf("Unable to initialize b+ tree index: %s", err.Error())
	}

	if bp == nil {
		t.Fatalf("expected B+ index got nil")
	}

	for _, test := range tests {
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

	buffManager, err := buffermanager.NewBufferManager(cConfig, w, pagr, true)
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

	bp, err := NewBpTree(buffManager, w)
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
