package bptree

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	keys := [][]byte{[]byte("age"), []byte("country"), []byte("name")}
	ptr := []uint32{25, 34, 89, 99}
	internalFrame := createTestInternalNode(keys, ptr)
	if internalFrame == nil {
		t.Fatal("No internal frame created")
	}

	for idx := range len(keys) {
		retrievedIdx, err := findKeyIndex(&internalFrame, keys[idx], 0, uint32(len(keys)-1))
		if err != nil {
			t.Fatalf("Could not find index: %v", err.Error())
		}

		if int(retrievedIdx) != idx {
			t.Fatalf("Expected index %d but got %d", idx, retrievedIdx)
		}
	}
}

func TestFindKeyIndexLeafNode(t *testing.T) {
	// +--------+-----------------+-----------+--------+
	// |  name  |       age       |  country  |  code  |
	// +--------+-----------------+-----------+--------+
	// |  Ben   |  thirty four    |  KENYA    |   KE   |
	// +--------+-----------------+-----------+--------+
	keys := [][]byte{[]byte("age"), []byte("code"), []byte("country"), []byte("name")}
	vals := [][]byte{[]byte("thirty four"), []byte("KE"), []byte("KENYA"), []byte("Ben")}
	leafNode := createTestLeafNode(25, keys, vals)
	if leafNode == nil {
		t.Fatal("No leaf node created")
	}

	for idx := range len(keys) {
		retrievedIdx, err := findKeyIndex(&leafNode, keys[idx], 0, uint32(len(keys)-1))
		if err != nil {
			t.Fatalf("Could not find index: %v", err.Error())
		}

		if int(retrievedIdx) != idx {
			t.Fatalf("Expected index %d but got %d", idx, retrievedIdx)
		}
	}
}

func TestFindInsertionIdxLeafNode(t *testing.T) {
	// +--------+-----------------+-----------+--------+
	// |  name  |       age       |  country  |  code  |
	// +--------+-----------------+-----------+--------+
	// |  Ben   |  thirty four    |  KENYA    |   KE   |
	// +--------+-----------------+-----------+--------+
	keys := [][]byte{[]byte("age"), []byte("code"), []byte("country"), []byte("name")}
	vals := [][]byte{[]byte("thirty four"), []byte("KE"), []byte("KENYA"), []byte("Ben")}
	leafNode := createTestLeafNode(25, keys, vals)
	if leafNode == nil {
		t.Fatal("No leaf node created")
	}

	insertionKey := []byte("bio") // should be at idx 1

	idx, err := findInsertionIdx(&leafNode, insertionKey, 0, uint32(len(keys)-1))
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if idx != 1 {
		t.Fatalf("Expected insertion idx %d, got %d", 1, idx)
	}

	// check exact key
	expectedIdx := 2
	idx, err = findInsertionIdx(&leafNode, keys[expectedIdx], 0, uint32(len(keys)-1))
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if idx != int32(expectedIdx) {
		t.Fatalf("Expected insertion idx %d, got %d", expectedIdx, idx)
	}
}

func TestFindInsertionIdxInternalNode(t *testing.T) {
	// +------+------+------+--------------+
	// |  age   |  name |  country  |      |
	// +--------+-------+-----------+  99  +
	// |  25    |  34   |  89       |      |
	// +--------+-------+-----------+------+
	keys := [][]byte{[]byte("age"), []byte("country"), []byte("name")}
	ptr := []uint32{25, 34, 89, 99}
	internalFrame := createTestInternalNode(keys, ptr)
	if internalFrame == nil {
		t.Fatal("No internal frame created")
	}

	insertionKey := []byte("bio") // should be at idx 1

	idx, err := findInsertionIdx(&internalFrame, insertionKey, 0, uint32(len(keys)-1))
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
	}

	if idx != 1 {
		t.Fatalf("Expected insertion idx %d, got %d", 1, idx)
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
	ptrs := []uint32{25, 88, 99, 150}
	node := createTestInternalNode(keys, ptrs)

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
}

// TODO: Test deleteFromNode
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
	node := createTestInternalNode(keys, ptrs)

	deletedPtr, err := bp.deleteFromNode(&node, keys[len(keys)-1], false)
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
	deletedPtr, err = bp.deleteFromNode(&node, deleteKey, true)
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

	deletedPtr, err := bp.deleteFromNode(&node, keys[len(keys)-1], false)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
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
	deletedPtr, err = bp.deleteFromNode(&node, deleteKey, true)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err.Error())
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
	node := createTestInternalNode(keys, ptrs)

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

	node := createTestInternalNode(keys, ptrs)
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
		node := createTestInternalNode(test.keys, test.ptrs)

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
		// +--------+-------+  88  +    +------------------+--------+   250     +
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

func createTestInternalNode(keys [][]byte, ptrs []uint32) []byte {
	if len(keys) != len(ptrs)-1 {
		panic("Invalid number of keys and pointers")
	}
	internalFr := make([]byte, pgr.PAGE_SIZE_BYTES)

	// set header
	helpers.SetFlag(&internalFr[0], []int{pgr.IsInternal})

	binary.LittleEndian.PutUint32(internalFr[1:5], 25) // pid=25
	binary.LittleEndian.PutUint32(internalFr[17:21], uint32(len(keys)))
	binary.LittleEndian.PutUint32(internalFr[25:29], uint32(pgr.PAGE_SIZE_BYTES-pgr.LOWER_PADDING_BYTES)) // upper offset
	binary.LittleEndian.PutUint32(internalFr[29:33], uint32(pgr.HEADER_SIZE_BYTES))                       // lower offset

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
	if len(keys) != len(vals) {
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
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "baobab.db")
	dman, err := diskmanager.NewDiskManager(diskmanager.DiskManagerConfig{DataFile: dbFile})

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Could not initialize disk manager: %s", err.Error()), t)
	}

	freelistFile := filepath.Join(t.TempDir(), "baobab")
	pgr, err := pgr.NewPager(pgr.PagerConfig{DManager: dman, FreeListFile: freelistFile, WorkerSize: 1500})
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Could not initialize pager: %s", err.Error()), t)
	}

	return pgr
}
