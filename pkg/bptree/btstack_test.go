package bp_tree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdd(t *testing.T) {
	tests := []struct {
		name          string
		expectedMaxId int
		expectedCount int
		path          *TraversePath
		isAdded       bool
	}{
		{name: "rootNode", expectedMaxId: 1, expectedCount: 1, path: &TraversePath{idx: 0, n: &Node{}}, isAdded: true},
		{name: "node1", expectedMaxId: 2, expectedCount: 2, path: &TraversePath{idx: 1, n: &Node{}}, isAdded: true},
		{name: "node2", expectedMaxId: 3, expectedCount: 3, path: &TraversePath{idx: 24, n: &Node{}}, isAdded: true},
		{name: "node3", expectedMaxId: 4, expectedCount: 4, path: &TraversePath{idx: 14, n: &Node{}}, isAdded: true},
	}

	st, err := NewBTStack(1)

	assert.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			added, err := st.Add(test.path)

			assert.NoError(t, err)
			assert.Equal(t, test.isAdded, added, fmt.Errorf("Item should be added to BTStack. added -> %v", added))
			assert.Equal(t, test.expectedCount, st.Count, "Count should be equal. Expected -> %d, got -> %d", test.expectedCount, st.Count)
			assert.Equal(t, test.expectedMaxId, st.maxId, "MaxId should match")

		})
	}
}

func TestPop(t *testing.T) {
	tests := []struct {
		name          string
		expectedMaxId int
		expectedCount int
		path          *TraversePath
		isAdded       bool
	}{
		{name: "rootNode", expectedMaxId: 1, expectedCount: 1, path: &TraversePath{idx: 0, n: &Node{}}, isAdded: true},
		{name: "node1", expectedMaxId: 2, expectedCount: 2, path: &TraversePath{idx: 1, n: &Node{}}, isAdded: true},
		{name: "node2", expectedMaxId: 3, expectedCount: 3, path: &TraversePath{idx: 24, n: &Node{}}, isAdded: true},
		{name: "node3", expectedMaxId: 4, expectedCount: 4, path: &TraversePath{idx: 14, n: &Node{}}, isAdded: true},
	}

	st, err := NewBTStack(1)

	assert.NoError(t, err)

	for _, test := range tests {
		added, err := st.Add(test.path)

		assert.NoError(t, err)
		assert.Equal(t, test.isAdded, added)
	}

	l := len(tests) - 1
	for i := range l {
		item := tests[l-i]

		t.Run(item.name, func(t *testing.T) {
			p, err := st.Pop()

			assert.NoError(t, err)
			assert.Equalf(t, item.path, p, "Traverse path should be equal")
			assert.Equal(t, l-i, st.Count)
			assert.Equal(t, item.path.idx, p.idx)
		})
	}
}
