package logger

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// test that an item added to the linked list is added to the tail
func TestAddItem(t *testing.T) {
	tests := []struct {
		name         string
		lReq         *LogReq
		expectedName string
	}{
		{name: "req 1", lReq: &LogReq{
			log: LogItem{
				pkg:      "logger",
				fn:       "TestAddItem()",
				logLevel: LevelDebug,
				msg:      "Log req 1",
			},
		}, expectedName: "Log req 1",
		},
		{name: "req 2", lReq: &LogReq{
			log: LogItem{
				pkg:      "logger",
				fn:       "TestAddItem()",
				logLevel: LevelDebug,
				msg:      "Log req 2",
			},
		}, expectedName: "Log req 2",
		},
		{name: "req 3", lReq: &LogReq{
			log: LogItem{
				pkg:      "logger",
				fn:       "TestAddItem()",
				logLevel: LevelDebug,
				msg:      "Log req 3",
			},
		}, expectedName: "Log req 3",
		},
		{name: "req 4", lReq: &LogReq{
			log: LogItem{
				pkg:      "logger",
				fn:       "TestAddItem()",
				logLevel: LevelDebug,
				msg:      "Log req 4",
			},
		}, expectedName: "Log req 4",
		},
		{name: "nil log request", lReq: nil, expectedName: "nil"},
	}

	q := newLogQueue()

	assert.NotNil(t, q, "Not able to create queue.")
	testLen := len(tests)
	// insert
	for i, testItem := range tests {
		if i == testLen-1 {
			break
		}

		t.Run(testItem.name, func(t *testing.T) {
			newItem, err := q.addItem(testItem.lReq)

			assert.NoError(t, err)
			assert.Equalf(t, testItem.lReq, newItem, fmt.Sprintf("Wrong item added. Expected: %v, got %v", testItem.lReq, newItem))

		})
	}

	// check head of items
	for i := range testLen - 1 {
		t.Run(tests[i].name, func(t *testing.T) {
			oldestReq := q.getOldest()
			assert.NotNil(t, oldestReq, "Invalid nil head value. ")

			assert.Equalf(t, tests[i].lReq, oldestReq, fmt.Sprintf("invalid log request set as head. Received %v, Expected %v", oldestReq, tests[i].lReq))

		})
	}

	// ensure passing nil log request returns error
	_, err := q.addItem(tests[testLen-1].lReq)

	assert.Errorf(t, err, "Expected error for a nil log request")
}
