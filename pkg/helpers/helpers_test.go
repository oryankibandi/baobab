package helpers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMax(t *testing.T) {
	tests := []struct {
		name        string
		lsnA        []byte
		lsnB        []byte
		expectedMax []byte
	}{
		{name: "max_1", lsnA: []byte{0x5, 0, 0, 0, 0x8, 0xa, 0, 0}, lsnB: []byte{0x6, 0, 0, 0, 0xff, 0, 0, 0}, expectedMax: []byte{0x6, 0, 0, 0, 0xff, 0, 0, 0}},
		{name: "max_2", lsnA: []byte{0x2, 0, 0, 0, 0xe6, 0x4, 0, 0}, lsnB: []byte{0x3, 0, 0, 0, 0x87, 0x14, 0, 0}, expectedMax: []byte{0x3, 0, 0, 0, 0x87, 0x14, 0, 0}},
		{name: "max_3", lsnA: []byte{0x2, 0, 0, 0, 0xfe, 0x1f, 0, 0}, lsnB: []byte{0x3, 0, 0, 0, 0x9, 0, 0, 0}, expectedMax: []byte{0x3, 0, 0, 0, 0x9, 0, 0, 0}},
		{name: "same_page_lsn", lsnA: []byte{0x5, 0, 0, 0, 0xe9, 0x11, 0, 0}, lsnB: []byte{0x5, 0, 0, 0, 0x94, 0x11, 0, 0}, expectedMax: []byte{0x5, 0, 0, 0, 0xe9, 0x11, 0, 0}},
		{name: "equal_lsn", lsnA: []byte{0x5, 0, 0, 0, 0xe9, 0x11, 0, 0}, lsnB: []byte{0x5, 0, 0, 0, 0xe9, 0x11, 0, 0}, expectedMax: []byte{0x5, 0, 0, 0, 0xe9, 0x11, 0, 0}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, err := MaxLSN(test.lsnA, test.lsnB)

			assert.NoError(t, err)

			assert.Equal(t, test.expectedMax, m, fmt.Errorf("Expected max: %v\n, got: %v\n", test.expectedMax, m))
		})
	}
}
