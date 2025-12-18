package logger

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleton(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "baobab.log")

	logger := NewLogger(path, DEBUG, 1)

	assert.NotNilf(t, logger, "new logger instance is nil")

	logger2 := NewLogger(path, PRODUCTION, 2)

	assert.NotNilf(t, logger2, "new logger 2 instance is nil")
	assert.Equalf(t, logger, logger2, "No singleton instance created.")

	err := logger.Close()

	assert.NoErrorf(t, err, "Unable to close logger")
}

func TestNewLogger(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "baobab.log")

	logger := NewLogger(path, DEBUG, 1)
	assert.NotNilf(t, logger, "new logger instance is nil")

	t.Run("log file creation", func(t *testing.T) {
		// write to trigger file creation. Lumberjack creates log file lazily
		ch := make(chan bool)
		err := logger.Write("testing", "TestNewLogger()", LevelDebug, "First Log", &ch)
		assert.NoError(t, err, "Could not write first log")

		// wait for log to be written
		<-ch
		assert.FileExistsf(t, path, fmt.Sprintf("file was not created: %s", path))

		err = logger.Close()
		assert.NoErrorf(t, err, "Unable to close logger")
	})
}

func TestWrite(t *testing.T) {
	tests := []struct {
		name    string
		pkgName string
		fn      string
		level   slog.Level
		msg     string
		retChan *chan bool
	}{
		{name: "test_1", pkgName: "testing", fn: "TestWrite()", level: LevelDebug, msg: "Testing message 1."},
		{name: "test_2", pkgName: "testing", fn: "TestWrite()", level: LevelDebug, msg: "Testing message 2."},
		{name: "test_3", pkgName: "testing", fn: "TestWrite()", level: LevelDebug, msg: "Testing message 3."},
		{name: "test_4", pkgName: "testing", fn: "TestWrite()", level: LevelDebug, msg: "Testing message 4."},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, logFile)

	logger := NewLogger(path, DEBUG, 1)
	assert.NotNilf(t, logger, "new logger instance is nil")

	logCh := make(chan bool, len(tests))
	// Write logs
	for _, item := range tests {
		t.Run(item.name, func(t *testing.T) {
			err := logger.Write(item.pkgName, item.fn, item.level, item.msg, &logCh)

			assert.NoErrorf(t, err, fmt.Sprintf("Unable to write to log: %s", err))
		})
	}

	// wait for logs to be written
	tLen := len(tests)
	var k atomic.Uint64

JobWait:
	for {
		select {
		case <-logCh:
			k.Add(1)
			if k.Load() >= uint64(tLen) {
				break JobWait
			}
		}
	}

	// Ensure log  files have been written
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		lines++
	}

	assert.Equalf(t, len(tests), lines, fmt.Sprintf("Expected %d logs written, got %d", len(tests), lines))

	err = logger.Close()
	assert.NoErrorf(t, err, "Unable to close logger")
}
