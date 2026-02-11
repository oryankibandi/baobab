package diskmanager

import (
	"errors"
	"testing"
)

func TestNewDiskManager(t *testing.T) {
	config := DiskManagerConfig{dataFile: "data"}

	_, err := NewDiskManager(DiskManagerConfig{})

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if !errors.As(err, &DiskioError{}) {
		t.Fatalf("Expected DiskioError, got %v", err)
	}

	dm, err := NewDiskManager(config)

	if err != nil || dm == nil {
		t.Fatal("Could not initiate disk manager")
	}
}
