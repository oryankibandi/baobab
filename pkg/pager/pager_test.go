package pager

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
)

func TestNewPager(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	freelistFile := filepath.Join(dir, "baobab")

	dManConfig := diskmanager.DiskManagerConfig{
		DataFile: dbFile,
	}

	dMan, err := diskmanager.NewDiskManager(dManConfig)
	if err != nil {
		helpers.PrintTestErrorMsg("Unable to initialize disk manager", t)
	}

	type tTest struct {
		name         string
		freelistFile string
		diskMan      *diskmanager.DiskManager
		isValid      bool
	}

	tests := make([]tTest, 0)

	// no freelist file
	tests = append(tests, tTest{
		name:    "test_newpager_no_freelist",
		diskMan: dMan,
		isValid: true,
	})

	// no diskmanager
	tests = append(tests, tTest{
		name:         "test_newpager_no_diskmanager",
		freelistFile: freelistFile,
		isValid:      false,
	})

	// no diskmanager or freelist
	tests = append(tests, tTest{
		name:    "test_newpager_no_freelist_and_diskmanager",
		isValid: false,
	})

	// valid
	dManConfigValid := diskmanager.DiskManagerConfig{
		DataFile: fmt.Sprintf("%s_valid", dbFile),
	}
	dManValid, err := diskmanager.NewDiskManager(dManConfigValid)
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to create new diskmanager: %s", err.Error()), t)
	}

	tests = append(tests, tTest{
		name:         "test_newpager_valid",
		diskMan:      dManValid,
		freelistFile: fmt.Sprintf("%s_valid", freelistFile),
		isValid:      true,
	})

	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			pgr, err := NewPager(PagerConfig{
				FreeListFile: v.freelistFile,
				DManager:     v.diskMan,
			})

			if v.isValid {
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %s", err.Error()), t)
				}

				if pgr == nil {
					helpers.PrintTestErrorMsg("Expected pager, got nil.", t)
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%s success", v.name))

				pgr.Close()
			} else {
				if err == nil {
					helpers.PrintTestErrorMsg("Expected error, got nil", t)
				}

				if !errors.As(err, &PagerError{}) {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected PagerError, got %v", err), t)
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%s success", v.name))
			}
		})
	}
}
