package pager

import (
	"errors"
	"fmt"
	"os"
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
		dbFile       string
		isValid      bool
	}

	tests := make([]tTest, 0)

	// no freelist file
	tests = append(tests, tTest{
		name:    "test_newpager_no_freelist",
		diskMan: dMan,
		dbFile:  dbFile,
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
	dbFileValid := fmt.Sprintf("%s_valid", dbFile)
	dManConfigValid := diskmanager.DiskManagerConfig{
		DataFile: dbFileValid,
	}
	dManValid, err := diskmanager.NewDiskManager(dManConfigValid)
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to create new diskmanager: %s", err.Error()), t)
	}

	tests = append(tests, tTest{
		name:         "test_newpager_valid",
		diskMan:      dManValid,
		freelistFile: fmt.Sprintf("%s_valid", freelistFile),
		dbFile:       dbFileValid,
		isValid:      true,
	})

	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			helpers.PrintInfoMsg(fmt.Sprintf("Freelist file -> %s", v.freelistFile))
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

				// ensure that the files have been created
				var expectedFlFile string

				if len(v.freelistFile) == 0 {
					expectedFlFile = fmt.Sprintf("%s_fl", DEFAULT_FREELIST_FILE)
				} else {
					expectedFlFile = fmt.Sprintf("%s_fl", v.freelistFile)
				}

				_, err := os.Stat(expectedFlFile)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("freelist file not created: %s.", err.Error()), t)
				}

				_, err = os.Stat(v.dbFile)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("dbFile not created: %s", err.Error()), t)
				}

				pgr.Close()
				helpers.PrintSuccessMsg(fmt.Sprintf("%s success", v.name))
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
