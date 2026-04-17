package buffermanager

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/pager"
)

func InitPager(t *testing.T) *pager.Pager {
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "baobab.db")
	dman, err := diskmanager.NewDiskManager(diskmanager.DiskManagerConfig{DataFile: dbFile})

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Could not initialize disk manager: %s", err.Error()), t)
	}

	freelistFile := filepath.Join(t.TempDir(), "baobab")
	pgr, err := pager.NewPager(pager.PagerConfig{DManager: dman, FreeListFile: freelistFile})
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Could not initialize pager: %s", err.Error()), t)
	}

	return pgr
}
