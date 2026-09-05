package install

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCopySetupExecutable(t *testing.T) {
	tempDir := t.TempDir()

	err := copySetupExecutable(tempDir)
	if err != nil {
		t.Fatalf("copySetupExecutable failed: %v", err)
	}

	expectedName := "setup"
	if runtime.GOOS == "windows" {
		expectedName = "setup.exe"
	}

	copiedPath := filepath.Join(tempDir, expectedName)
	info, err := os.Stat(copiedPath)
	if err != nil {
		t.Fatalf("copied executable not found at %s: %v", copiedPath, err)
	}

	if info.Size() == 0 {
		t.Fatalf("copied executable is 0 bytes")
	}

	if runtime.GOOS != "windows" {
		if info.Mode()&0111 == 0 {
			t.Fatalf("copied executable does not have execute permissions: %v", info.Mode())
		}
	}

	// Test idempotency: calling copySetupExecutable with the same destDir when already present
	err = copySetupExecutable(tempDir)
	if err != nil {
		t.Fatalf("second copySetupExecutable call failed: %v", err)
	}
}
