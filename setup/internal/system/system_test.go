package system

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateInstallDir(t *testing.T) {
	tempDir := t.TempDir()

	// New directory
	res := ValidateInstallDir(tempDir)
	if !res.Valid {
		t.Fatalf("expected valid for tempDir, got error: %s", res.Error)
	}
	if res.HasConfig {
		t.Fatalf("expected HasConfig false for fresh directory")
	}

	// Create config.yaml
	testConfig := "TCP: 1883\nNodeName: testnode\n"
	err := os.WriteFile(filepath.Join(tempDir, "config.yaml"), []byte(testConfig), 0644)
	if err != nil {
		t.Fatalf("failed to write config.yaml: %v", err)
	}

	resWithConfig := ValidateInstallDir(tempDir)
	if !resWithConfig.Valid {
		t.Fatalf("expected valid for tempDir with config: %s", resWithConfig.Error)
	}
	if !resWithConfig.HasConfig {
		t.Fatalf("expected HasConfig true")
	}
	if resWithConfig.RawConfig != testConfig {
		t.Fatalf("expected rawConfig '%s', got '%s'", testConfig, resWithConfig.RawConfig)
	}
}
