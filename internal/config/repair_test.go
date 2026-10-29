package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRepairFileRebuildsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	original := "prism:\n  region: global\nbridge:\n  totally_unknown_key: 1\n"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected strict load to fail on the unknown key")
	}
	if !Repairable(path) {
		t.Fatal("unknown-key damage must be repairable")
	}

	backup, err := RepairFile(path)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	saved, err := os.ReadFile(backup)
	if err != nil || string(saved) != original {
		t.Fatalf("backup must hold the original content: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("repaired config must load: %v", err)
	}
	if cfg.Prism.Region != PrismRegionGlobal {
		t.Fatalf("known fields must survive the repair, got region=%q", cfg.Prism.Region)
	}
}

func TestRepairFileRejectsSyntaxDamage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("prism: [unclosed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if Repairable(path) {
		t.Fatal("syntax damage must not be repairable")
	}
	if _, err := RepairFile(path); err == nil || !strings.Contains(err.Error(), "not auto-repairable") {
		t.Fatalf("expected refusal, got %v", err)
	}
}

func TestDoctorConfigDefaults(t *testing.T) {
	cfg := Default()
	if !cfg.Doctor.IsEnabled() || !cfg.Doctor.IsAutoHealEnabled() {
		t.Fatal("doctor must default to enabled with auto-heal on")
	}
	disabled := false
	cfg.Doctor.Enabled = &disabled
	cfg.Doctor.AutoHeal = &disabled
	if cfg.Doctor.IsEnabled() || cfg.Doctor.IsAutoHealEnabled() {
		t.Fatal("explicit false must disable")
	}
}

func TestRepairFilePreservesDefaultedFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	// Omits bridge.enabled: Load defaults it to enabled.
	if err := os.WriteFile(path, []byte("prism:\n  region: cn\nbridge:\n  unknown_key: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := RepairFile(path); err != nil {
		t.Fatalf("repair: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load repaired: %v", err)
	}
	if !cfg.Bridge.Enabled {
		t.Fatal("repair must not disable the bridge by materializing zero-value defaults")
	}
}

func TestSavePreservesDoctorSwitches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := Default()
	disabled := false
	cfg.Doctor.AutoHeal = &disabled
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	reloaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Doctor.AutoHeal == nil || *reloaded.Doctor.AutoHeal {
		t.Fatal("doctor.auto_heal=false must survive Config.Save")
	}
}
