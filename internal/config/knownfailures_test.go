package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadKnownFailures_MissingFileIsEmpty(t *testing.T) {
	m, err := LoadKnownFailures(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if err != nil {
		t.Fatalf("missing file should not error, got %v", err)
	}
	if len(m) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(m))
	}
}

func TestLoadKnownFailures_ParsesAndNormalizesKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "known-failures.json")
	content := `{
	  "eth_getBalance/test_40.json": {"pr": "geth#35271", "note": "empty {} block param"},
	  "eth_sendRawTransaction/test_23": {"pr": "geth#35129", "note": "empty raw tx"}
	}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	m, err := LoadKnownFailures(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The .json extension in a key must be normalized away so it matches the
	// runtime test name (which carries the extension).
	if kf, ok := m["eth_getBalance/test_40"]; !ok || kf.PR != "geth#35271" {
		t.Fatalf("expected normalized key eth_getBalance/test_40 with PR geth#35271, got %+v (ok=%v)", kf, ok)
	}
	if kf, ok := m["eth_sendRawTransaction/test_23"]; !ok || kf.PR != "geth#35129" {
		t.Fatalf("expected eth_sendRawTransaction/test_23 with PR geth#35129, got %+v (ok=%v)", kf, ok)
	}
}

func TestLoadKnownFailures_InvalidJSONErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("{not json"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKnownFailures(path); err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestNormalizeTestKey(t *testing.T) {
	cases := map[string]string{
		"eth_getBalance/test_40.json": "eth_getBalance/test_40",
		"eth_getBalance/test_40":      "eth_getBalance/test_40",
		"eth_x/test_1.tar":            "eth_x/test_1",
	}
	for in, want := range cases {
		if got := NormalizeTestKey(in); got != want {
			t.Errorf("NormalizeTestKey(%q) = %q, want %q", in, got, want)
		}
	}
}
