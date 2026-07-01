package config

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// KnownFailure describes a test that is expected to fail, with a link to the PR
// tracking the fix and a short human-readable reason.
type KnownFailure struct {
	PR   string `json:"pr"`
	Note string `json:"note"`
}

// NormalizeTestKey returns the extension-less "<api>/<test>" key used to match a
// test against the known-failures map (e.g. "eth_getBalance/test_40.json" ->
// "eth_getBalance/test_40").
func NormalizeTestKey(name string) string {
	return strings.TrimSuffix(name, filepath.Ext(name))
}

// LoadKnownFailures reads the known-failures JSON file at path. A missing file is
// not an error (returns an empty map), so the list is entirely optional. Keys are
// normalized to be extension-insensitive.
func LoadKnownFailures(path string) (map[string]KnownFailure, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return map[string]KnownFailure{}, nil
		}
		return nil, err
	}

	var raw map[string]KnownFailure
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	out := make(map[string]KnownFailure, len(raw))
	for k, v := range raw {
		out[NormalizeTestKey(k)] = v
	}
	return out, nil
}
