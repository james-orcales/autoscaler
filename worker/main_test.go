package main

import (
	"testing"
)

// Does not handle UTF
func TestAllSubstrings(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		// Basic cases
		{"a", []string{"a"}},
		{"ab", []string{"a", "ab", "b"}},
		{"abc", []string{"a", "ab", "abc", "b", "bc", "c"}},
		{"", []string{""}},

		// Control characters (Newline and Tab)
		{"\n", []string{"\n"}},
		{"\t\n", []string{"\t", "\t\n", "\n"}},
		{"a\tb", []string{"a", "a\t", "a\tb", "\t", "\tb", "b"}},

		// Symbols and Punctuation
		{"-!", []string{"-", "-!", "!"}},
		{"..", []string{".", "..", "."}},
	}

	for _, tt := range tests {
		got := AllSubstrings(tt.in)

		// Verify length first
		if len(got) != len(tt.want) {
			t.Fatalf("Input: %q\nGot len: %d\nWant len: %d", tt.in, len(got), len(tt.want))
		}

		// Verify content
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("Input: %q\nIndex: %d\nGot: %q\nWant: %q", tt.in, i, got[i], tt.want[i])
			}
		}
	}
}
