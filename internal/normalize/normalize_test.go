package normalize

import "testing"

func TestQuery(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"  iPhone 15  ", "iphone 15"},
		{"Nike   Air", "nike air"},
		{"", ""},
		{"   ", ""},
		{"Ёлка", "ёлка"},
	}
	for _, tt := range tests {
		if got := Query(tt.in); got != tt.want {
			t.Fatalf("Query(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
