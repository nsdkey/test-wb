package normalize

import (
	"strings"
	"unicode"
)

func Query(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(raw))
	prevSpace := false
	for _, r := range raw {
		if unicode.IsSpace(r) {
			if !prevSpace {
				b.WriteRune(' ')
				prevSpace = true
			}
			continue
		}
		b.WriteRune(r)
		prevSpace = false
	}
	return strings.TrimSpace(b.String())
}
