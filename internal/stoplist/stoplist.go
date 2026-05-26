package stoplist

import (
	"strings"
	"sync"
)

type List struct {
	mu    sync.RWMutex
	words map[string]struct{}
}

func New(initial ...string) *List {
	sl := &List{words: make(map[string]struct{})}
	for _, w := range initial {
		sl.Add(w)
	}
	return sl
}

func (l *List) Add(word string) bool {
	word = strings.TrimSpace(strings.ToLower(word))
	if word == "" {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.words[word]; ok {
		return false
	}
	l.words[word] = struct{}{}
	return true
}

func (l *List) Remove(word string) bool {
	word = strings.TrimSpace(strings.ToLower(word))
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.words[word]; !ok {
		return false
	}
	delete(l.words, word)
	return true
}

func (l *List) Contains(query string) bool {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if _, ok := l.words[query]; ok {
		return true
	}
	for word := range l.words {
		if strings.Contains(query, word) {
			return true
		}
	}
	return false
}

func (l *List) Snapshot() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]string, 0, len(l.words))
	for w := range l.words {
		out = append(out, w)
	}
	return out
}
