package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"trending/internal/aggregator"
	"trending/internal/config"
	"trending/internal/stoplist"
)

type Server struct {
	cfg      config.Config
	agg      *aggregator.Aggregator
	stoplist *stoplist.List
	mux      *http.ServeMux
}

func NewServer(cfg config.Config, agg *aggregator.Aggregator, sl *stoplist.List) *Server {
	s := &Server{
		cfg:      cfg,
		agg:      agg,
		stoplist: sl,
		mux:      http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /api/v1/top", s.handleTop)
	s.mux.HandleFunc("GET /api/v1/stoplist", s.handleStoplistGet)
	s.mux.HandleFunc("POST /api/v1/stoplist", s.handleStoplistAdd)
	s.mux.HandleFunc("DELETE /api/v1/stoplist", s.handleStoplistDelete)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleTop(w http.ResponseWriter, r *http.Request) {
	n := s.cfg.DefaultTopN
	if raw := r.URL.Query().Get("n"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeError(w, http.StatusBadRequest, "invalid query parameter n")
			return
		}
		n = parsed
	}

	resp := s.agg.Top(n)
	writeJSON(w, http.StatusOK, resp)
}

type stoplistModifyRequest struct {
	Word string `json:"word"`
}

func (s *Server) handleStoplistGet(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"words": s.stoplist.Snapshot(),
	})
}

func (s *Server) handleStoplistAdd(w http.ResponseWriter, r *http.Request) {
	word, err := wordFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	added := s.stoplist.Add(word)
	writeJSON(w, http.StatusOK, map[string]any{
		"word":  strings.ToLower(strings.TrimSpace(word)),
		"added": added,
	})
}

func (s *Server) handleStoplistDelete(w http.ResponseWriter, r *http.Request) {
	word, err := wordFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	removed := s.stoplist.Remove(word)
	writeJSON(w, http.StatusOK, map[string]any{
		"word":    strings.ToLower(strings.TrimSpace(word)),
		"removed": removed,
	})
}

func wordFromRequest(r *http.Request) (string, error) {
	if w := strings.TrimSpace(r.URL.Query().Get("word")); w != "" {
		return w, nil
	}
	var body stoplistModifyRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return "", errEmptyWord
	}
	if strings.TrimSpace(body.Word) == "" {
		return "", errEmptyWord
	}
	return body.Word, nil
}

var errEmptyWord = &badRequestError{msg: "word is required"}

type badRequestError struct{ msg string }

func (e *badRequestError) Error() string { return e.msg }

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=1")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) ListenAndServe(addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return srv.ListenAndServe()
}
