package contract

import "time"

type SearchEvent struct {
	Query     string    `json:"query"`
	Timestamp time.Time `json:"timestamp"`
	SessionID string    `json:"session_id"`
}
