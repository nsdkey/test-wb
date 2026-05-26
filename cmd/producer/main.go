package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"

	"trending/internal/contract"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "kafka brokers")
	topic := flag.String("topic", "search.events", "kafka topic")
	query := flag.String("query", "iphone 15", "search query")
	session := flag.String("session", "demo-session", "session id")
	count := flag.Int("count", 1, "number of events")
	flag.Parse()

	w := &kafka.Writer{
		Addr:     kafka.TCP(*brokers),
		Topic:    *topic,
		Balancer: &kafka.Hash{},
	}
	defer w.Close()

	ctx := context.Background()
	for i := 0; i < *count; i++ {
		ev := contract.SearchEvent{
			Query:     *query,
			Timestamp: time.Now().UTC(),
			SessionID: *session,
		}
		payload, err := json.Marshal(ev)
		if err != nil {
			log.Fatal(err)
		}
		err = w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(ev.SessionID),
			Value: payload,
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("sent %d event(s) query=%q session=%s", *count, *query, *session)
	os.Exit(0)
}
