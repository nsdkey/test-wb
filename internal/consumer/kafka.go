package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"trending/internal/aggregator"
	"trending/internal/config"
	"trending/internal/contract"
)

type Consumer struct {
	reader *kafka.Reader
	agg    *aggregator.Aggregator
	log    *slog.Logger
}

func New(cfg config.Config, agg *aggregator.Aggregator, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.KafkaGroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
	return &Consumer{reader: reader, agg: agg, log: log}
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.reader.Close()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Error("kafka fetch failed", "err", err)
			continue
		}

		var ev contract.SearchEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			c.log.Warn("invalid kafka payload", "err", err, "offset", msg.Offset)
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if ev.Timestamp.IsZero() {
			ev.Timestamp = msg.Time
		}
		if ev.Timestamp.IsZero() {
			ev.Timestamp = time.Now().UTC()
		}

		c.agg.Record(ev)
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.log.Error("kafka commit failed", "err", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
