package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"trending/internal/aggregator"
	"trending/internal/api"
	"trending/internal/config"
	"trending/internal/consumer"
	"trending/internal/stoplist"
)

func main() {
	cfg := config.Load()
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	sl := stoplist.New()
	agg := aggregator.New(cfg, sl)
	go agg.Start(ctx)

	kafkaConsumer := consumer.New(cfg, agg, log)
	go func() {
		if err := kafkaConsumer.Run(ctx); err != nil {
			log.Error("kafka consumer stopped", "err", err)
			cancel()
		}
	}()

	server := api.NewServer(cfg, agg, sl)

	go func() {
		log.Info("http server starting", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(cfg.HTTPAddr); err != nil && err != http.ErrServerClosed {
			log.Error("http server failed", "err", err)
			cancel()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	_, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = kafkaConsumer.Close()
}
