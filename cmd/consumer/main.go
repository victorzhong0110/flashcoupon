// consumer 进程消费 Redis Stream，把订单写入 MySQL，
// 同时负责超时取消还库存，以及热点分桶之间的再平衡。
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/victorzhong0110/teabreak/internal/config"
	"github.com/victorzhong0110/teabreak/internal/order"
	"github.com/victorzhong0110/teabreak/internal/stock"
	"github.com/victorzhong0110/teabreak/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}
	db, err := order.Open(cfg.MySQLDSN)
	if err != nil {
		log.Error("mysql", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := order.Migrate(db); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		PoolSize:     64,
		MinIdleConns: 8,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	defer rdb.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		log.Info("consumer metrics", "addr", cfg.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics", "err", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	w := &worker.Worker{
		RDB:               rdb,
		Repo:              order.NewRepo(db),
		Stock:             stock.New(rdb, cfg.StreamKey, cfg.IdemTTL),
		Stream:            cfg.StreamKey,
		DLQ:               cfg.DLQKey,
		Group:             cfg.Group,
		Workers:           cfg.Workers,
		MaxDeliveries:     cfg.MaxDeliveries,
		ClaimIdle:         cfg.ClaimIdle,
		CancelEvery:       cfg.CancelEvery,
		CancelParallel:    cfg.CancelParallel,
		CancelBatch:       cfg.CancelBatch,
		CancelFreshWindow: cfg.CancelFreshWindow,
		RebalanceEvery:    cfg.RebalanceEvery,
		RebalanceGap:      cfg.RebalanceGap,
		Log:               log,
	}
	if err := w.Run(ctx); err != nil {
		log.Error("worker", "err", err)
		os.Exit(1)
	}
	shut, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shut)
}
