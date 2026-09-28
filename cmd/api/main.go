// api 进程只做同步的事：限流、读券模板、执行 Lua 预扣、把结果返回给客户端。
// 订单落库在 consumer 进程。两边通过 Redis Stream 交接。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/victorzhong0110/teabreak/internal/cache"
	"github.com/victorzhong0110/teabreak/internal/config"
	"github.com/victorzhong0110/teabreak/internal/httpserver"
	"github.com/victorzhong0110/teabreak/internal/idgen"
	"github.com/victorzhong0110/teabreak/internal/model"
	"github.com/victorzhong0110/teabreak/internal/order"
	"github.com/victorzhong0110/teabreak/internal/ratelimit"
	"github.com/victorzhong0110/teabreak/internal/reconcile"
	"github.com/victorzhong0110/teabreak/internal/seckill"
	"github.com/victorzhong0110/teabreak/internal/stock"
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
		PoolSize:     200,
		MinIdleConns: 20,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	defer rdb.Close()

	repo := order.NewRepo(db)
	st := stock.New(rdb, cfg.StreamKey, cfg.IdemTTL)
	meta := cache.NewMeta(rdb, func(ctx context.Context, id int64) (*model.Coupon, error) {
		return repo.GetCoupon(ctx, id)
	}, cache.Options{
		SoftTTL: cfg.CacheSoftTTL,
		HardTTL: cfg.CacheHardTTL,
		NegTTL:  cfg.CacheNegTTL,
	})
	svc := seckill.New(repo, st, meta, ratelimit.NewRedis(rdb), idgen.New(cfg.SnowflakeWorker), cfg)
	var warmErr error
	for i := 0; i < 30; i++ {
		warmErr = svc.Warmup(context.Background())
		if warmErr == nil {
			break
		}
		log.Warn("warmup retry", "err", warmErr, "try", i)
		time.Sleep(time.Second)
	}
	if warmErr != nil {
		log.Error("warmup", "err", warmErr)
		os.Exit(1)
	}
	checker := reconcile.New(repo, st, rdb, cfg.StreamKey, cfg.DLQKey, cfg.Group)
	engine := httpserver.New(svc, checker, log, os.Getenv("PPROF") != "0")

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           engine,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				svc.RefreshStockGauges(context.Background())
			}
		}
	}()
	if os.Getenv("PPROF") != "0" {
		// pprof 挂在独立端口，避免和业务路由缠在一起。演示才开，不要映射到公网。
		go func() {
			log.Info("pprof", "addr", ":6060")
			if err := http.ListenAndServe(":6060", nil); err != nil {
				log.Error("pprof", "err", err)
			}
		}()
	}
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen", "err", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()
	shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shut)
}
