// Package config 从环境变量读配置。默认值按 docker compose 的网络起得来，
// 本机直连时用 127.0.0.1 的映射端口覆盖。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr    string
	MetricsAddr string

	MySQLDSN  string
	RedisAddr string

	// StreamKey / DLQKey / Group 决定消费者从哪条流读。
	// 测试会换成独立的 stream，避免和正在跑的 compose 消费者抢消息。
	StreamKey string
	DLQKey    string
	Group     string

	Workers       int
	MaxDeliveries int64
	ClaimIdle     time.Duration

	// GlobalRPS <= 0 表示关闭全局限流。压测默认关闭，才能测到库存路径本身的上限。
	GlobalRPS   float64
	GlobalBurst int
	// UserLimit 是单个用户在 UserWindow 内最多打几次抢券接口（滑动窗口）。
	UserLimit  int
	UserWindow time.Duration

	RebalanceEvery time.Duration
	// RebalanceGap 是最满桶和最空桶的库存差超过多少才搬运。
	RebalanceGap int
	CancelEvery  time.Duration
	// CancelParallel 是同时扫过期单的协程数。一半只看刚到期的时间窗，一半清旧积压。
	CancelParallel int
	// CancelBatch 是每个协程一次用 SKIP LOCKED 领走的行数。
	CancelBatch int
	// CancelFreshWindow 是“刚到期”窗口。窗口内的单不跟几十万张旧积压排队。
	CancelFreshWindow time.Duration

	IdemTTL time.Duration

	CacheSoftTTL time.Duration
	CacheHardTTL time.Duration
	CacheNegTTL  time.Duration

	// CombinedHotpath 把幂等回放、用户滑动窗口和扣库存并进一条 Lua。
	// 关掉时保持原来的三次往返，用来做同一套流量下的前后对比。
	CombinedHotpath bool
	// SnowflakeWorker 是雪花算法的 worker id。多实例必须各不相同，否则订单号会撞。
	SnowflakeWorker int64
}

func Load() Config {
	return Config{
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		MetricsAddr:       env("METRICS_ADDR", ":8081"),
		MySQLDSN:          env("MYSQL_DSN", "root:root@tcp(127.0.0.1:13306)/coupon?parseTime=true&charset=utf8mb4&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s"),
		RedisAddr:         env("REDIS_ADDR", "127.0.0.1:16379"),
		StreamKey:         env("STREAM_KEY", "tb:stream:orders"),
		DLQKey:            env("DLQ_KEY", "tb:stream:orders:dlq"),
		Group:             env("CONSUMER_GROUP", "order-writers"),
		Workers:           envInt("WORKERS", 8),
		MaxDeliveries:     int64(envInt("MAX_DELIVERIES", 8)),
		ClaimIdle:         envDur("CLAIM_IDLE", 15*time.Second),
		GlobalRPS:         envFloat("GLOBAL_RPS", 0),
		GlobalBurst:       envInt("GLOBAL_BURST", 2000),
		UserLimit:         envInt("USER_LIMIT", 30),
		UserWindow:        envDur("USER_WINDOW", 10*time.Second),
		RebalanceEvery:    envDur("REBALANCE_EVERY", 2*time.Second),
		RebalanceGap:      envInt("REBALANCE_GAP", 8),
		CancelEvery:       envDur("CANCEL_EVERY", time.Second),
		CancelParallel:    envInt("CANCEL_PARALLEL", 4),
		CancelBatch:       envInt("CANCEL_BATCH", 200),
		CancelFreshWindow: envDur("CANCEL_FRESH_WINDOW", 3*time.Second),
		IdemTTL:           envDur("IDEM_TTL", 24*time.Hour),
		CacheSoftTTL:      envDur("CACHE_SOFT_TTL", 30*time.Second),
		CacheHardTTL:      envDur("CACHE_HARD_TTL", 2*time.Minute),
		CacheNegTTL:       envDur("CACHE_NEG_TTL", 20*time.Second),
		CombinedHotpath:   envBool("COMBINED_HOTPATH", false),
		SnowflakeWorker:   int64(envInt("SNOWFLAKE_WORKER", 1)),
	}
}

func (c Config) Validate() error {
	if c.MySQLDSN == "" || c.RedisAddr == "" {
		return fmt.Errorf("MYSQL_DSN and REDIS_ADDR are required")
	}
	if c.Workers < 1 {
		return fmt.Errorf("WORKERS must be >= 1")
	}
	if c.MaxDeliveries < 1 {
		return fmt.Errorf("MAX_DELIVERIES must be >= 1")
	}
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func envDur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
