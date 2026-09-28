// reconcile 对一张券做一致性检查，退出码 0 表示安静且账平，2 表示还没消化完或账不平。
// 压测脚本和故障注入都调用它，而不是肉眼对数字。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/victorzhong0110/teabreak/internal/config"
	"github.com/victorzhong0110/teabreak/internal/order"
	"github.com/victorzhong0110/teabreak/internal/reconcile"
	"github.com/victorzhong0110/teabreak/internal/stock"
)

func main() {
	couponID := flag.Int64("coupon", 0, "券 id")
	wait := flag.Duration("wait", 0, "在这段时间内反复检查，直到 consistent 或超时")
	flag.Parse()
	if *couponID <= 0 {
		fmt.Fprintln(os.Stderr, "need -coupon")
		os.Exit(1)
	}
	cfg := config.Load()
	db, err := order.Open(cfg.MySQLDSN)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	checker := reconcile.New(order.NewRepo(db), stock.New(rdb, cfg.StreamKey, cfg.IdemTTL), rdb, cfg.StreamKey, cfg.DLQKey, cfg.Group)

	deadline := time.Now().Add(*wait)
	var rep reconcile.Report
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rep, err = checker.Check(ctx, *couponID)
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if rep.Consistent || *wait == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(rep)
	if !rep.Consistent || rep.Oversell != 0 {
		os.Exit(2)
	}
}
