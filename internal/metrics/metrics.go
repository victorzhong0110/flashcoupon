// Package metrics 是进程内的 Prometheus 指标。
// 标签只用结果和策略，不用 user_id / order_id，否则基数会把 Prometheus 打爆。
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	GrabTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "teabreak_grab_requests_total",
		Help: "抢券请求次数，按结果和库存策略拆开。",
	}, []string{"result", "strategy"})

	GrabDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "teabreak_grab_duration_seconds",
		Help:    "抢券接口耗时（含限流和 Redis/DB）。",
		Buckets: []float64{.0005, .001, .002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"strategy"})

	ConsumeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "teabreak_consume_total",
		Help: "订单消费者处理结果。duplicate 表示幂等命中，属于正常重试。",
	}, []string{"result"})

	DLQTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "teabreak_dlq_messages_total",
		Help: "超过最大投递次数后进入死信流的消息数。",
	})

	CancelTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "teabreak_cancel_total",
		Help: "超时取消次数。returned 表示库存已经还回去。",
	}, []string{"result"})

	RebalanceTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "teabreak_rebalance_moves_total",
		Help: "分桶之间成功搬运库存的次数。",
	})

	CacheLoadTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "teabreak_cache_load_total",
		Help: "券模板缓存的命中层级。bloom 表示非法 id 被挡在数据库外面。",
	}, []string{"source"})

	StockRemaining = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "teabreak_stock_remaining",
		Help: "Redis 各分桶剩余库存之和。只由 API 进程刷新。",
	}, []string{"coupon_id"})

	MQPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "teabreak_mq_pending",
		Help: "消费组里已投递但未 ACK 的消息数（XPENDING）。",
	})

	MQLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "teabreak_mq_lag",
		Help: "还没有投递给消费组的消息数（Redis 7 XINFO GROUPS lag）。",
	})
)
