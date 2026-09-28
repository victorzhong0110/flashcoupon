// Package idgen 是进程内的雪花算法。
//
// 布局（63 bit，最高位留 0 保证是正数）：
//
//	41 bit 毫秒时间差 | 10 bit worker | 12 bit 毫秒内序列
//
// 订单号如果改成 MySQL AUTO_INCREMENT，Redis 异步落库和 DB 直写会抢同一段 id，
// 容易撞车。雪花在进程内生成，DB 直写路径就不必再访问 Redis。
// 代价是时钟大幅回拨时会阻塞等到时间追上。
package idgen

import (
	"sync"
	"time"
)

// epoch 取一个固定时刻，让 id 更短，也避免依赖 1970。
const epoch int64 = 1704067200000 // 2024-01-01 UTC 毫秒

type Generator struct {
	mu     sync.Mutex
	worker int64
	last   int64
	seq    int64
}

func New(workerID int64) *Generator {
	if workerID < 0 {
		workerID = -workerID
	}
	return &Generator{worker: workerID & 1023}
}

func (g *Generator) Next() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now().UnixMilli() - epoch
	if now < g.last {
		// 时钟回拨：等到逻辑时钟追上，避免发出重复 id。
		for now < g.last {
			time.Sleep(time.Millisecond)
			now = time.Now().UnixMilli() - epoch
		}
	}
	if now == g.last {
		g.seq = (g.seq + 1) & 4095
		if g.seq == 0 {
			for now <= g.last {
				now = time.Now().UnixMilli() - epoch
			}
		}
	} else {
		g.seq = 0
	}
	g.last = now
	return (now << 22) | (g.worker << 12) | g.seq
}
