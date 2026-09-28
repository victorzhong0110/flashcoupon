// 布隆过滤器只用来挡“根本不存在的券 id”。
// 它没有假阴性：加进去的 id 一定能查到。没加进去的 id 有小概率被误判为存在，
// 那种请求会落到后面的缓存和数据库，再用短 TTL 的空值接住。
package cache

import (
	"hash/fnv"
	"sync"
)

type Bloom struct {
	mu   sync.RWMutex
	bits []uint64
	m    uint64
	k    int
}

func NewBloom(bits, k int) *Bloom {
	if bits < 64 {
		bits = 64
	}
	if k < 1 {
		k = 1
	}
	n := (bits + 63) / 64
	return &Bloom{bits: make([]uint64, n), m: uint64(n * 64), k: k}
}

func (b *Bloom) Add(id int64) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	h1, h2 := hashPair(id)
	for i := 0; i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % b.m
		b.bits[pos/64] |= 1 << (pos % 64)
	}
}

func (b *Bloom) MayContain(id int64) bool {
	if b == nil {
		return true
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	h1, h2 := hashPair(id)
	for i := 0; i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % b.m
		if b.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

func hashPair(id int64) (uint64, uint64) {
	h := fnv.New64a()
	var buf [8]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(uint64(id) >> (8 * i))
	}
	_, _ = h.Write(buf[:])
	h1 := h.Sum64()
	h2 := h1>>1 | 1 // 奇数步长，避免退化为 0
	return h1, h2
}
