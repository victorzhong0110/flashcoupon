package cache

import "testing"

func TestBloomNoFalseNegative(t *testing.T) {
	b := NewBloom(1<<16, 4)
	for id := int64(1); id <= 1000; id++ {
		b.Add(id)
	}
	for id := int64(1); id <= 1000; id++ {
		if !b.MayContain(id) {
			t.Fatalf("false negative %d", id)
		}
	}
	// 不要求零假阳性，只要求远处的 id 大多被拒绝。100 个里如果全被放行，过滤器就没起作用。
	var passed int
	for id := int64(1_000_000); id < 1_000_100; id++ {
		if b.MayContain(id) {
			passed++
		}
	}
	if passed > 5 {
		t.Fatalf("bloom let through %d/100 unknown ids", passed)
	}
}
