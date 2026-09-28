package idgen

import "testing"

func TestSnowflakeUnique(t *testing.T) {
	g := New(7)
	seen := make(map[int64]struct{}, 20000)
	for i := 0; i < 20000; i++ {
		id := g.Next()
		if id <= 0 {
			t.Fatalf("id must be positive, got %d", id)
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestSnowflakeMonotonicWithinProcess(t *testing.T) {
	g := New(1)
	prev := g.Next()
	for i := 0; i < 1000; i++ {
		id := g.Next()
		if id <= prev {
			t.Fatalf("id went backwards: %d then %d", prev, id)
		}
		prev = id
	}
}
