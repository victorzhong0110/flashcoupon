package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestLocalBucketExactBurstWhenNoRefill(t *testing.T) {
	// rate=0：不补充。并发下放行次数必须恰好等于桶容量，这是 -race 要守的不变量。
	b := NewLocal(0, 100)
	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if b.Allow() {
					ok.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := ok.Load(); got != 100 {
		t.Fatalf("allowed %d, want 100", got)
	}
}

func TestLocalBucketDisabled(t *testing.T) {
	b := NewLocal(-1, 1)
	for i := 0; i < 10; i++ {
		if !b.Allow() {
			t.Fatal("negative rate must disable the bucket")
		}
	}
}
