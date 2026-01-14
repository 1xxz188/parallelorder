package parq

import (
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
)

func benchMsgCapacity(n int) uint32 {
	// 避免 Put 因队列容量太小而失败，同时也避免基准测试占用过多内存
	switch {
	case n <= 1024:
		return 1024
	case n >= 1_000_000:
		return 1_000_000
	default:
		return uint32(n)
	}
}

func benchNodeNum(keys int) uint32 {
	// readyChan 的 buffer + nodeNum 限制并发元素数量，给一些冗余避免阻塞
	if keys <= 0 {
		return 1024
	}
	n := keys*2 + 128
	if n < 1024 {
		n = 1024
	}
	return uint32(n)
}

func makeBenchKeys(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "k" + strconv.Itoa(i)
	}
	return keys
}

func BenchmarkPush_SingleKey(b *testing.B) {
	workNums := []uint32{
		1,
		uint32(runtime.GOMAXPROCS(0)),
		uint32(runtime.GOMAXPROCS(0)) * 4,
	}

	for _, workNum := range workNums {
		b.Run(fmt.Sprintf("work=%d", workNum), func(b *testing.B) {
			var processed int64
			done := make(chan struct{})

			handle := func(pq *Parq[string, int], key string, data int) {
				if atomic.AddInt64(&processed, 1) == int64(b.N) {
					close(done)
				}
			}

			opt := DefaultOptionsString(handle).
				WithWorkNum(workNum).
				WithNodeNum(benchNodeNum(1)).
				WithMsgCapacity(benchMsgCapacity(b.N)).
				WithOneCallCnt(32)

			entity, err := New[string, int](opt)
			if err != nil {
				b.Fatalf("New failed: %v", err)
			}
			defer entity.Stop()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := entity.Push("key", i); err != nil {
					b.Fatalf("Push failed: %v", err)
				}
			}
			<-done
			b.StopTimer()
		})
	}
}

func BenchmarkPush_MultiKey(b *testing.B) {
	workNum := uint32(runtime.GOMAXPROCS(0))
	keyCounts := []int{16, 256, 2048}

	for _, keyCount := range keyCounts {
		keys := makeBenchKeys(keyCount)
		b.Run(fmt.Sprintf("keys=%d/work=%d", keyCount, workNum), func(b *testing.B) {
			var processed int64
			done := make(chan struct{})

			handle := func(pq *Parq[string, int], key string, data int) {
				if atomic.AddInt64(&processed, 1) == int64(b.N) {
					close(done)
				}
			}

			opt := DefaultOptionsString(handle).
				WithWorkNum(workNum).
				WithNodeNum(benchNodeNum(keyCount)).
				WithMsgCapacity(benchMsgCapacity(max(1024, b.N/keyCount))).
				WithOneCallCnt(32)

			entity, err := New[string, int](opt)
			if err != nil {
				b.Fatalf("New failed: %v", err)
			}
			defer entity.Stop()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := keys[i%keyCount]
				if err := entity.Push(key, i); err != nil {
					b.Fatalf("Push failed: %v", err)
				}
			}
			<-done
			b.StopTimer()
		})
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func BenchmarkPushParallel_SingleKey(b *testing.B) {
	workNums := []uint32{
		1,
		uint32(runtime.GOMAXPROCS(0)),
		uint32(runtime.GOMAXPROCS(0)) * 4,
	}

	for _, workNum := range workNums {
		b.Run(fmt.Sprintf("work=%d", workNum), func(b *testing.B) {
			var processed int64
			var doneClosed atomic.Bool
			done := make(chan struct{})

			handle := func(pq *Parq[string, int], key string, data int) {
				if atomic.AddInt64(&processed, 1) == int64(b.N) {
					if doneClosed.CompareAndSwap(false, true) {
						close(done)
					}
				}
			}

			opt := DefaultOptionsString(handle).
				WithWorkNum(workNum).
				WithNodeNum(benchNodeNum(1)).
				WithMsgCapacity(benchMsgCapacity(b.N)).
				WithOneCallCnt(32)

			entity, err := New[string, int](opt)
			if err != nil {
				b.Fatalf("New failed: %v", err)
			}
			defer entity.Stop()

			var pushFailed atomic.Bool
			var pushErr error

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := entity.Push("key", 1); err != nil {
						if pushFailed.CompareAndSwap(false, true) {
							pushErr = err
						}
						return
					}
				}
			})
			if pushErr != nil {
				b.Fatalf("Push failed: %v", pushErr)
			}
			<-done
			b.StopTimer()
		})
	}
}

func BenchmarkPushParallel_MultiKey(b *testing.B) {
	workNum := uint32(runtime.GOMAXPROCS(0))
	keyCounts := []int{16, 256, 2048}

	for _, keyCount := range keyCounts {
		keys := makeBenchKeys(keyCount)
		b.Run(fmt.Sprintf("keys=%d/work=%d", keyCount, workNum), func(b *testing.B) {
			var processed int64
			var doneClosed atomic.Bool
			done := make(chan struct{})

			handle := func(pq *Parq[string, int], key string, data int) {
				if atomic.AddInt64(&processed, 1) == int64(b.N) {
					if doneClosed.CompareAndSwap(false, true) {
						close(done)
					}
				}
			}

			opt := DefaultOptionsString(handle).
				WithWorkNum(workNum).
				WithNodeNum(benchNodeNum(keyCount)).
				WithMsgCapacity(benchMsgCapacity(max(1024, b.N/keyCount))).
				WithOneCallCnt(32)

			entity, err := New[string, int](opt)
			if err != nil {
				b.Fatalf("New failed: %v", err)
			}
			defer entity.Stop()

			// 用全局递增序列分配 key，确保并发下 key 分布更均匀
			var seq uint64
			var pushFailed atomic.Bool
			var pushErr error

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := atomic.AddUint64(&seq, 1) - 1
					key := keys[int(i%uint64(keyCount))]
					if err := entity.Push(key, 1); err != nil {
						if pushFailed.CompareAndSwap(false, true) {
							pushErr = err
						}
						return
					}
				}
			})
			if pushErr != nil {
				b.Fatalf("Push failed: %v", pushErr)
			}
			<-done
			b.StopTimer()
		})
	}
}
