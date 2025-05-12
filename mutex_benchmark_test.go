// Copyright(c) 2025 Visvasity LLC

package idlock

import (
	"context"
	"sync"
	"testing"
)

// BenchmarkMutexContention measures Lock/Unlock times with varying goroutine counts
func BenchmarkMutexContention(b *testing.B) {
	// Test with 1, 2, 4, 8, 16, 32 goroutines
	for _, routines := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(
			// Name the sub-benchmark with the number of goroutines
			"N="+string(rune(routines+'0')),
			// Benchmark function for specific number of goroutines
			func(b *testing.B) {
				var mu sync.Mutex
				var wg sync.WaitGroup

				// Barrier setup: ready channel to signal goroutine readiness, start channel to signal benchmark start
				ready := make(chan struct{}, routines)
				start := make(chan struct{})
				// Each goroutine will perform b.N / routines iterations
				iterations := b.N / routines
				if iterations == 0 {
					iterations = 1
				}

				// Launch specified number of goroutines
				for i := 0; i < routines; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						// Signal readiness
						ready <- struct{}{}
						// Wait for start signal
						<-start
						// Perform Lock/Unlock iterations
						for j := 0; j < iterations; j++ {
							mu.Lock()
							mu.Unlock()
						}
					}()
				}

				// Wait for all goroutines to be ready
				for i := 0; i < routines; i++ {
					<-ready
				}

				b.ResetTimer()
				defer b.StopTimer()

				// Close start channel to release all goroutines simultaneously
				close(start)
				// Wait for all goroutines to complete
				wg.Wait()
			},
		)
	}
}

// BenchmarkIDLockMutexContention measures Lock/Unlock times with varying goroutine counts
func BenchmarkIDLockMutexContention(b *testing.B) {
	ctx := context.Background()

	// Test with 1, 2, 4, 8, 16, 32 goroutines
	for _, routines := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(
			// Name the sub-benchmark with the number of goroutines
			"N="+string(rune(routines+'0')),
			// Benchmark function for specific number of goroutines
			func(b *testing.B) {
				var wg sync.WaitGroup

				// Barrier setup: ready channel to signal goroutine readiness, start channel to signal benchmark start
				ready := make(chan struct{}, routines)
				start := make(chan struct{})
				// Each goroutine will perform b.N / routines iterations
				iterations := b.N / routines
				if iterations == 0 {
					iterations = 1
				}

				var mu Mutex

				// Launch specified number of goroutines
				for i := 0; i < routines; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						// Signal readiness
						ready <- struct{}{}
						// Wait for start signal
						<-start
						// Perform Lock/Unlock iterations
						for j := 0; j < iterations; j++ {
							unlock, err := mu.Lock(ctx, i)
							if err != nil {
								b.Fatal(err)
							}
							unlock()
						}
					}()
				}

				// Wait for all goroutines to be ready
				for i := 0; i < routines; i++ {
					<-ready
				}

				b.ResetTimer()
				defer b.StopTimer()

				// Close start channel to release all goroutines simultaneously
				close(start)
				// Wait for all goroutines to complete
				wg.Wait()
			},
		)
	}
}
