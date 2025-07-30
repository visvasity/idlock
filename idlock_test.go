// Copyright (c) 2025 Visvasity LLC

package idlock

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestLockBasic tests basic locking and unlocking of specific IDs.
func TestLockBasic(t *testing.T) {
	var m Mutex[any]
	ctx := context.Background()

	// Lock two IDs
	unlock, err := m.Lock(ctx, "user1", 42)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Verify that the IDs are locked by attempting to lock them again
	_, ok := m.TryLock("user1")
	if ok {
		t.Error("TryLock succeeded on already locked ID 'user1'")
	}
	_, ok = m.TryLock(42)
	if ok {
		t.Error("TryLock succeeded on already locked ID 42")
	}

	// Unlock one ID and verify it can be locked again
	unlock("user1")
	_, ok = m.TryLock("user1")
	if !ok {
		t.Error("TryLock failed on unlocked ID 'user1'")
	}

	// Unlock all remaining IDs
	unlock()
	_, ok = m.TryLock(42)
	if !ok {
		t.Error("TryLock failed on unlocked ID 42")
	}
}

// TestLockAll tests exclusive locking of the entire ID space.
func TestLockAll(t *testing.T) {
	var m Mutex[string]
	ctx := context.Background()

	// Acquire exclusive lock
	unlockAll, err := m.LockAll(ctx)
	if err != nil {
		t.Fatalf("LockAll failed: %v", err)
	}

	// Verify that no other locks can be acquired
	_, ok := m.TryLock("user1")
	if ok {
		t.Error("TryLock succeeded during LockAll")
	}
	_, ok = m.TryLockAll()
	if ok {
		t.Error("TryLockAll succeeded during LockAll")
	}

	// Release exclusive lock and verify new locks can be acquired
	unlockAll()
	_, ok = m.TryLock("user1")
	if !ok {
		t.Error("TryLock failed after unlocking LockAll")
	}
}

// TestConcurrentLock tests concurrent locking of different IDs.
func TestConcurrentLock(t *testing.T) {
	var m Mutex[int]
	ctx := context.Background()
	var wg sync.WaitGroup

	// Launch multiple goroutines to lock different IDs
	for i := range 5 {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			unlock, err := m.Lock(ctx, id)
			if err != nil {
				t.Errorf("Lock failed for ID %d: %v", id, err)
				return
			}
			// Simulate some work
			time.Sleep(50 * time.Millisecond)
			unlock()
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify that all IDs are unlocked
	for i := range 5 {
		_, ok := m.TryLock(i)
		if !ok {
			t.Errorf("ID %d is still locked", i)
		}
	}
}

// TestContextCancellation tests that locking respects context cancellation.
func TestContextCancellation(t *testing.T) {
	var m Mutex[string]
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Lock an ID
	_, err := m.Lock(ctx, "user1")
	if err != nil {
		t.Fatalf("Initial lock failed: %v", err)
	}

	// Attempt to lock the same ID with a canceling context
	_, err = m.Lock(ctx, "user1")
	if err == nil {
		t.Error("Lock succeeded despite context cancellation")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("Expected DeadlineExceeded, got %v", err)
	}
}

// TestPanicCases tests panic scenarios for invalid operations.
func TestPanicCases(t *testing.T) {
	var m Mutex[string]
	ctx := context.Background()

	// Test panic on unlocking non-locked ID
	unlock, err := m.Lock(ctx, "user1")
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for unlocking non-locked ID")
		}
	}()
	unlock("user2")
}

// TestTryLockContention tests TryLock behavior under contention.
func TestTryLockContention(t *testing.T) {
	var m Mutex[string]
	ctx := context.Background()

	// Lock an ID
	unlock, err := m.Lock(ctx, "user1")
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	defer unlock()

	// TryLock should fail due to contention
	_, ok := m.TryLock("user1")
	if ok {
		t.Error("TryLock succeeded on contended ID")
	}

	// TryLock on a different ID should succeed
	_, ok = m.TryLock("user2")
	if !ok {
		t.Error("TryLock failed on uncontended ID")
	}
}

// TestLockAllBlocksOthers tests that LockAll blocks all other lock attempts.
func TestLockAllBlocksOthers(t *testing.T) {
	var m Mutex[any]
	ctx := context.Background()
	var wg sync.WaitGroup

	// Acquire LockAll
	unlockAll, err := m.LockAll(ctx)
	if err != nil {
		t.Fatalf("LockAll failed: %v", err)
	}

	// Attempt to lock specific IDs concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, ok := m.TryLock(101)
		if ok {
			t.Error("TryLock succeeded during LockAll")
		}
	}()

	// Attempt LockAll concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, ok := m.TryLockAll()
		if ok {
			t.Error("TryLockAll succeeded during LockAll")
		}
	}()

	// Release LockAll after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		unlockAll()
	}()

	wg.Wait()
}

func TestTryLockAll(t *testing.T) {
	ctx := context.Background()

	var mutex Mutex[int]

	{
		unlock, ok := mutex.TryLockAll()
		if !ok {
			t.Fatalf("wanted true, got false")
		}
		unlock()
	}

	{
		unlock, ok := mutex.TryLockAll()
		if !ok {
			t.Fatalf("wanted true, got false")
		}
		// t.Logf("mutex.running=%#v", mutex.running)
		// t.Logf("mutex.waiting=%#v", mutex.waiting)
		// t.Logf("len(mutex.lockedIDMap)=%d", len(mutex.lockedIDMap))
		if _, ok := mutex.TryLockAll(); ok {
			t.Fatalf("wanted false, got true")
		}
		unlock()
	}

	{
		unlock, err := mutex.LockAll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := mutex.TryLockAll(); ok {
			t.Fatal("wanted false, got true")
		}
		unlock()
		unlock, ok := mutex.TryLockAll()
		if !ok {
			t.Fatal("wanted true, got false")
		}
		unlock()
	}

	{
		unlock, err := mutex.Lock(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := mutex.TryLockAll(); ok {
			t.Fatal("wanted false, got true")
		}
		unlock()

		unlock2, ok := mutex.TryLockAll()
		if !ok {
			t.Fatalf("wanted true, got false")
		}
		unlock2()
	}

	{
		unlock, err := mutex.Lock(ctx, 10, 100)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := mutex.TryLockAll(); ok {
			t.Fatal("wanted false, got true")
		}
		unlock(100) // Unlock one time at a time.

		if _, ok := mutex.TryLockAll(); ok {
			t.Fatal("wanted false, got true")
		}
		unlock(10) // Unlock last item.

		unlock2, ok := mutex.TryLockAll()
		if !ok {
			t.Fatalf("wanted true, got false")
		}
		unlock2()
	}
}
