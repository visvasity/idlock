// Copyright (c) 2025 Visvasity LLC

package idlock

import (
	"context"
	"testing"
)

func TestTryLockAll(t *testing.T) {
	ctx := context.Background()

	var mutex Mutex

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
		unlock, err := mutex.Lock(ctx, 10, "job1")
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := mutex.TryLockAll(); ok {
			t.Fatal("wanted false, got true")
		}
		unlock("job1") // Unlock one time at a time.

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
