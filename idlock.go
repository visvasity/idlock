// Copyright (c) 2025 Visvasity LLC

// Package idlock provides a Mutex type for synchronizing access to resources
// identified by comparable IDs from an unbounded ID space. It allows locking
// of multiple ids at once and unlocking of individual locked ids progressively
// or all locked IDs. Mutexes also support exclusive locking of the entire ID
// space if necessary.
package idlock

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// Mutex provides a thread-safe mechanism for mutual exclusion over resources
// identified by comparable IDs (e.g., strings, integers, or different types
// that can be used as a map keys). Users can also lock the entire ID space
// for exclusive access.
//
// Zero values of Mutex type can be used directly without initialization. The
// mutex is safe for concurrent use by multiple goroutines.
type Mutex[T any] struct {
	mu sync.Mutex

	// waiting holds clients waiting for one or more ids in FIFO order.
	waiting []*client[T]

	// List of lock entries that have acquired ids successfully.
	running []*client[T]

	// lockedIDMap holds list of ids that are currently locked to their
	// respective client. When this map is empty, it may mean no ids are locked
	// or all ids are locked depending on len(running) is zero or one.
	lockedIDMap map[any]*client[T]
}

type client[T any] struct {
	owner *Mutex[T]

	cond sync.Cond

	all bool

	ids []T
}

// LockAll acquires exclusive access to the entire ID space, preventing any
// other calls to [Mutex.Lock] or [Mutex.LockAll] from succeeding until the
// lock is released. It blocks until the ID space is available or the input
// context is canceled. If the context is canceled, it returns a non-nil error
// from [context.Cause].
//
// The returned function releases the exclusive lock, allowing other lock
// operations to proceed.
func (m *Mutex[T]) LockAll(ctx context.Context) (unlockAll func(), err error) {
	c := &client[T]{
		owner: m,
		all:   true,
		cond: sync.Cond{
			L: &m.mu,
		},
	}
	if !c.wait(ctx) {
		return nil, context.Cause(ctx)
	}
	return c.unlockAll, nil
}

// TryLockAll is similar to LockAll, but fails with (nil, false) if caller has
// to block due to lock unavailability.
func (m *Mutex[T]) TryLockAll() (unlockAll func(), ok bool) {
	// Use an already canceled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	unlock, err := m.LockAll(ctx)
	if err != nil {
		return nil, false
	}
	return unlock, true
}

// Lock acquires exclusive access to the specified IDs, which must be non-empty,
// unique, and satisfy Go's comparable constraint (e.g., strings, integers). It
// blocks until all IDs are available or the provided context is canceled. If the
// context is canceled, it returns a non-nil error from [context.Cause]. Panics if
// no IDs are provided, any ID is not comparable, or IDs are not unique.
//
// The returned function unlocks the specified IDs. If no IDs are passed to the
// unlock function, it releases all locked IDs. If specific IDs are provided, they
// must be unique and previously locked by this call; otherwise, it panics.
func (m *Mutex[T]) Lock(ctx context.Context, ids ...T) (unlockIDs func(ids ...T), err error) {
	if len(ids) == 0 {
		panic("no input ids")
	}

	c := &client[T]{
		owner: m,
		cond: sync.Cond{
			L: &m.mu,
		},
		ids: slices.Clone(ids),
	}
	if !c.wait(ctx) {
		return nil, context.Cause(ctx)
	}
	return c.unlock, nil
}

// TryLock is similar to [Lock], but fails with (nil, false) if caller has to
// block due to lock unavailability.
func (m *Mutex[T]) TryLock(ids ...T) (unlockIDs func(ids ...T), ok bool) {
	// Use an already canceled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	unlock, err := m.Lock(ctx, ids...)
	if err != nil {
		return nil, false
	}
	return unlock, true
}

func (c *client[T]) unlockAll() {
	m := c.owner

	m.mu.Lock()
	defer m.mu.Unlock()

	if n := len(m.running); n != 1 {
		panic(fmt.Sprintf("found %d clients running when only 1 is expected", n))
	}
	if n := len(m.lockedIDMap); n != 0 {
		panic(fmt.Sprintf("found %d ids locked when zero are expected", n))
	}

	// Remove the client from running list.
	if c != m.running[0] {
		panic(fmt.Sprintf("this client has already unlocked"))
	}
	m.running = m.running[:0]

	// Signal all waiters up to the first LockAll client.
	for _, waiter := range m.waiting {
		waiter.cond.Signal()
		if waiter.all {
			break
		}
	}
}

func (c *client[T]) unlock(ids ...T) {
	// Check that all input ids belong to the client.
	for i, v := range ids {
		if !slices.ContainsFunc(c.ids, func(x T) bool { return any(x) == any(v) }) {
			panic(fmt.Sprintf("input id %d=%v wasn't locked by this client", i, v))
		}
	}

	// Check if client has unlocked all its ids already, in which case, we just
	// treat this as noop. We must have len(ids) == 0 in this case anyway.
	if len(c.ids) == 0 {
		return
	}

	m := c.owner

	m.mu.Lock()
	defer m.mu.Unlock()

	p := slices.Index(m.running, c)
	if p == -1 {
		panic("client is not running")
	}

	if len(ids) == 0 {
		ids = c.ids
	}

	// Make the ids as free.
	for _, id := range ids {
		if v, ok := m.lockedIDMap[id]; !ok {
			panic(fmt.Sprintf("id %v is not locked", id))
		} else if c != v {
			panic(fmt.Sprintf("id %v is locked by some other client", id))
		}
		delete(m.lockedIDMap, id)
	}

	// Remove freed ids from c.ids as well.
	c.removeIDs(ids)

	// Remove the client from running list.
	if len(c.ids) == 0 {
		m.running = slices.Delete(m.running, p, p+1)
	}

	// Signal all waiters up to the first LockAll client.
	for _, waiter := range m.waiting {
		waiter.cond.Signal()
		if waiter.all {
			break
		}
	}
}

// wait blocks till it acquires the ids necessary for a lock operation
// represented by the entry.
func (c *client[T]) wait(ctx context.Context) bool {
	stopf := context.AfterFunc(ctx, func() {
		c.cond.L.Lock()
		c.cond.Signal()
		c.cond.L.Unlock()
	})
	defer stopf()

	m := c.owner

	m.mu.Lock()
	defer m.mu.Unlock()

	// Lazy initialize the map.
	if m.lockedIDMap == nil {
		m.lockedIDMap = make(map[any]*client[T])
	}

	// A LockAll client can proceed only if,
	//
	// - len(m.lockedIDMap) is zero
	// - there are no clients waiting ahead
	// - there are no clients active

	if c.all {
		m.waiting = append(m.waiting, c)
		defer m.removeWaiter(c)

		for m.waiting[0] != c || len(m.lockedIDMap) != 0 || len(m.running) != 0 {
			if ctx.Err() != nil {
				return false
			}
			c.cond.Wait()
			if ctx.Err() != nil {
				return false
			}
		}
		m.running = append(m.running, c)
		return true
	}

	// A normal Lock with ids can proceed only if
	//
	// - there is no LockAll client running or waiting ahead
	// - none its ids are currently locked, i.e., none of its ids are found in
	//   the m.lockedIDMap
	// - none of its ids are wanted by other waiting clients ahead

	m.waiting = append(m.waiting, c)
	defer m.removeWaiter(c)

	for m.isLockAllRunning() || m.isLockAllWaitingAhead(c) || m.isAnyIDLocked(c.ids) || m.isAnyIDWantedAhead(c) {
		if ctx.Err() != nil {
			return false
		}
		c.cond.Wait()
		if ctx.Err() != nil {
			return false
		}
	}

	// Add all ids to the m.lockedIDMap map.
	for _, id := range c.ids {
		if _, ok := m.lockedIDMap[id]; ok {
			panic(fmt.Sprintf("ready client id %v is found in the lockedIDMap", id))
		}
		m.lockedIDMap[id] = c
	}
	m.running = append(m.running, c)
	return true
}

func (c *client[T]) removeIDs(ids []T) {
	if len(ids) > 0 {
		if &ids[0] == &c.ids[0] {
			c.ids = slices.Delete(c.ids, 0, len(ids))
			return
		}
		for _, id := range ids {
			p := slices.IndexFunc(c.ids, func(x T) bool { return any(x) == any(id) })
			if p == -1 {
				panic(fmt.Sprintf("id %v not found in the client id list", id))
			}
			c.ids = slices.Delete(c.ids, p, p+1)
		}
	}
}

func (m *Mutex[T]) removeWaiter(c *client[T]) {
	p := slices.Index(m.waiting, c)
	if p == -1 {
		panic("client is not found in the waiting list")
	}
	m.waiting = slices.Delete(m.waiting, p, p+1)
}

func (m *Mutex[T]) isLockAllRunning() bool {
	return len(m.running) == 1 && m.running[0].all
}

func (m *Mutex[T]) isLockAllWaitingAhead(c *client[T]) bool {
	p := slices.Index(m.waiting, c)
	if p == -1 {
		panic("input client is not in the waiting list")
	}
	for i := 0; i < p; i++ {
		if m.waiting[i].all {
			return true
		}
	}
	return false
}

func (m *Mutex[T]) isAnyIDLocked(ids []T) bool {
	for _, id := range ids {
		if _, ok := m.lockedIDMap[id]; ok {
			return true
		}
	}
	return false
}

func (m *Mutex[T]) isAnyIDWantedAhead(c *client[T]) bool {
	idMap := make(map[any]struct{}, len(c.ids))
	for _, id := range c.ids {
		idMap[id] = struct{}{}
	}

	p := slices.Index(m.waiting, c)
	if p == -1 {
		panic("input client is not in the waiting list")
	}
	for i := 0; i < p; i++ {
		if m.waiting[i].all {
			return true
		}
		for _, id := range m.waiting[i].ids {
			if _, ok := idMap[id]; ok {
				return true
			}
		}
	}
	return false
}
