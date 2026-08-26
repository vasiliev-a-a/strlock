// Package strlock provides a simple, in-process lock manager for managing
// mutual exclusion locks on dynamic string keys.
//
// It is intended for scenarios where you need per-key serialization of work
// across multiple goroutines, but the set of keys is not known at compile time
// and may be transient. A common example is serializing access to per-user or
// per-resource state keyed by an ID string.
//
// # Usage
//
// Create a manager with [NewManager] and call [Manager.AcquireLock] with
// a context and a key:
//
//	m := strlock.NewManager()
//
//	release, err := m.AcquireLock(ctx, "user:42")
//	if err != nil {
//		// handle error
//	}
//	defer release()
//
//	// Critical section: no other goroutine can hold the lock for "user:42"
//	// until release is called.
//
// # The release function is mandatory
//
// [Manager.AcquireLock] returns a release function that MUST be called
// exactly once when the critical section is complete. Calling it more than once
// is a safe no-op, but failing to call it leaks a reference and permanently
// blocks other goroutines from acquiring the lock on that key.
//
// # Context cancellation
//
// AcquireLock respects the provided context:
//
//   - If the context is already cancelled when AcquireLock is called, no lock is
//     acquired and an error is returned immediately.
//   - If the context is cancelled while waiting for a held lock, the wait is
//     aborted and an error is returned.
//   - Once the lock is acquired, the context plays no further role. The caller
//     owns the lock until the release function is called; context cancellation
//     does not release an acquired lock.
//
// # Error handling
//
// AcquireLock returns wrapped errors. Callers should use [errors.Is] to inspect
// them rather than string matching:
//
//   - [ErrNilContext] is returned (wrapped) when a nil context is provided
//
//   - [ErrEmptyKey] is returned (wrapped) when an empty key is provided
//
//   - [context.Canceled] and [context.DeadlineExceeded] are returned (wrapped)
//     when the context is cancelled before the lock is acquired.
//
//     if errors.Is(err, strlock.ErrEmptyKey) {
//     // ...
//     }
//
// # Memory management
//
// Locks are reference-counted. When all holders of a key release their locks and
// the reference count drops to zero, the internal entry for that key is removed
// from the manager's map. There is no manual cleanup API and no unbounded growth
// for transient keys.
//
// # Concurrency guarantees
//
//   - Mutual exclusion is guaranteed per key: at most one goroutine may hold the
//     lock for a given key at any time.
//   - Different keys are fully independent: acquiring a lock on key A never
//     blocks acquisition of key B.
//   - The [Manager] itself is safe for concurrent use by multiple goroutines.
//
// # Non-goals
//
//   - Not a read/write lock: locks are exclusive only.
//   - Not reentrant: calling AcquireLock on a key the caller already holds will
//     deadlock, since the caller would be waiting for itself to release the lock.
//   - Not a distributed lock: all coordination is in-process within a single
//     [Manager] instance.
package strlock
