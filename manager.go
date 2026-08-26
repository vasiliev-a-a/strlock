package strlock

import (
	"context"
	"sync"
)

// Error is the type of every error returned by this package.
// Callers can use [errors.As] to check whether an error originated from
// strlock, and [errors.Is] or [errors.Unwrap] to inspect an underlying cause
// such as [context.Canceled].
type Error interface {
	error
	Unwrap() error

	// isStrlockError marks types that originate from this package.
	isStrlockError()
}

type strlockError struct {
	msg string
	err error
}

// newError returns a new [Error] with the given message and an optional
// wrapped cause. It panics if `m` is empty and `e` is nil, since an error
// that conveys nothing is a programming mistake.
func newError(m string, e error) Error {
	if m == "" && e == nil {
		panic("silent errors are forbidden")
	}
	return &strlockError{msg: m, err: e}
}

func (e *strlockError) Error() string {
	switch {
	case e.err == nil:
		return e.msg
	case e.msg == "":
		return e.err.Error()
	default:
		return e.msg + ": " + e.err.Error()
	}
}

func (e *strlockError) Unwrap() error { return e.err }

func (e *strlockError) isStrlockError() {}

var (
	ErrNilContext = newError("context is required", nil)
	ErrEmptyKey   = newError("key must be a non-empty string", nil)
)

type lock struct {
	ch   chan struct{}
	refs int
}

// Manager manages locks for string keys.
// It allows concurrent acquisition of locks for different keys while ensuring
// that only one goroutine can hold a lock for a specific key at any given time.
// Important: The zero value of Manager is not usable; use [NewManager] to create a new instance.
type Manager struct {
	mu    sync.Mutex
	locks map[string]*lock
}

// NewManager returns a new [Manager] that is ready for use.
func NewManager() *Manager {
	return &Manager{
		locks: make(map[string]*lock),
	}
}

// AcquireLock attempts to acquire a lock for the given key.
// It returns a function that MUST be called to release the lock,
// and an error if the context is cancelled before acquiring the lock,
// or a nil context or empty key is provided.
// On error, the returned function is a non-nil no-op, so it is safe to
// defer unconditionally.
// Every error returned has type [Error]; see its documentation for how to
// inspect errors.
func (m *Manager) AcquireLock(ctx context.Context, key string) (func(), error) {
	if ctx == nil {
		return func() {}, ErrNilContext
	}
	if key == "" {
		return func() {}, ErrEmptyKey
	}

	m.mu.Lock()
	l, ok := m.locks[key]
	if !ok {
		l = &lock{ch: make(chan struct{}, 1)}
		m.locks[key] = l
	}
	l.refs++
	m.mu.Unlock()

	// Check if context is already done before attempting to acquire lock.
	select {
	case <-ctx.Done():
		// Context is already canceled, clean up and return error.
		m.releaseRef(key, l)
		return func() {}, newError("", context.Cause(ctx))
	default:
		// Context is not done, proceed with lock acquisition.
	}

	select {
	case l.ch <- struct{}{}:
		// Lock is acquired.
		var once sync.Once
		return func() {
			once.Do(func() {
				<-l.ch
				m.releaseRef(key, l)
			})
		}, nil
	case <-ctx.Done():
		// Context is canceled.
		m.releaseRef(key, l)
		return func() {}, newError("", context.Cause(ctx))
	}
}

// releaseRef is a helper function that performs ref cleanup under the lock.
func (m *Manager) releaseRef(key string, l *lock) {
	m.mu.Lock()
	l.refs--
	if l.refs == 0 {
		delete(m.locks, key)
	}
	m.mu.Unlock()
}
