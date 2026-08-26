package strlock

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNilContext = errors.New("context is required")
	ErrEmptyKey   = errors.New("key must be a non-empty string")
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

// NewManager returns a new Manager that is ready for use.
func NewManager() *Manager {
	return &Manager{
		locks: make(map[string]*lock),
	}
}

// AcquireLock attempts to acquire a lock for the given key.
// It returns a function that MUST be called to release the lock,
// and an error if the context is cancelled before acquiring the lock,
// or an empty context or key is provided.
func (m *Manager) AcquireLock(ctx context.Context, key string) (func(), error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if key == "" {
		return nil, ErrEmptyKey
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
		return nil, fmt.Errorf("acquire lock: %w", context.Cause(ctx))
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
		return nil, fmt.Errorf("acquire lock: %w", context.Cause(ctx))
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
