package strlock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestManager_AcquireLock(t *testing.T) {
	testKey := "ct8MYib7Dbm0VNbr"
	m := NewManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release, err := m.AcquireLock(ctx, testKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if release == nil {
		t.Fatalf("expected `func()`, got `nil`")
	}

	_, ok := m.locks[testKey]
	if !ok {
		t.Fatalf("expected the value to exist in the map")
	}
}

func TestManager_AcquireLockNilContext(t *testing.T) {
	testKey := "Evr3UVpz2MYis8Zg"
	m := NewManager()

	_, err := m.AcquireLock(nil, testKey)
	if err == nil {
		t.Fatalf("expected an error")
	}
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("expected %q error, got %q", ErrNilContext, err)
	}
}

func TestManager_AcquireLockEmptyKey(t *testing.T) {
	testKey := ""
	m := NewManager()

	_, err := m.AcquireLock(context.Background(), testKey)
	if err == nil {
		t.Fatalf("expected an error")
	}
	if !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected %q error, got %q", ErrEmptyKey, err)
	}
}

func TestManager_ReleaseLock(t *testing.T) {
	testKey := "Wb2EYdj9Urh6Zau9"
	m := NewManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	releaseLock, err := m.AcquireLock(ctx, testKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if releaseLock == nil {
		t.Fatalf("expected `func()`, got `nil`")
	}

	releaseLock()
	if _, ok := m.locks[testKey]; ok {
		t.Fatalf("expected the value to be absent in the map")
	}
}

func TestManager_DoubleRelease(t *testing.T) {
	testKey := "double-release-test"
	m := NewManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release1, err := m.AcquireLock(ctx, testKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	release1()
	// Should be a safe no-op.
	release1()

	// Should be able to acquire again.
	release2, err := m.AcquireLock(ctx, testKey)
	if err != nil {
		t.Fatalf("unexpected error on re-acquire: %v", err)
	}
	release2()
}

func TestManager_Concurrency(t *testing.T) {
	const workersCount = 10

	var max, count int
	var countMu sync.Mutex
	var workersWg sync.WaitGroup

	errChan := make(chan error, workersCount)
	testKey := "U3fGF9uP6yQX8dxG"
	m := NewManager()
	ctx, cancel := context.WithTimeout(context.Background(), (workersCount/2+1)*time.Second)
	defer cancel()

	for i := 0; i < workersCount; i++ {
		workersWg.Add(1)
		go func() {
			defer workersWg.Done()

			releaseLock, err := m.AcquireLock(ctx, testKey)
			if err != nil {
				errChan <- err
			} else {
				countMu.Lock()
				count++
				if count > max {
					max = count
				}
				countMu.Unlock()
				time.Sleep(300 * time.Millisecond)
				count--
				releaseLock()
			}
		}()
	}

	doneCh := make(chan struct{})
	go func() {
		workersWg.Wait()
		close(errChan)
		close(doneCh)
	}()

	select {
	case <-doneCh:
		for err := range errChan {
			t.Errorf("unexpected error: %v", err)
		}

		if _, ok := m.locks[testKey]; ok {
			t.Fatalf("expected the value to be absent in the map")
		}
		if max > 1 {
			t.Fatalf("expected a mutual exclusion, got max %d concurrent", max)
		}

	case <-ctx.Done():
		t.Fatalf("expected to complete in time")
	}
}

func TestManager_CancelledContext(t *testing.T) {
	testKey := "cancelled-context-test"
	m := NewManager()

	// Test with already cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	releaseLock, err := m.AcquireLock(ctx, testKey)
	if err == nil {
		t.Fatalf("expected an error for cancelled context")
	}
	if releaseLock != nil {
		t.Fatalf("expected nil release function for cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got: %v", err)
	}
	// Verify no lock was left in the map
	if _, ok := m.locks[testKey]; ok {
		t.Fatalf("expected no lock in map after cancelled context")
	}
}

func TestManager_Parallelism(t *testing.T) {
	var errs []error
	var errsMu sync.Mutex
	var workersWg sync.WaitGroup

	testKeys := []string{"U3fGF9uP6yQX8dxG", "pgCD7aAN4znU2erK", "CA9ccB6wE5jjHA4s", "wHZ0iK8mYH2bUL2r", "X2kqP6hY3bV8mEJ9"}
	m := NewManager()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	for _, key := range testKeys {
		workersWg.Add(1)
		go func(k string) {
			defer workersWg.Done()

			releaseLock, err := m.AcquireLock(ctx, k)
			if err != nil {
				errsMu.Lock()
				errs = append(errs, err)
				errsMu.Unlock()
			} else {
				time.Sleep(300 * time.Millisecond)
				releaseLock()
			}
		}(key)
	}

	doneCh := make(chan struct{})
	go func() {
		workersWg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		for _, err := range errs {
			t.Fatalf("unexpected error: %v", err)
		}

		for _, key := range testKeys {
			if _, ok := m.locks[key]; ok {
				t.Errorf("expected no value under the key %q", key)
			}
		}

	case <-ctx.Done():
		t.Fatalf("expected to complete in time")
	}
}

func TestManager_Context(t *testing.T) {
	testKey := "9XNmz7EQm6Uz7Kt7"
	m := NewManager()

	// This one should acquire the lock and hold it for 1 second.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel1()

	releaseLock1, err1 := m.AcquireLock(ctx1, testKey)
	if err1 != nil {
		t.Fatalf("unexpected error: %v", err1)
	}
	defer releaseLock1()

	// This one should fail to acquire the lock because the first one is holding it.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()

	errCh := make(chan error, 1)
	go func() {
		_, err2 := m.AcquireLock(ctx2, testKey)
		errCh <- err2
	}()

	select {
	case <-ctx1.Done():
		t.Fatalf("expected to complete in time")

	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected an error")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected error `context.DeadlineExceeded`, got: %v", err)
		}
	}
}
