package common

import (
	"sync"
)

// FairMutex is similar to a sync.Mutex but has an outer mutex protecting the
// real (inner) one. This attempts to reduce the unfairness by allowing other
// threads to "barge in" and block the next acquisition of the mutex until they
// have had their chance. See https://github.com/golang/go/issues/13086 for
// details.
type FairMutex struct {
	outer sync.Mutex
	inner sync.Mutex
}

// Lock locks the (contended) outer mutex and while it's being held, it locks
// the inner lock.
func (m *FairMutex) Lock() {
	m.outer.Lock()
	m.inner.Lock()
	m.outer.Unlock()
}

// Unlock unlocks the inner lock.
func (m *FairMutex) Unlock() {
	m.inner.Unlock()
}
