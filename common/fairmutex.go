package common

import (
	"sync"
)

// fairMutex is similar to a sync.Mutex but has an outer mutex protecting the
// real (inner) one. This attempts to reduce the unfairness by allowing other
// threads to "barge in" and block the next acquisition of the mutex until they
// have had their chance. See https://github.com/golang/go/issues/13086 for
// details.
type FairMutex struct {
	outer sync.Mutex
	inner sync.Mutex
}

func (m *FairMutex) Lock() {
	m.outer.Lock()
	m.inner.Lock()
	m.outer.Unlock()
}

func (m *FairMutex) Unlock() {
	m.inner.Unlock()
}
