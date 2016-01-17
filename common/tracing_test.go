package common

import (
	"testing"
)

func TestMemoryCollector(t *testing.T) {
	c := NewMemoryEventCollector()
	f := NewEventFactory("main", "main")
	if err := f.Register(c); err != nil {
		t.Fatalf("Could not register factory: %q", err)
	}
	c.Add(f.NewEvent("test", EventBegin))
	c.Add(f.NewEvent("test", EventEnd))
	c.Add(f.NewCompleteEvent("test"))
	if _, err := c.MarshalJSON(); err != nil {
		t.Fatalf("Could not marshal result: %q", err)
	}

}
