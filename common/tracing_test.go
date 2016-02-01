package common

import (
	"encoding/json"
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
	c.Add(f.NewIssuerClockSyncEvent())

	// Serialization
	buf, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Could not marshal result: %q", err)
	}

	// Deserialization
	var c2 MemoryEventCollector
	if err = json.Unmarshal(buf, &c2); err != nil {
		t.Fatalf("Could not unmarshal result: %q", err)
	}
	if len(c.Events) != len(c2.Events) {
		t.Fatalf("Deserialized events do not match. expected %s got %s", c.Events, c2.Events)
	}
}
