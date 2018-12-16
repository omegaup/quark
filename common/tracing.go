package common

import (
	"encoding/json"
	"hash/fnv"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// EventBegin represents the beginning of a Duration Event.
	EventBegin = "B"

	// EventClockSync represents an event that is used to synchronize different
	// clock domains. For instance, a client and a server will have clocks that
	// are not necessarily in-sync and might drift a bit from each other.
	EventClockSync = "c"

	// EventEnd represents the end of a Duration Event.
	EventEnd = "E"

	// EventComplete represents a complete Duration Event. This is logically the
	// combination of a Begin and End events, and avoids some overhead.
	EventComplete = "X"

	// EventInstant represents an event that does not have a duration associated
	// with it.
	EventInstant = "i"

	// EventMetadata represents extra information that is not necessarily an
	// Event. It is typically used to add things like process and thread names.
	EventMetadata = "M"
)

var (
	syncID uint64
)

func init() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	syncID = uint64(r.Uint32())
}

// NewSyncID allocates a locally-unique SyncID. A counter is initialized
// to a random 63-bit integer on startup and then atomically incremented each
// time a new ID is needed.
func NewSyncID() uint64 {
	return atomic.AddUint64(&syncID, 1)
}

// An Event represents an abstract event in the system.
type Event interface {
	// Finalize performs any necessary post-processing for this Event.
	Finalize()
}

// An Arg represents a name-value pair.
type Arg struct {
	Name  string
	Value interface{}
}

// A NormalEvent is the base class of all other Events.
type NormalEvent struct {
	Name      string                 `json:"name"`
	Type      string                 `json:"ph"`
	Timestamp int64                  `json:"ts"`
	PID       int                    `json:"pid"`
	TID       int                    `json:"tid"`
	Args      map[string]interface{} `json:"args,omitempty"`
	finalized bool
}

// Finalize marks this Event as having all its post-processing done.
func (e *NormalEvent) Finalize() {
	e.finalized = true
}

// A CompleteEvent is a NormalEvent that has a full duration associated to it.
type CompleteEvent struct {
	NormalEvent
	Duration int64 `json:"dur"`
}

// Finalize computes the duration of the current CompleteEvent.
func (e *CompleteEvent) Finalize() {
	if e.NormalEvent.finalized {
		return
	}
	e.Duration = time.Now().UnixNano()/1000 - e.Timestamp
	e.NormalEvent.Finalize()
}

// IssuerClockSyncEvent is a NormalEvent that is associated with a Clock Sync
// Event in the server clock domain. The SyncID can be sent to the client so
// that the clock domains can be correlated.
type IssuerClockSyncEvent struct {
	NormalEvent
	SyncID uint64 `json:"-"`
}

// Finalize records the timestamp whtn the event was issued and when the client
// replied that it received the event. This allows to synchronized both clock
// domains.
func (e *IssuerClockSyncEvent) Finalize() {
	if e.NormalEvent.finalized {
		return
	}
	e.Args["sync_id"] = e.SyncID
	e.Args["issue_ts"] = e.Timestamp
	e.Timestamp = time.Now().UnixNano() / 1000
	e.NormalEvent.Finalize()
}

// An EventCollector aggregates events in Chromium's Trace Event Format, which
// is a (modified) JSON list of objects. The full documentation can be found at
// https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview
type EventCollector interface {
	Add(Event) error
	io.Closer
}

// A WriterEventCollector is an EventCollector that writes its output to an
// io.Writer. It uses a sync.Mutex to synchronize events so that all writes are
// thread-safe.
type WriterEventCollector struct {
	output io.WriteCloser
	lock   sync.Mutex
}

// NewWriterEventCollector returns a WriterEventCollector for a specified io.WriteCloser.
func NewWriterEventCollector(output io.WriteCloser) *WriterEventCollector {
	return &WriterEventCollector{
		output: output,
	}
}

// Add writes the specified event to the stream.
func (collector *WriterEventCollector) Add(e Event) error {
	e.Finalize()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	collector.lock.Lock()
	defer collector.lock.Unlock()
	if _, err = collector.output.Write(b); err != nil {
		return err
	}
	if _, err = collector.output.Write([]byte(",\n")); err != nil {
		return err
	}
	return nil
}

// Close closes the underlying writer.
func (collector *WriterEventCollector) Close() error {
	return collector.output.Close()
}

// A MemoryEventCollector is an EventCollector that stores all the Events in an
// in-memory queue. It uses a sync.Mutex to synchonize access to the queue so
// that all the writes are thread-safe.
type MemoryEventCollector struct {
	Events []Event
	lock   sync.Mutex
}

// NewMemoryEventCollector returns a MemoryEventCollector.
func NewMemoryEventCollector() *MemoryEventCollector {
	return &MemoryEventCollector{
		Events: make([]Event, 0),
	}
}

// Add adds the specified Event to the in-memory queue of Events.
func (collector *MemoryEventCollector) Add(e Event) error {
	e.Finalize()
	collector.lock.Lock()
	defer collector.lock.Unlock()
	collector.Events = append(collector.Events, e)
	return nil
}

// MarshalJSON returns the JSON representation of the tracing events.
func (collector *MemoryEventCollector) MarshalJSON() ([]byte, error) {
	collector.lock.Lock()
	defer collector.lock.Unlock()
	return json.Marshal(collector.Events)
}

// UnmarshalJSON reads the serialized JSON in buf and converts it into an
// in-memory representation.
func (collector *MemoryEventCollector) UnmarshalJSON(buf []byte) error {
	var rawEvents []struct {
		Name      string                 `json:"name"`
		Type      string                 `json:"ph"`
		Timestamp int64                  `json:"ts"`
		PID       int                    `json:"pid"`
		TID       int                    `json:"tid"`
		Args      map[string]interface{} `json:"args,omitempty"`
		Duration  *int64                 `json:"dur,omitempty"`
	}

	if err := json.Unmarshal(buf, &rawEvents); err != nil {
		return err
	}

	collector.lock.Lock()
	defer collector.lock.Unlock()
	for _, rawEvent := range rawEvents {
		normalEvent := NormalEvent{
			Name:      rawEvent.Name,
			Type:      rawEvent.Type,
			Timestamp: rawEvent.Timestamp,
			PID:       rawEvent.PID,
			TID:       rawEvent.TID,
			Args:      rawEvent.Args,
			finalized: true,
		}
		if rawEvent.Type == EventComplete {
			event := CompleteEvent{
				NormalEvent: normalEvent,
				Duration:    *rawEvent.Duration,
			}
			collector.Events = append(collector.Events, &event)
		} else if rawEvent.Type == EventClockSync {
			syncID := (uint64)(rawEvent.Args["sync_id"].(float64))
			if _, ok := rawEvent.Args["issue_ts"]; ok {
				event := IssuerClockSyncEvent{
					NormalEvent: normalEvent,
					SyncID:      syncID,
				}
				collector.Events = append(collector.Events, &event)
			} else {
				normalEvent.Args["sync_id"] = syncID
				collector.Events = append(collector.Events, &normalEvent)
			}
		} else {
			collector.Events = append(collector.Events, &normalEvent)
		}
	}

	return nil
}

// Close is a no-op.
func (collector *MemoryEventCollector) Close() error {
	return nil
}

// A MultiEventCollector is an EventCollector that can broadcast Events to
// multiple EventCollectors.
type MultiEventCollector struct {
	collectors []EventCollector
}

// NewMultiEventCollector returns a new MultiEventCollector with all the
// specified collectors.
func NewMultiEventCollector(collectors ...EventCollector) *MultiEventCollector {
	return &MultiEventCollector{
		collectors: collectors,
	}
}

// Add broadcasts the specified Event to all the EventCollectors.
func (collector *MultiEventCollector) Add(e Event) error {
	for _, collector := range collector.collectors {
		if err := collector.Add(e); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all underlying collectors.
func (collector *MultiEventCollector) Close() error {
	var lastError error
	for _, collector := range collector.collectors {
		if err := collector.Close(); err != nil {
			lastError = err
		}
	}
	return lastError
}

// A NullEventCollector is an EventCollector that discards all the collected
// objects.
type NullEventCollector struct {
}

// Add does nothing.
func (collector *NullEventCollector) Add(e Event) error {
	return nil
}

// Close does nothing.
func (collector *NullEventCollector) Close() error {
	return nil
}

// An EventFactory can create events modelled after Chrome's Trace Events.
type EventFactory struct {
	processName, threadName string
	pid, tid                int
}

// NewEventFactory creates a new EventFactory with the provided process and thread names.
func NewEventFactory(processName, threadName string) *EventFactory {
	processHash := fnv.New32a()
	processHash.Write([]byte(processName))
	threadHash := fnv.New32a()
	threadHash.Write([]byte(threadName))
	return &EventFactory{
		processName: processName,
		threadName:  threadName,
		pid:         int(processHash.Sum32()),
		tid:         int(threadHash.Sum32()),
	}
}

// NewEvent creates a generic event.
func (factory *EventFactory) NewEvent(
	name string,
	eventType string,
	args ...Arg,
) *NormalEvent {
	ev := &NormalEvent{
		Name:      name,
		Type:      eventType,
		Timestamp: time.Now().UnixNano() / 1000,
		PID:       factory.pid,
		TID:       factory.tid,
	}
	if len(args) > 0 {
		ev.Args = make(map[string]interface{})
		for _, arg := range args {
			ev.Args[arg.Name] = arg.Value
		}
	}
	return ev
}

// NewIssuerClockSyncEvent creates a ClockSync event in a server context. The
// autogenerated SyncID can be sent to a client so that the clocks can be
// correlated.
func (factory *EventFactory) NewIssuerClockSyncEvent() *IssuerClockSyncEvent {
	ev := &IssuerClockSyncEvent{
		NormalEvent: *factory.NewEvent("clock_sync", EventClockSync),
		SyncID:      NewSyncID(),
	}
	ev.Args = make(map[string]interface{})
	return ev
}

// NewReceiverClockSyncEvent creates a ClockSync event in a client context.
// This allows to correlate the clocks of the server and client.
func (factory *EventFactory) NewReceiverClockSyncEvent(syncID uint64) *NormalEvent {
	ev := factory.NewEvent("clock_sync", EventClockSync, Arg{"sync_id", syncID})
	return ev
}

// NewCompleteEvent creates a CompleteEvent with the supplied arguments. It
// saves the overhead of having a start and end events.
func (factory *EventFactory) NewCompleteEvent(
	name string,
	args ...Arg,
) *CompleteEvent {
	return &CompleteEvent{
		NormalEvent: *factory.NewEvent(name, EventComplete, args...),
	}
}

// Register adds the supplied collector to the factory. All the events sent to
// the factory will be aggregated by the collector.
func (factory *EventFactory) Register(collector EventCollector) error {
	if err := collector.Add(
		factory.NewEvent(
			"process_name",
			EventMetadata,
			Arg{"name", factory.processName},
		),
	); err != nil {
		return err
	}

	return collector.Add(
		factory.NewEvent(
			"thread_name",
			EventMetadata,
			Arg{"name", factory.threadName},
		),
	)
}
