package common

import (
	"encoding/json"
	"hash/fnv"
	"io"
	"math/rand"
	"sync/atomic"
	"time"
)

const (
	EventBegin     = "B"
	EventClockSync = "c"
	EventEnd       = "E"
	EventComplete  = "X"
	EventInstant   = "i"
	EventMetadata  = "M"
)

var (
	syncID uint64
)

func init() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	syncID = uint64(r.Int63())
}

// NewSyncID allocates a locally-unique SyncID. A counter is initialized
// to a random 63-bit integer on startup and then atomically incremented each
// time a new ID is needed.
func NewSyncID() uint64 {
	return atomic.AddUint64(&syncID, 1)
}

type Event interface {
	Finalize()
}

type Arg struct {
	Name  string
	Value interface{}
}

type NormalEvent struct {
	Name      string                 `json:"name"`
	Type      string                 `json:"ph"`
	Timestamp int64                  `json:"ts"`
	PID       int                    `json:"pid"`
	TID       int                    `json:"tid"`
	Args      map[string]interface{} `json:"args,omitempty"`
}

func (e *NormalEvent) Finalize() {
}

type CompleteEvent struct {
	NormalEvent
	Duration int64 `json:"dur"`
}

func (e *CompleteEvent) Finalize() {
	e.Duration = time.Now().UnixNano()/1000 - e.Timestamp
	e.NormalEvent.Finalize()
}

type IssuerClockSyncEvent struct {
	NormalEvent
	SyncID uint64 `json:"-"`
}

func (e *IssuerClockSyncEvent) Finalize() {
	e.Args["sync_id"] = e.SyncID
	e.Args["issue_ts"] = e.Timestamp
	e.Timestamp = time.Now().UnixNano() / 1000
	e.NormalEvent.Finalize()
}

type EventCollector interface {
	Add(Event) error
}

type WriterEventCollector struct {
	output io.Writer
}

func NewWriterEventCollector(
	output io.Writer,
	appending bool,
) (*WriterEventCollector, error) {
	if !appending {
		if _, err := output.Write([]byte("[\n")); err != nil {
			return nil, err
		}
	}
	return &WriterEventCollector{
		output: output,
	}, nil
}

func (collector *WriterEventCollector) Add(e Event) error {
	e.Finalize()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err = collector.output.Write(b); err != nil {
		return err
	}
	if _, err = collector.output.Write([]byte(",\n")); err != nil {
		return err
	}
	return nil
}

type MemoryEventCollector struct {
	Events []Event
}

func NewMemoryEventCollector() *MemoryEventCollector {
	return &MemoryEventCollector{
		Events: make([]Event, 0),
	}
}

func (collector *MemoryEventCollector) Add(e Event) error {
	e.Finalize()
	collector.Events = append(collector.Events, e)
	return nil
}

func (collector *MemoryEventCollector) MarshalJSON() ([]byte, error) {
	return json.Marshal(collector.Events)
}

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

	for _, rawEvent := range rawEvents {
		normalEvent := NormalEvent{
			Name:      rawEvent.Name,
			Type:      rawEvent.Type,
			Timestamp: rawEvent.Timestamp,
			PID:       rawEvent.PID,
			TID:       rawEvent.TID,
			Args:      rawEvent.Args,
		}
		if rawEvent.Type == EventComplete {
			event := CompleteEvent{
				NormalEvent: normalEvent,
				Duration:    *rawEvent.Duration,
			}
			collector.Events = append(collector.Events, &event)
		} else if rawEvent.Type == EventClockSync {
			event := IssuerClockSyncEvent{
				NormalEvent: normalEvent,
				SyncID:      (uint64)(rawEvent.Args["sync_id"].(float64)),
			}
			collector.Events = append(collector.Events, &event)
		} else {
			collector.Events = append(collector.Events, &normalEvent)
		}
	}

	return nil
}

type MultiEventCollector struct {
	collectors []EventCollector
}

func NewMultiEventCollector(collectors ...EventCollector) *MultiEventCollector {
	return &MultiEventCollector{
		collectors: collectors,
	}
}

func (collector *MultiEventCollector) Add(e Event) error {
	for _, collector := range collector.collectors {
		if err := collector.Add(e); err != nil {
			return err
		}
	}
	return nil
}

type NullEventCollector struct {
}

func (collector *NullEventCollector) Add(e Event) error {
	return nil
}

type EventFactory struct {
	processName, threadName string
	pid, tid                int
}

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

func (factory *EventFactory) NewIssuerClockSyncEvent() *IssuerClockSyncEvent {
	ev := &IssuerClockSyncEvent{
		NormalEvent: *factory.NewEvent("clock_sync", EventClockSync),
		SyncID:      NewSyncID(),
	}
	ev.Args = make(map[string]interface{})
	return ev
}

func (factory *EventFactory) NewReceiverClockSyncEvent(syncID uint64) *NormalEvent {
	ev := factory.NewEvent("clock_sync", EventClockSync, Arg{"sync_id", syncID})
	return ev
}

func (factory *EventFactory) NewCompleteEvent(
	name string,
	args ...Arg,
) *CompleteEvent {
	return &CompleteEvent{
		NormalEvent: *factory.NewEvent(name, EventComplete, args...),
	}
}

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

	if err := collector.Add(
		factory.NewEvent(
			"thread_name",
			EventMetadata,
			Arg{"name", factory.threadName},
		),
	); err != nil {
		return err
	}

	return nil
}
