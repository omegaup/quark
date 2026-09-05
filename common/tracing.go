package common

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	mathrand "math/rand"
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
	r := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
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

// === ENHANCED TRACING WITH REQUEST CORRELATION ===

// TraceID represents a unique identifier for request tracing
type TraceID string

// SpanID represents a unique identifier for a span within a trace
type SpanID string

// GenerateTraceID creates a new unique trace ID
func GenerateTraceID() TraceID {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return TraceID(fmt.Sprintf("%x", bytes))
}

// GenerateSpanID creates a new unique span ID
func GenerateSpanID() SpanID {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return SpanID(fmt.Sprintf("%x", bytes))
}

// TracedEvent extends NormalEvent with trace correlation
type TracedEvent struct {
	NormalEvent
	TraceID  TraceID `json:"trace_id"`
	SpanID   SpanID  `json:"span_id"`
	ParentID SpanID  `json:"parent_span_id,omitempty"`
}

// TracedCompleteEvent extends CompleteEvent with trace correlation
type TracedCompleteEvent struct {
	CompleteEvent
	TraceID  TraceID `json:"trace_id"`
	SpanID   SpanID  `json:"span_id"`
	ParentID SpanID  `json:"parent_span_id,omitempty"`
}

// TraceContext holds correlation information for distributed tracing
type TraceContext struct {
	TraceID   TraceID           `json:"trace_id"`
	SpanID    SpanID            `json:"span_id"`
	ParentID  SpanID            `json:"parent_span_id,omitempty"`
	Operation string            `json:"operation"`
	StartTime time.Time         `json:"start_time"`
	Tags      map[string]string `json:"tags"`
	mu        sync.RWMutex      `json:"-"`
}

// NewTraceContext creates a new trace context
func NewTraceContext(operation string) *TraceContext {
	return &TraceContext{
		TraceID:   GenerateTraceID(),
		SpanID:    GenerateSpanID(),
		Operation: operation,
		StartTime: time.Now(),
		Tags:      make(map[string]string),
	}
}

// NewChildSpan creates a child span within the same trace
func (tc *TraceContext) NewChildSpan(operation string) *TraceContext {
	return &TraceContext{
		TraceID:   tc.TraceID,
		SpanID:    GenerateSpanID(),
		ParentID:  tc.SpanID,
		Operation: operation,
		StartTime: time.Now(),
		Tags:      make(map[string]string),
	}
}

// AddTag adds a tag to the trace context (thread-safe)
func (tc *TraceContext) AddTag(key, value string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.Tags == nil {
		tc.Tags = make(map[string]string)
	}
	tc.Tags[key] = value
}

// GetTag retrieves a tag from the trace context (thread-safe)
func (tc *TraceContext) GetTag(key string) (string, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	value, exists := tc.Tags[key]
	return value, exists
}

// LogFields returns structured log fields with trace correlation
func (tc *TraceContext) LogFields() map[string]interface{} {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	
	fields := map[string]interface{}{
		"trace_id":  string(tc.TraceID),
		"span_id":   string(tc.SpanID),
		"operation": tc.Operation,
		"duration_ms": float64(time.Since(tc.StartTime)) / float64(time.Millisecond),
	}
	
	if tc.ParentID != "" {
		fields["parent_span_id"] = string(tc.ParentID)
	}
	
	// Add all tags with prefix
	for k, v := range tc.Tags {
		fields[fmt.Sprintf("tag_%s", k)] = v
	}
	
	return fields
}

// GetHTTPHeaders returns HTTP headers for trace propagation
func (tc *TraceContext) GetHTTPHeaders() map[string]string {
	return map[string]string{
		"X-Trace-ID": string(tc.TraceID),
		"X-Span-ID":  string(tc.SpanID),
	}
}

// ParseHTTPHeaders extracts trace context from HTTP headers
func ParseHTTPHeaders(headers map[string]string) *TraceContext {
	traceID := TraceID(headers["X-Trace-ID"])
	spanID := SpanID(headers["X-Span-ID"])
	
	if traceID == "" || spanID == "" {
		return nil
	}
	
	return &TraceContext{
		TraceID:   traceID,
		SpanID:    spanID,
		StartTime: time.Now(),
		Tags:      make(map[string]string),
	}
}

// Enhanced EventFactory with trace correlation
func (factory *EventFactory) NewTracedEvent(
	name string,
	eventType string,
	traceCtx *TraceContext,
	args ...Arg,
) *TracedEvent {
	baseEvent := factory.NewEvent(name, eventType, args...)
	return &TracedEvent{
		NormalEvent: *baseEvent,
		TraceID:     traceCtx.TraceID,
		SpanID:      traceCtx.SpanID,
		ParentID:    traceCtx.ParentID,
	}
}

// NewTracedCompleteEvent creates a CompleteEvent with trace correlation
func (factory *EventFactory) NewTracedCompleteEvent(
	name string,
	traceCtx *TraceContext,
	args ...Arg,
) *TracedCompleteEvent {
	baseEvent := factory.NewCompleteEvent(name, args...)
	return &TracedCompleteEvent{
		CompleteEvent: *baseEvent,
		TraceID:       traceCtx.TraceID,
		SpanID:        traceCtx.SpanID,
		ParentID:      traceCtx.ParentID,
	}
}

// === CONTEXT INTEGRATION ===

type traceContextKey struct{}

var TraceContextKey = &traceContextKey{}

// ContextWithTrace adds trace context to a standard Go context
func ContextWithTrace(ctx context.Context, traceCtx *TraceContext) context.Context {
	return context.WithValue(ctx, TraceContextKey, traceCtx)
}

// TraceFromContext extracts trace context from a Go context
func TraceFromContext(ctx context.Context) (*TraceContext, bool) {
	trace, ok := ctx.Value(TraceContextKey).(*TraceContext)
	return trace, ok
}

// GetOrCreateTraceFromContext gets trace from context or creates new one
func GetOrCreateTraceFromContext(ctx context.Context, operation string) (*TraceContext, context.Context) {
	if trace, ok := TraceFromContext(ctx); ok {
		return trace, ctx
	}
	
	trace := NewTraceContext(operation)
	newCtx := ContextWithTrace(ctx, trace)
	return trace, newCtx
}

// === SUBMISSION-SPECIFIC TRACING ===

// SubmissionTracer provides tracing specific to omegaup submissions
type SubmissionTracer struct {
	traceCtx *TraceContext
	factory  *EventFactory
}

// NewSubmissionTracer creates a tracer for a specific submission
func NewSubmissionTracer(submissionID int64, userID int64, problemAlias string) *SubmissionTracer {
	traceCtx := NewTraceContext("submission_processing")
	traceCtx.AddTag("submission_id", fmt.Sprintf("%d", submissionID))
	traceCtx.AddTag("user_id", fmt.Sprintf("%d", userID))
	traceCtx.AddTag("problem", problemAlias)
	
	factory := NewEventFactory("omegaup-grader", "submission-processor")
	
	return &SubmissionTracer{
		traceCtx: traceCtx,
		factory:  factory,
	}
}

// TraceContext returns the trace context
func (st *SubmissionTracer) TraceContext() *TraceContext {
	return st.traceCtx
}

// TracePhase traces a specific phase of submission processing
func (st *SubmissionTracer) TracePhase(phase string) *TracedCompleteEvent {
	childSpan := st.traceCtx.NewChildSpan(phase)
	childSpan.AddTag("phase", phase)
	
	event := st.factory.NewTracedCompleteEvent(phase, childSpan)
	return event
}

// === LOGGING INTEGRATION ===

// CorrelatedLogger wraps logging with trace correlation
type CorrelatedLogger struct {
	traceCtx *TraceContext
}

// NewCorrelatedLogger creates a logger with trace correlation
func NewCorrelatedLogger(traceCtx *TraceContext) *CorrelatedLogger {
	return &CorrelatedLogger{traceCtx: traceCtx}
}

// Info logs an info message with trace correlation
func (cl *CorrelatedLogger) Info(message string, keysAndValues ...interface{}) {
	fields := cl.traceCtx.LogFields()
	fields["level"] = "INFO"
	fields["message"] = message
	
	// Add additional key-value pairs
	for i := 0; i < len(keysAndValues)-1; i += 2 {
		if key, ok := keysAndValues[i].(string); ok {
			fields[key] = keysAndValues[i+1]
		}
	}
	
	// In production, send to your logging system
	fmt.Printf("trace_id=%s span_id=%s %s %+v\n", 
		cl.traceCtx.TraceID, cl.traceCtx.SpanID, message, fields)
}

// Error logs an error message with trace correlation
func (cl *CorrelatedLogger) Error(message string, err error, keysAndValues ...interface{}) {
	fields := cl.traceCtx.LogFields()
	fields["level"] = "ERROR"
	fields["message"] = message
	fields["error"] = err.Error()
	
	// Add additional key-value pairs
	for i := 0; i < len(keysAndValues)-1; i += 2 {
		if key, ok := keysAndValues[i].(string); ok {
			fields[key] = keysAndValues[i+1]
		}
	}
	
	// In production, send to your logging system
	fmt.Printf("trace_id=%s span_id=%s ERROR: %s - %v %+v\n", 
		cl.traceCtx.TraceID, cl.traceCtx.SpanID, message, err, fields)
}
