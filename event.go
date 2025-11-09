package pglogrepl

// Event represents a database change event type in PostgreSQL logical replication.
//
// Events are dispatched to registered callbacks when data modifications occur.
type Event int8

// EventCallback is a function that handles database change events.
//
// Parameters:
//   - table: The table where the change occurred (includes schema)
//   - data: Column data as a map of column name to value
//
// The callback should be non-blocking and handle errors internally.
// If the callback panics, it will stop the replication process.
//
// Example:
//
//	callback := func(table pglogrepl.Table, data map[string]any) {
//	    log.Printf("Change on %s: %v", table.Name(), data)
//	    // Process the change...
//	}
type EventCallback func(Table, map[string]any)

const (
	// EventInsert is fired when a new row is inserted.
	// The data map contains all column values for the new row.
	EventInsert Event = iota + 1

	// EventUpdate is fired when an existing row is modified.
	// The data map contains the new values for all columns.
	// Note: TOAST columns that didn't change will not be included.
	EventUpdate

	// EventDelete is fired when a row is deleted.
	// The data map contains the old values (requires REPLICA IDENTITY FULL).
	EventDelete

	// EventTruncate is fired when a table is truncated.
	// Currently not fully implemented in the dispatch logic.
	EventTruncate
)

// String returns the string representation of the event type.
func (e Event) String() string {
	switch e {
	case EventInsert:
		return "insert"
	case EventUpdate:
		return "update"
	case EventDelete:
		return "delete"
	case EventTruncate:
		return "truncate"
	default:
		return "unknown"
	}
}

// SetCallback registers an event callback for the specified event type.
//
// Only one callback can be registered per event type. Calling SetCallback
// multiple times for the same event will replace the previous callback.
//
// Parameters:
//   - event: The event type to listen for (EventInsert, EventUpdate, etc.)
//   - cb: The callback function to invoke when the event occurs
//
// This method is thread-safe and can be called before or after Start().
//
// Example:
//
//	repl.SetCallback(pglogrepl.EventInsert, func(table pglogrepl.Table, data map[string]any) {
//	    fmt.Printf("New row in %s: %v\n", table, data)
//	})
//
//	repl.SetCallback(pglogrepl.EventUpdate, func(table pglogrepl.Table, data map[string]any) {
//	    fmt.Printf("Updated row in %s: %v\n", table, data)
//	})
func (p *PgLogRepl) SetCallback(event Event, cb EventCallback) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cbs[event] = cb
}

func (p *PgLogRepl) dispatch(event Event, table Table, data map[string]any) bool {
	if p.Err() != nil {
		return false
	}

	p.mu.RLock()
	cb, ok := p.cbs[event]
	p.mu.RUnlock()

	if ok && cb != nil {
		if p.hasTable(table) {
			cb(table, data)
		} else {
			p.log.LogWarn("mismatch table %q for event %q", table, event)
		}
	} else {
		p.log.LogInfo("mismatch event %q for table %q", event, table)
	}

	return true
}
