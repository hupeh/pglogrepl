package pglogrepl

type Event int8

type EventCallback func(Table, map[string]any)

const (
	EventInsert Event = iota + 1
	EventUpdate
	EventDelete
	EventTruncate
)

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
