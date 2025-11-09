package pglogrepl

// Logger defines the interface for logging replication events.
//
// Implementations should be thread-safe as methods may be called concurrently
// from multiple goroutines during replication.
type Logger interface {
	// LogInfo logs informational messages about normal replication operations.
	// Examples: connection status, WAL position updates, event dispatching.
	LogInfo(format string, args ...any)

	// LogError logs error messages for failures that stop or impact replication.
	// Examples: connection errors, parsing failures, callback panics.
	LogError(format string, args ...any)

	// LogWarn logs warning messages for unexpected but non-fatal situations.
	// Examples: unknown tables, mismatched events, configuration issues.
	LogWarn(format string, args ...any)
}

// LoggerFunc is a function adapter that implements the Logger interface.
//
// It allows using a simple function as a logger by wrapping it with level information.
// The function receives the log level as the first parameter, followed by format and args.
//
// Example:
//
//	logger := pglogrepl.LoggerFunc(func(level, format string, args ...any) {
//	    msg := fmt.Sprintf(format, args...)
//	    log.Printf("[%s] %s", level, msg)
//	})
type LoggerFunc func(level, format string, args ...any)

// LogInfo implements the Logger interface.
func (f LoggerFunc) LogInfo(format string, args ...any) {
	f("INFO", format, args...)
}

// LogError implements the Logger interface.
func (f LoggerFunc) LogError(format string, args ...any) {
	f("ERROR", format, args...)
}

// LogWarn implements the Logger interface.
func (f LoggerFunc) LogWarn(format string, args ...any) {
	f("WARN", format, args...)
}
