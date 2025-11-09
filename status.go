package pglogrepl

const (
	// StatusStopped indicates that replication is not running.
	// This is the initial state and the final state after Stop().
	StatusStopped int32 = iota

	// StatusStarting indicates that replication is initializing.
	// During this phase, connections are established, publications and slots are created,
	// and the LSN file is loaded.
	StatusStarting

	// StatusSyncing indicates that full table synchronization is in progress.
	// This occurs when no valid LSN exists, requiring a complete data copy
	// from a consistent snapshot before starting incremental replication.
	StatusSyncing

	// StatusListening indicates that incremental replication is active.
	// The replication slot is consuming WAL changes and dispatching events.
	StatusListening

	// StatusStopping indicates that graceful shutdown is in progress.
	// Ongoing operations are completing, and resources are being cleaned up.
	StatusStopping
)

// StatusName returns the human-readable name for a status code.
//
// Example:
//
//	status := repl.Status()
//	fmt.Printf("Current status: %s\n", pglogrepl.StatusName(status))
func StatusName(s int32) string {
	switch s {
	case StatusStopped:
		return "stopped"
	case StatusStarting:
		return "starting"
	case StatusSyncing:
		return "syncing"
	case StatusListening:
		return "listening"
	case StatusStopping:
		return "stopping"
	default:
		return "unknown"
	}
}
