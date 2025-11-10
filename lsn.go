package pglogrepl

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/jackc/pglogrepl"
)

var errLSN = errors.New("LSN")

// LSN manages a PostgreSQL Log Sequence Number with persistent storage.
//
// It provides thread-safe read/write operations and maintains the LSN position
// in a file for crash recovery. The LSN tracks the current position in the
// PostgreSQL Write-Ahead Log (WAL) during logical replication.
//
// # File Format
//
// The LSN is stored in a text file with the format: "checksum/LSN_string"
// Example: "1234567890/0/16B2E58"
//
// The checksum is calculated from the database, schema, tables, publication,
// and slot names to ensure the LSN file matches the current configuration.
//
// # Usage Pattern
//
//	lsn := &LSN{filename: "/var/lib/app/lsn.dat", checksum: 0x12345678}
//
//	// Load from file
//	if err := lsn.Reload(); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Update LSN (writes to file buffer)
//	lsn.Set(newLSN)
//
//	// Periodically sync to disk
//	ticker := time.NewTicker(time.Minute)
//	go func() {
//	    for range ticker.C {
//	        lsn.Sync()
//	    }
//	}()
//
//	// Clean shutdown
//	defer lsn.Close()
//
// See https://www.postgresql.org/docs/current/datatype-pg-lsn.html for more
// information about PostgreSQL LSN.
type LSN struct {
	filename string
	file     *os.File
	mu       sync.Mutex
	value    atomic.Uint64
	checksum uint32
}

// Valid returns true if the LSN has a non-zero value.
//
// A zero LSN indicates that no replication position has been set,
// typically on the first run before any data has been processed.
func (l *LSN) Valid() bool {
	return l.value.Load() > 0
}

// Set updates the LSN to a new value and writes it to the file.
//
// The new value is written to the file buffer immediately but not synced to disk.
// Call Sync() periodically to ensure durability.
//
// If the new LSN is the same as the current value, no write occurs.
//
// This method is thread-safe.
//
// Example:
//
//	if err := lsn.Set(newPosition); err != nil {
//	    log.Printf("Failed to update LSN: %v", err)
//	}
func (l *LSN) Set(lsn pglogrepl.LSN) error {
	// Theoretically, lsn should not be less than l.value, but since we don't use
	// concurrent processing for logical replication, we only check for inequality here
	if l.value.Swap(uint64(lsn)) != uint64(lsn) {
		return l.write()
	}
	return nil
}

// Get retrieves the current LSN value.
//
// This method is thread-safe and can be called concurrently with Set().
//
// Example:
//
//	currentPos := lsn.Get()
//	fmt.Printf("Current LSN: %s\n", currentPos)
func (l *LSN) Get() pglogrepl.LSN {
	return pglogrepl.LSN(l.value.Load())
}

// Reload reads the LSN value from the file and opens it for writing.
//
// This method should be called once during initialization to restore the
// last saved LSN position. If the file doesn't exist, it will be created
// and the LSN will be initialized to zero.
//
// The file format is validated using the checksum to ensure it matches
// the current configuration. If the checksum doesn't match, an error is returned.
//
// After successful reload, the file remains open for subsequent write operations.
//
// Returns an error if:
//   - The file cannot be read (except for non-existence)
//   - The file format is invalid
//   - The checksum doesn't match
//   - The LSN string cannot be parsed
//
// Example:
//
//	lsn := &LSN{filename: "lsn.dat", checksum: 0x12345678}
//	if err := lsn.Reload(); err != nil {
//	    log.Fatalf("Failed to load LSN: %v", err)
//	}
func (l *LSN) Reload() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If file is already open, close it first
	if l.file != nil {
		l.file.Close()
		l.file = nil
	}

	file, err := os.Open(l.filename)
	if err != nil {
		if os.IsNotExist(err) {
			// File does not exist, create new file
			return l.openFile()
		}
		return fmt.Errorf("failed to reload LSN: %w", err)
	}
	defer file.Close()

	var checksum uint32
	var str string
	n, err := fmt.Fscanf(file, "%d/%s", &checksum, &str)
	if err != nil {
		return fmt.Errorf("failed to scan %w file: %w", errLSN, err)
	}
	if n != 2 {
		if n > 0 {
			return fmt.Errorf("failed to scan %w file", errLSN)
		}
		l.value.Store(0)
		return l.openFile()
	}
	if checksum != l.checksum {
		return fmt.Errorf(
			"failed to scan %w file: expected checksum %x, got %x",
			errLSN, l.checksum, checksum,
		)
	}
	val, err := pglogrepl.ParseLSN(str)
	if err != nil {
		return fmt.Errorf("failed to scan %w file: %w", errLSN, err)
	}

	l.value.Store(uint64(val))

	// Reopen file for writing
	return l.openFile()
}

// Reset sets the LSN to zero and writes it to the file.
//
// This is useful for forcing a full resynchronization on the next start.
// The reset value is written immediately but may not be synced to disk
// until Sync() is called.
//
// Example:
//
//	if err := lsn.Reset(); err != nil {
//	    log.Printf("Failed to reset LSN: %v", err)
//	}
func (l *LSN) Reset() error {
	l.value.Store(0)
	return l.write()
}

// openFile opens the LSN file for reading and writing.
//
// This is an internal method that must be called with the mutex held.
// It creates the file if it doesn't exist.
//
// The file is opened with O_RDWR|O_CREATE flags and 0644 permissions.
func (l *LSN) openFile() error {
	file, err := os.OpenFile(l.filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open %w file: %w", errLSN, err)
	}
	l.file = file
	return nil
}

// write writes the current LSN value to the file without syncing.
//
// This is an internal method that acquires the mutex and writes the LSN
// to the file buffer. The data is not guaranteed to reach disk until
// Sync() is called.
//
// The write process:
//  1. Seeks to the beginning of the file
//  2. Writes the formatted LSN string (checksum/LSN)
//  3. Truncates the file to the new content length
//
// This ensures that shorter LSN values properly overwrite longer ones.
func (l *LSN) write() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		if err := l.openFile(); err != nil {
			return err
		}
	}

	str := fmt.Sprintf("%d/%s", l.checksum, l.Get().String())

	// Seek to the beginning of the file first
	if _, err := l.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek %w file: %w", errLSN, err)
	}

	// Write data
	if _, err := l.file.WriteString(str); err != nil {
		return fmt.Errorf("failed to write %w: %w", errLSN, err)
	}

	// Truncate file (if new content is shorter than old content)
	if err := l.file.Truncate(int64(len(str))); err != nil {
		return fmt.Errorf("failed to truncate %w file: %w", errLSN, err)
	}

	return nil
}

// Sync flushes the file buffer to disk.
//
// This method should be called periodically (e.g., every minute) to ensure
// that LSN updates are persisted. Without calling Sync(), the LSN may remain
// in the OS buffer cache and could be lost on crash or power failure.
//
// It is safe to call Sync() even if no writes have occurred since the last sync.
// If the file is not open, this method returns nil.
//
// This method is thread-safe.
//
// Example:
//
//	// Periodic sync in a goroutine
//	ticker := time.NewTicker(time.Minute)
//	defer ticker.Stop()
//	for range ticker.C {
//	    if err := lsn.Sync(); err != nil {
//	        log.Printf("Failed to sync LSN: %v", err)
//	    }
//	}
func (l *LSN) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil
	}

	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync %w file: %w", errLSN, err)
	}

	return nil
}

// Close performs a final sync and closes the LSN file.
//
// This method should be called during application shutdown to ensure all
// LSN updates are persisted to disk before the process terminates.
//
// The method will:
//  1. Sync any pending writes to disk
//  2. Close the file handle
//  3. Set the internal file pointer to nil
//
// It is safe to call Close() multiple times. Subsequent calls will return nil.
//
// After Close() is called, any subsequent Set() operations will reopen the file.
//
// This method is thread-safe.
//
// Example:
//
//	// Graceful shutdown
//	defer func() {
//	    if err := lsn.Close(); err != nil {
//	        log.Printf("Failed to close LSN file: %v", err)
//	    }
//	}()
func (l *LSN) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil
	}

	// Sync to disk
	if err := l.file.Sync(); err != nil {
		l.file.Close()
		l.file = nil
		return fmt.Errorf("failed to sync %w file on close: %w", errLSN, err)
	}

	// Close file
	if err := l.file.Close(); err != nil {
		l.file = nil
		return fmt.Errorf("failed to close %w file: %w", errLSN, err)
	}

	l.file = nil
	return nil
}

// String returns the string representation of the current LSN.
//
// The format follows PostgreSQL's LSN convention: "XXX/XXXXXXXX"
// where the values are hexadecimal.
//
// Example output: "0/16B2E58"
func (l *LSN) String() string {
	return l.Get().String()
}
