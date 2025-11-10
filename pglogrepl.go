// Package pglogrepl provides PostgreSQL logical replication support with both full and incremental synchronization.
//
// This package implements a complete solution for tracking PostgreSQL database changes using logical replication.
// It supports initial full synchronization using consistent snapshots and continuous incremental synchronization
// through logical replication slots.
//
// # Features
//
//   - Full table synchronization using consistent snapshots
//   - Incremental synchronization via logical replication
//   - Automatic publication and replication slot management
//   - LSN (Log Sequence Number) persistence for crash recovery
//   - Event callbacks for INSERT, UPDATE, DELETE, and TRUNCATE operations
//   - Support for schema filtering and table-specific replication
//
// # Basic Usage
//
//	config := pglogrepl.Config{
//	    Host:     "localhost",
//	    Port:     5432,
//	    Username: "postgres",
//	    Password: "password",
//	    Database: "mydb",
//	    Tables:   []string{"users", "orders"}, // or empty for all tables
//	    Schema:   "public",
//	    LSNFile:  "/var/lib/myapp/lsn.dat",
//	}
//
//	repl := pglogrepl.New(config)
//
//	// Set event callbacks
//	repl.SetCallback(pglogrepl.EventInsert, func(table pglogrepl.Table, data map[string]any) {
//	    log.Printf("INSERT on %s: %v", table, data)
//	})
//
//	repl.SetCallback(pglogrepl.EventUpdate, func(table pglogrepl.Table, data map[string]any) {
//	    log.Printf("UPDATE on %s: %v", table, data)
//	})
//
//	// Start replication
//	if err := repl.Start(context.Background()); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Gracefully stop
//	defer repl.Stop()
//
// # Synchronization Process
//
// When starting without a valid LSN (first run):
//
//  1. Creates a database snapshot with REPEATABLE READ isolation
//  2. Performs full table synchronization from the snapshot
//  3. Transitions to incremental synchronization mode
//  4. Starts listening for WAL changes via logical replication
//
// When starting with a valid LSN (recovery):
//
//  1. Directly starts incremental synchronization from the saved LSN
//  2. Continues processing WAL changes from where it left off
//
// # LSN Persistence
//
// The package maintains a Log Sequence Number (LSN) file to track replication progress.
// This enables crash recovery and prevents data loss or duplication:
//
//   - LSN is written to file on every change (buffered)
//   - Periodic sync to disk (every minute by default)
//   - Automatic recovery on restart
//   - Checksum validation for file integrity
//
// # Status Monitoring
//
// The replication process goes through several states:
//
//   - StatusStopped: Not running
//   - StatusStarting: Initializing connections and slots
//   - StatusSyncing: Performing full synchronization
//   - StatusListening: Active incremental replication
//   - StatusStopping: Gracefully shutting down
//
// Check current status with:
//
//	status := repl.Status()
//	name := pglogrepl.StatusName(status)
//
// # Requirements
//
//   - PostgreSQL 10+ with logical replication enabled
//   - wal_level = logical in postgresql.conf
//   - max_replication_slots > 0
//   - Database user with REPLICATION privilege
//
// # Error Handling
//
// Any error during replication will cause the process to stop.
// Check for errors with:
//
//	if err := repl.Err(); err != nil {
//	    log.Printf("Replication error: %v", err)
//	}
package pglogrepl

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// Config holds the configuration for PostgreSQL logical replication.
//
// All connection parameters follow the standard PostgreSQL connection string format.
// The replication process will create a publication and replication slot automatically
// based on the provided names (or use defaults if not specified).
type Config struct {
	// Host is the PostgreSQL server hostname or IP address.
	// Defaults to "localhost" if not specified.
	Host string
	// Port is the PostgreSQL server port number.
	// Defaults to 5432 if not specified.
	Port int
	// Username is the PostgreSQL user for authentication.
	// This user must have REPLICATION privilege.
	Username string
	// Password is the PostgreSQL user password.
	Password string
	// Database is the target database name for replication.
	Database string
	// SSLMode controls the SSL/TLS connection mode.
	// Valid values: disable, require, verify-ca, verify-full.
	// Defaults to "disable" if not specified.
	SSLMode string
	// Tables is the list of table names to replicate (without schema prefix).
	// If empty, all tables in the schema will be replicated.
	// Example: []string{"users", "orders", "products"}
	Tables []string
	// Schema is the PostgreSQL schema name.
	// Defaults to "public" if not specified.
	Schema string
	// PubName is the name for the logical replication publication.
	// Defaults to "pglogrepl_demo" if not specified.
	PubName string
	// SlotName is the name for the replication slot.
	// Defaults to PubName + "_sync_slot" if not specified.
	SlotName string
	// LSNFile is the file path for persisting the LSN (Log Sequence Number).
	// This file is used for crash recovery and prevents data loss.
	// The file contains the last processed WAL position.
	LSNFile string
	// Logger is the custom logger for replication events.
	// If nil, a default stdout logger will be used.
	Logger Logger
}

// PgLogRepl manages PostgreSQL logical replication with full and incremental synchronization.
//
// It maintains connections to PostgreSQL, tracks replication progress via LSN,
// and dispatches data change events to registered callbacks.
//
// The replication process is thread-safe and supports graceful shutdown.
type PgLogRepl struct {
	dsn          string
	database     string
	schema       string
	tables       []Table
	pubName      string
	slotName     string
	forAllTables bool

	lsn *LSN
	log Logger

	mu  sync.RWMutex
	cbs map[Event]EventCallback
	err error

	status atomic.Int32
	closed chan chan struct{}
}

// New creates a new PgLogRepl instance with the provided configuration.
//
// This function initializes the replication manager with default values for
// unspecified configuration fields. It does not establish database connections
// or start replication - call Start() to begin the replication process.
//
// The function will:
//   - Build the PostgreSQL connection string
//   - Initialize the LSN tracking file
//   - Set up event callback registry
//   - Configure logging (uses stdout if no logger provided)
//   - Sort and validate table names if specified
//
// Example:
//
//	config := pglogrepl.Config{
//	    Host:     "localhost",
//	    Username: "postgres",
//	    Password: "secret",
//	    Database: "mydb",
//	    Tables:   []string{"users", "orders"},
//	    LSNFile:  "/var/lib/app/lsn.dat",
//	}
//	repl := pglogrepl.New(config)
func New(config Config) *PgLogRepl {
	schema := cmp.Or(config.Schema, "public")
	publication := cmp.Or(config.PubName, "pglogrepl_demo")

	repl := &PgLogRepl{
		dsn: fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			cmp.Or(config.Host, "localhost"),
			cmp.Or(config.Port, 5432),
			config.Username, config.Password, config.Database,
			cmp.Or(config.SSLMode, "disable"),
		),
		database:     config.Database,
		schema:       schema,
		pubName:      publication,
		slotName:     cmp.Or(config.SlotName, publication+"_sync_slot"),
		forAllTables: len(config.Tables) == 0,

		lsn:    &LSN{filename: config.LSNFile},
		cbs:    make(map[Event]EventCallback),
		log:    config.Logger,
		closed: make(chan chan struct{}),
	}

	if n := len(config.Tables); n > 0 {
		repl.tables = make([]Table, n)
		for i, name := range config.Tables {
			repl.tables[i] = newTable(schema, name)
		}
		slices.SortFunc(repl.tables, func(a, b Table) int {
			return strings.Compare(a.String(), b.String())
		})
	}

	if repl.log == nil {
		repl.log = LoggerFunc(func(level, format string, args ...any) {
			if len(args) > 0 {
				format = fmt.Sprintf(format, args...)
			}
			fmt.Fprintf(
				os.Stdout,
				"%s %s [pglogrepl] %s\n",
				time.Now().Format(time.DateTime),
				level,
				format,
			)
		})
	}

	return repl
}

// DSN returns the PostgreSQL connection string used by this replication instance.
//
// The DSN (Data Source Name) contains the connection parameters including host, port,
// username, password, database, and SSL mode. This can be useful for:
//   - Debugging connection issues
//   - Creating additional connections with the same parameters
//   - Passing to CheckLogicalReplication() for configuration verification
//
// Example:
//
//	repl := pglogrepl.New(config)
//	dsn := repl.DSN()
//
//	// Check configuration before starting
//	result, err := pglogrepl.CheckLogicalReplication(context.Background(), dsn)
//	if !result.Supported {
//	    log.Fatal("PostgreSQL not configured for logical replication")
//	}
func (p *PgLogRepl) DSN() string {
	return p.dsn
}

// Status returns the current replication status.
//
// Returns one of the following status codes:
//   - StatusStopped: Replication is not running
//   - StatusStarting: Replication is initializing
//   - StatusSyncing: Performing full table synchronization
//   - StatusListening: Active incremental replication
//   - StatusStopping: Gracefully shutting down
//
// Use StatusName() to convert the status code to a human-readable string.
func (p *PgLogRepl) Status() int32 {
	return p.status.Load()
}

// Err returns the last error that occurred during replication.
//
// If replication stopped due to an error, this method will return that error.
// Returns nil if no error has occurred or if the error has been cleared.
//
// This method is thread-safe.
func (p *PgLogRepl) Err() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.err
}

func (p *PgLogRepl) setError(err error) {
	p.mu.Lock()
	p.err = err
	p.mu.Unlock()

	p.Stop()
}

func (p *PgLogRepl) hasTable(table Table) bool {
	return slices.ContainsFunc(p.tables, table.Equal)
}

// Start begins the replication process.
//
// This method performs the following steps:
//  1. Initializes LSN tracking from file (if exists)
//  2. Discovers or validates configured tables
//  3. Creates publication and replication slot if needed
//  4. Starts full synchronization (if no LSN) or incremental sync (if LSN exists)
//  5. Begins periodic LSN persistence (every minute)
//
// If the LSN file doesn't exist or is invalid, full synchronization will be performed:
//   - Creates a consistent database snapshot
//   - Reads all rows from configured tables
//   - Dispatches INSERT events for each row
//   - Transitions to incremental synchronization
//
// If a valid LSN exists, incremental synchronization starts immediately:
//   - Connects to the replication slot
//   - Starts consuming WAL changes from the saved LSN position
//   - Dispatches events for each data modification
//
// The method returns after the replication process is fully initialized and running.
// Any error during startup will stop the process and return the error.
//
// This method can only be called when status is StatusStopped.
// Calling Start() multiple times will return an error.
//
// Example:
//
//	ctx := context.Background()
//	if err := repl.Start(ctx); err != nil {
//	    log.Fatalf("Failed to start replication: %v", err)
//	}
func (p *PgLogRepl) Start(ctx context.Context) error {
	if !p.status.CompareAndSwap(StatusStopped, StatusStarting) {
		return errors.New("already started")
	}

	if err := p.initLSN(); err != nil {
		p.status.Store(StatusStopped)
		return err
	}

	if err := p.initTables(ctx); err != nil {
		p.status.Store(StatusStopped)
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		exit := <-p.closed
		exit <- struct{}{}
	}()

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := p.lsn.Sync()
				if err != nil {
					p.log.LogError("failed to persist lsn: %v", err)
				}
			}
		}
	}()

	errChan := make(chan error)
	defer close(errChan)

	if !p.lsn.Valid() {
		go p.fullSync(ctx, func(err error) {
			if err != nil {
				p.status.CompareAndSwap(StatusStarting, StatusSyncing)
			}
			errChan <- err
		})
	} else {
		go p.incrementalSync(ctx, func(err error) {
			if err != nil {
				p.status.CompareAndSwap(StatusStarting, StatusListening)
			}
			errChan <- err
		})
	}

	if err := <-errChan; err != nil {
		p.status.Store(StatusStopped)
		return err
	}

	if err := p.Err(); err != nil {
		return err
	}

	return nil
}

func (p *PgLogRepl) initLSN() error {
	tables := "*"
	if !p.forAllTables {
		tables = ""
		for i, t := range p.tables {
			if i > 0 {
				tables += ","
			}
			tables += t.String()
		}
	}

	p.lsn.checksum = crc32.ChecksumIEEE(fmt.Appendf(
		nil, "%s %s %s %s %s",
		p.database,
		p.schema,
		tables,
		p.pubName,
		p.slotName,
	))

	if err := p.lsn.Reload(); err != nil {
		if !errors.Is(err, errLSN) {
			return err
		}
		p.log.LogInfo("%s", err.Error())
	}

	return nil
}

func (p *PgLogRepl) initTables(ctx context.Context) error {
	if !p.forAllTables {
		return nil
	}

	conn, err := pgconn.Connect(ctx, p.dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	sql := "SELECT table_schema||'.'||table_name FROM information_schema.tables WHERE table_schema='%s'"
	results, err := conn.Exec(ctx, fmt.Sprintf(sql, p.schema)).ReadAll()
	if err != nil {
		return err
	}
	for _, result := range results {
		for _, data := range result.Rows {
			p.tables = append(p.tables, Table(string(data[0])))
		}
	}
	slices.SortFunc(p.tables, func(a, b Table) int {
		return strings.Compare(a.String(), b.String())
	})
	return nil
}

func (p *PgLogRepl) fullSync(ctx context.Context, onDone func(error)) {
	// Create snapshot connection
	snapConn, err := pgconn.Connect(ctx, p.dsn)
	if err != nil {
		onDone(err)
		return
	}
	defer snapConn.Close(ctx)

	// Begin transaction with REPEATABLE READ isolation level
	_, err = snapConn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ").ReadAll()
	if err != nil {
		onDone(err)
		return
	}
	// Export snapshot
	rows, err := snapConn.Exec(ctx, "SELECT pg_export_snapshot()").ReadAll()
	if err != nil {
		snapConn.Exec(ctx, "ROLLBACK")
		onDone(err)
		return
	}
	if len(rows) == 0 || len(rows[0].Rows) == 0 {
		snapConn.Exec(ctx, "ROLLBACK")
		onDone(fmt.Errorf("no snapshot returned"))
		return
	}

	// Since we use a consistent snapshot to implement full synchronization,
	// we must ensure the snapshot and sync transactions coexist.
	// Using defer here ensures the transaction remains available during full sync.
	defer snapConn.Exec(ctx, "COMMIT")

	// Get snapshot name
	snapshotName := string(rows[0].Rows[0][0])

	// Create full sync connection
	syncConn, err := pgconn.Connect(ctx, p.dsn)
	if err != nil {
		onDone(err)
		return
	}
	defer syncConn.Close(ctx)

	// Begin transaction
	_, err = syncConn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ").ReadAll()
	if err != nil {
		onDone(err)
		return
	}

	setSnapSQL := fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)
	if _, err = syncConn.Exec(ctx, setSnapSQL).ReadAll(); err != nil {
		syncConn.Exec(ctx, "ROLLBACK")
		onDone(err)
		return
	}

	// Startup completed
	p.status.CompareAndSwap(StatusStarting, StatusSyncing)
	onDone(nil)

	typeMap := pgtype.NewMap()

	// Begin full synchronization
	for _, tbl := range p.tables {
		sql := fmt.Sprintf("SELECT * FROM %s;", tbl.String())
		res := syncConn.ExecParams(ctx, sql, nil, nil, nil, nil)

		colNames := res.FieldDescriptions()
		for res.NextRow() {
			rowData := make(map[string]any)
			vals := res.Values()
			for i, v := range vals {
				data, err := decodeTextColumnData(typeMap, v, colNames[i].DataTypeOID)
				if err != nil {
					p.setError(fmt.Errorf("error decoding column data: %w", err))
					return
				}
				//rowData[colNames[i].Name] = string(v)
				rowData[colNames[i].Name] = data
			}
			if !p.dispatch(EventInsert, tbl, rowData) {
				return
			}
		}

		if _, err = res.Close(); err != nil {
			syncConn.Exec(ctx, "ROLLBACK")
			p.setError(err)
			return
		}
	}

	// Commit transaction
	_, err = syncConn.Exec(ctx, "COMMIT").ReadAll()
	if err != nil {
		p.setError(err)
		return
	}

	// Start listening for database changes to implement incremental sync
	if p.Err() == nil {
		go p.incrementalSync(ctx, func(err error) {
			if err != nil {
				p.setError(err)
				return
			}
			if !p.status.CompareAndSwap(StatusSyncing, StatusListening) {
				p.log.LogError("Failed to listen for incremental sync")
			}
		})
	}
}

// Incremental synchronization
func (p *PgLogRepl) incrementalSync(ctx context.Context, onDone func(error)) {
	conn, err := pgconn.Connect(ctx, p.dsn+" replication=database")
	if err != nil {
		onDone(err)
		return
	}
	defer conn.Close(ctx)

	// Create publication
	err = p.ensurePublication(ctx, conn)
	if err != nil {
		onDone(err)
		return
	}

	// Create replication slot
	err = p.ensureReplicationSlot(ctx, conn)
	if err != nil {
		onDone(err)
		return
	}

	// Ensure sync position is correct
	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		onDone(err)
		return
	}
	p.log.LogDebug(
		"SystemID=%q Timeline=%d XLogPos=%q DBName=%q",
		sysident.SystemID,
		sysident.Timeline,
		sysident.XLogPos,
		sysident.DBName,
	)

	startLSN := min(p.lsn.Get(), sysident.XLogPos)

	// Start replication slot
	err = pglogrepl.StartReplication(ctx, conn, p.slotName, startLSN, pglogrepl.StartReplicationOptions{
		// streaming of large transactions is available since PG 14 (protocol version 2)
		// we also need to set 'streaming' to 'true'
		PluginArgs: []string{
			"proto_version '2'",
			fmt.Sprintf("publication_names '%s'", p.pubName),
			"messages 'true'",
			"streaming 'true'",
		},
	})
	if err != nil {
		onDone(err)
		return
	}
	p.log.LogInfo("logical replication started on slot %q", p.slotName)

	// Startup completed
	onDone(nil)

	clientXLogPos := startLSN
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	relationsV2 := map[uint32]*pglogrepl.RelationMessageV2{}
	typeMap := pgtype.NewMap()

	dispatch := func(event Event, relationID uint32, tuple *pglogrepl.TupleData) bool {
		rel, ok := relationsV2[relationID]
		if !ok {
			p.log.LogError("unknown relation ID %d for event %s", relationID, event)
			return true
		}

		values := map[string]any{}
		for idx, col := range tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple,
				// and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				values[colName], err = decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					p.setError(fmt.Errorf("error decoding column data: %w", err))
					return false
				}
			}
		}

		tbl := newTable(rel.Namespace, rel.RelationName)

		p.log.LogDebug("dispatch event %q, table %q, values %v", event, tbl, values)

		return p.dispatch(event, tbl, values)
	}

	// whenever we get StreamStartMessage we set inStream to true and then pass it to DecodeV2 function
	// on StreamStopMessage we set it back to false
	inStream := false

	// Listen for database changes to implement incremental sync
	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: clientXLogPos,
			})
			if err != nil {
				p.setError(err)
				return
			}
			p.log.LogDebug("sent standby status message at %q", clientXLogPos)
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		subCtx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := conn.ReceiveMessage(subCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			p.setError(fmt.Errorf("ReceiveMessage failed: %w", err))
			return
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			p.setError(fmt.Errorf("received Postgres WAL error: %+v", errMsg))
			return
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			p.log.LogDebug("Received unexpected message: %T", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				p.setError(err)
				return
			}
			p.log.LogDebug(
				"Primary Keepalive Message => ServerWALEnd %q ServerTime %q ReplyRequested %t",
				pkm.ServerWALEnd,
				pkm.ServerTime,
				pkm.ReplyRequested,
			)
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
				p.lsn.Set(clientXLogPos)
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				p.setError(err)
				return
			}

			p.log.LogDebug(
				"XLogData => WALStart %q ServerWALEnd %q ServerTime %q WALData",
				xld.WALStart, xld.ServerWALEnd, xld.ServerTime,
			)

			logicalMsg, err := pglogrepl.ParseV2(xld.WALData, inStream)
			if err != nil {
				p.setError(fmt.Errorf("failed to parse logical replication message: %w", err))
				return
			}
			p.log.LogDebug("Receive a logical replication message: %s", logicalMsg.Type())

			switch logicalMsg := logicalMsg.(type) {
			case *pglogrepl.RelationMessageV2:
				relationsV2[logicalMsg.RelationID] = logicalMsg

			case *pglogrepl.BeginMessage:
				// Indicates the beginning of a group of changes in a transaction.
				// This is only sent for committed transactions. You won't get any
				// events from rolled back transactions.

			case *pglogrepl.CommitMessage:

			case *pglogrepl.InsertMessageV2:
				p.log.LogDebug("insert for xid %d", logicalMsg.Xid)
				if !dispatch(EventInsert, logicalMsg.RelationID, logicalMsg.Tuple) {
					return
				}

			case *pglogrepl.UpdateMessageV2:
				p.log.LogDebug("update for xid %d", logicalMsg.Xid)
				if !dispatch(EventUpdate, logicalMsg.RelationID, logicalMsg.NewTuple) {
					return
				}

			case *pglogrepl.DeleteMessageV2:
				p.log.LogDebug("delete for xid %d", logicalMsg.Xid)
				if !dispatch(EventDelete, logicalMsg.RelationID, logicalMsg.OldTuple) {
					return
				}

			case *pglogrepl.TruncateMessageV2:
				p.log.LogDebug("truncate for xid %d", logicalMsg.Xid)
				// TRUNCATE can affect multiple tables at once
				for _, relationID := range logicalMsg.RelationIDs {
					rel, ok := relationsV2[relationID]
					if !ok {
						p.log.LogError("unknown relation ID %d for truncate event", relationID)
						continue
					}
					tbl := newTable(rel.Namespace, rel.RelationName)
					// TRUNCATE doesn't have row data, pass empty map
					if !p.dispatch(EventTruncate, tbl, map[string]any{}) {
						return
					}
				}

			case *pglogrepl.TypeMessageV2:
			case *pglogrepl.OriginMessage:

			case *pglogrepl.LogicalDecodingMessageV2:
				p.log.LogDebug("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)

			case *pglogrepl.StreamStartMessageV2:
				inStream = true
				p.log.LogDebug("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
			case *pglogrepl.StreamStopMessageV2:
				inStream = false
				p.log.LogDebug("Stream stop message")
			case *pglogrepl.StreamCommitMessageV2:
				p.log.LogDebug("Stream commit message: xid %d", logicalMsg.Xid)
			case *pglogrepl.StreamAbortMessageV2:
				p.log.LogDebug("Stream abort message: xid %d", logicalMsg.Xid)
			default:
				p.log.LogInfo("Unknown message type in pgoutput stream: %T", logicalMsg)
			}

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
				p.lsn.Set(clientXLogPos)
			}
		}
	}
}

func (p *PgLogRepl) ensurePublication(ctx context.Context, conn *pgconn.PgConn) error {
	checkSQL := fmt.Sprintf("SELECT 1 FROM pg_publication WHERE pubname = '%s'", p.pubName)
	pgRes, err := conn.Exec(ctx, checkSQL).ReadAll()
	if err != nil {
		return err
	}

	// Publication exists
	if len(pgRes) > 0 && len(pgRes[0].Rows) > 0 {
		sql := "SELECT schemaname||'.'||tablename FROM pg_publication_tables WHERE pubname = '%s'"
		pgRes, err = conn.Exec(ctx, fmt.Sprintf(sql, p.pubName)).ReadAll()
		if err != nil {
			return err
		}
		var intersects int
		for _, result := range pgRes {
			for _, row := range result.Rows {
				if p.hasTable(Table(row[0])) {
					// Allow more tables in database than specified, so only count intersection
					intersects++
				}
			}
		}
		// Specified tables are a subset of publication tables
		if intersects == len(p.tables) {
			return nil
		}

		// Check and delete replication slot
		checkSQL = "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1"
		res := conn.ExecParams(ctx, checkSQL, nil, nil, nil, nil).Read()
		if res.Err != nil {
			return res.Err
		}
		if len(res.Rows) > 0 {
			dropSlotSQL := fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", p.slotName)
			_, err = conn.Exec(ctx, dropSlotSQL).ReadAll()
			if err != nil {
				return err
			}
		}

		dropPubSQL := fmt.Sprintf("DROP PUBLICATION %s;", p.pubName)
		_, err = conn.Exec(ctx, dropPubSQL).ReadAll()
		if err != nil {
			return err
		}
	}

	createSQL := fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", p.pubName)
	if !p.forAllTables {
		var tables []string
		for _, table := range p.tables {
			tables = append(tables, table.String())
		}
		createSQL = strings.ReplaceAll(
			createSQL,
			"FOR ALL TABLES",
			"FOR TABLE "+strings.Join(tables, ","),
		)
	}
	_, err = conn.Exec(ctx, createSQL).ReadAll()
	return err
}

func (p *PgLogRepl) ensureReplicationSlot(ctx context.Context, conn *pgconn.PgConn) error {
	findSQL := "SELECT slot_name, plugin, slot_type, database, temporary FROM pg_replication_slots WHERE slot_name = '%s'"
	results, err := conn.Exec(ctx, fmt.Sprintf(findSQL, p.slotName)).ReadAll()
	if err != nil {
		return fmt.Errorf("failed to get replication slot: %w", err)
	}

	if len(results) > 0 && len(results[0].Rows) > 0 {
		for _, row := range results[0].Rows {
			if !bytes.EqualFold(row[3], []byte(p.database)) {
				p.log.LogError("expect database '%s', got '%s'", p.database, row[3])
				return fmt.Errorf("expect database '%s', got '%s'", p.database, row[3])
			}

			// Check if attributes are consistent
			if !bytes.EqualFold(row[1], []byte("pgoutput")) ||
				bytes.EqualFold(row[4], []byte{'t'}) ||
				!bytes.EqualFold(row[2], []byte("LOGICAL")) {
				dropSQL := "SELECT pg_drop_replication_slot('%s')"
				_, err = conn.Exec(ctx, fmt.Sprintf(dropSQL, p.slotName)).ReadAll()
				if err != nil {
					return err
				}
				p.log.LogInfo("Dropped replication slot: %q", p.slotName)
				continue
			}

			return nil
		}
	}

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, p.slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary:      false,
		SnapshotAction: "EXPORT_SNAPSHOT",
		Mode:           pglogrepl.LogicalReplication,
	})

	return err
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

// Stop gracefully shuts down the replication process.
//
// This method will:
//   - Signal all active goroutines to stop
//   - Wait for ongoing operations to complete
//   - Sync the final LSN to disk
//   - Close database connections
//   - Transition status to StatusStopped
//
// Stop is idempotent - calling it multiple times is safe and will return nil
// if already stopped or stopping.
//
// Returns the last error that occurred during replication, or nil if no error.
//
// Example:
//
//	// Graceful shutdown on signal
//	sigChan := make(chan os.Signal, 1)
//	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
//	<-sigChan
//	if err := repl.Stop(); err != nil {
//	    log.Printf("Replication stopped with error: %v", err)
//	}
func (p *PgLogRepl) Stop() error {
	switch p.Status() {
	case StatusStopped, StatusStopping:
		return nil
	default:
		p.status.Store(StatusStopping)
	}

	stop := make(chan struct{})
	p.closed <- stop
	<-stop

	p.status.Store(StatusStopped)

	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.err
}
