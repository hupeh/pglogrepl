# pglogrepl

A Go package for PostgreSQL logical replication with both full and incremental synchronization support.

[中文文档](./README.zh-Hans.md)

## Features

- **Full Synchronization**: Initial data sync using consistent PostgreSQL snapshots
- **Incremental Synchronization**: Real-time change tracking via logical replication
- **Automatic Recovery**: LSN persistence for crash recovery and resumption
- **Event-Driven**: Callback-based architecture for data change events
- **Thread-Safe**: Concurrent-safe operations throughout
- **Schema Filtering**: Support for specific schemas and tables
- **Auto-Management**: Automatic publication and replication slot handling

## Installation

```bash
go get github.com/hupeh/pglogrepl
```

## Prerequisites

### PostgreSQL Configuration

Your PostgreSQL server must be configured for logical replication:

1. **Check if logical replication is supported:**

   ```go
   package main

   import (
       "context"
       "log"

       "github.com/hupeh/pglogrepl"
   )

   func main() {
       dsn := "host=localhost port=5432 user=postgres password=secret dbname=mydb"
       result, err := pglogrepl.CheckLogicalReplication(context.Background(), dsn)
       if err != nil {
           log.Fatal(err)
       }

       log.Println(result.String())

       if !result.Supported {
           log.Fatal("Logical replication is not properly configured")
       }
   }
   ```

2. **If not configured, edit `postgresql.conf`:**

   ```ini
   wal_level = logical
   max_replication_slots = 4
   max_wal_senders = 4
   ```

3. Restart PostgreSQL to apply changes

4. Grant replication privileges to your user:

   ```sql
   ALTER USER your_user WITH REPLICATION;
   ```

### Supported PostgreSQL Versions

- PostgreSQL 10+
- Recommended: PostgreSQL 14+ (for streaming large transactions)

## Quick Start

```go
package main

import (
    "context"
    "log"

    "github.com/hupeh/pglogrepl"
)

func main() {
    // Configure connection
    config := pglogrepl.Config{
        Host:     "localhost",
        Port:     5432,
        Username: "postgres",
        Password: "password",
        Database: "mydb",
        Schema:   "public",
        Tables:   []string{"users", "orders"}, // Empty for all tables
        LSNFile:  "/var/lib/myapp/lsn.dat",
    }

    // Create replication instance
    repl := pglogrepl.New(config)

    // Register event callbacks
    repl.SetCallback(pglogrepl.EventInsert, func(table pglogrepl.Table, data map[string]any) {
        log.Printf("[INSERT] %s: %v", table.Name(), data)
    })

    repl.SetCallback(pglogrepl.EventUpdate, func(table pglogrepl.Table, data map[string]any) {
        log.Printf("[UPDATE] %s: %v", table.Name(), data)
    })

    repl.SetCallback(pglogrepl.EventDelete, func(table pglogrepl.Table, data map[string]any) {
        log.Printf("[DELETE] %s: %v", table.Name(), data)
    })

    // Start replication
    ctx := context.Background()
    if err := repl.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer repl.Stop()

    // Block forever (or until signal)
    select {}
}
```

## How It Works

### Initial Start (No LSN)

1. Creates a consistent database snapshot with `REPEATABLE READ` isolation
2. Reads all existing rows from configured tables
3. Dispatches `EventInsert` for each existing row
4. Creates a replication slot and publication
5. Transitions to incremental synchronization
6. Begins consuming WAL changes from the snapshot position

### Resume (With LSN)

1. Loads the saved LSN from file
2. Validates the LSN against current configuration
3. Connects to the existing replication slot
4. Resumes consuming WAL changes from the saved position
5. Dispatches events for each data modification

### LSN Persistence

The Log Sequence Number (LSN) tracks the current position in the PostgreSQL Write-Ahead Log:

- **Write**: LSN is written to file on every change (buffered)
- **Sync**: Automatically synced to disk every minute
- **Format**: `checksum/LSN_string` (e.g., `1234567890/0/16B2E58`)
- **Recovery**: Prevents duplicate processing after crashes

## Configuration

### Config Fields

| Field      | Type       | Description                                      | Default               |
| ---------- | ---------- | ------------------------------------------------ | --------------------- |
| `Host`     | `string`   | PostgreSQL server hostname                       | `localhost`           |
| `Port`     | `int`      | PostgreSQL server port                           | `5432`                |
| `Username` | `string`   | Database user (requires REPLICATION privilege)   | -                     |
| `Password` | `string`   | Database password                                | -                     |
| `Database` | `string`   | Target database name                             | -                     |
| `SSLMode`  | `string`   | SSL mode (disable/require/verify-ca/verify-full) | `disable`             |
| `Tables`   | `[]string` | Tables to replicate (empty = all tables)         | `[]` (all)            |
| `Schema`   | `string`   | PostgreSQL schema name                           | `public`              |
| `PubName`  | `string`   | Publication name                                 | `pglogrepl_demo`      |
| `SlotName` | `string`   | Replication slot name                            | `{PubName}_sync_slot` |
| `LSNFile`  | `string`   | LSN persistence file path                        | -                     |
| `Logger`   | `Logger`   | Custom logger implementation                     | stdout logger         |

### Custom Logger

Implement the `Logger` interface for custom logging:

```go
type MyLogger struct{}

func (l *MyLogger) LogInfo(format string, args ...any) {
    log.Printf("[INFO] "+format, args...)
}

func (l *MyLogger) LogError(format string, args ...any) {
    log.Printf("[ERROR] "+format, args...)
}

func (l *MyLogger) LogWarn(format string, args ...any) {
    log.Printf("[WARN] "+format, args...)
}

config.Logger = &MyLogger{}
```

Or use the function adapter:

```go
config.Logger = pglogrepl.LoggerFunc(func(level, format string, args ...any) {
    msg := fmt.Sprintf(format, args...)
    log.Printf("[%s] %s", level, msg)
})
```

## Event Handling

### Event Types

- `EventInsert`: Triggered when a new row is inserted
- `EventUpdate`: Triggered when a row is modified
- `EventDelete`: Triggered when a row is deleted
- `EventTruncate`: Triggered when a table is truncated (not fully implemented)

### Callback Data

The callback receives:

- `table`: Qualified table name (includes schema)
- `data`: Map of column names to values

```go
repl.SetCallback(pglogrepl.EventUpdate, func(table pglogrepl.Table, data map[string]any) {
    schema := table.Schema() // e.g., "public"
    name := table.Name()     // e.g., "users"

    if userID, ok := data["id"].(int64); ok {
        log.Printf("User %d updated in %s.%s", userID, schema, name)
    }
})
```

### Notes on DELETE Events

For `DELETE` events to include old row data, you must set the table's replica identity:

```sql
-- Option 1: Full row (all columns)
ALTER TABLE users REPLICA IDENTITY FULL;

-- Option 2: Using primary key (default, only key columns)
ALTER TABLE users REPLICA IDENTITY DEFAULT;

-- Option 3: Using a unique index
ALTER TABLE users REPLICA IDENTITY USING INDEX users_email_key;
```

## Status Monitoring

### Status Codes

- `StatusStopped`: Replication is not running
- `StatusStarting`: Initializing connections and slots
- `StatusSyncing`: Performing full synchronization
- `StatusListening`: Active incremental replication
- `StatusStopping`: Gracefully shutting down

### Checking Status

```go
status := repl.Status()
statusName := pglogrepl.StatusName(status)
log.Printf("Replication status: %s", statusName)

if err := repl.Err(); err != nil {
    log.Printf("Replication error: %v", err)
}
```

## Graceful Shutdown

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/hupeh/pglogrepl"
)

func main() {
    config := pglogrepl.Config{
        // ... configuration
    }

    repl := pglogrepl.New(config)

    // Setup signal handling
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

    // Start replication
    ctx := context.Background()
    if err := repl.Start(ctx); err != nil {
        log.Fatal(err)
    }

    // Wait for signal
    <-sigChan
    log.Println("Shutting down...")

    // Graceful stop
    if err := repl.Stop(); err != nil {
        log.Printf("Error during shutdown: %v", err)
    }
}
```

## Advanced Usage

### Filtering Specific Tables

```go
config := pglogrepl.Config{
    // ... other fields
    Tables: []string{"users", "orders", "products"},
}
```

### Replicating All Tables

```go
config := pglogrepl.Config{
    // ... other fields
    Tables: []string{}, // Empty = all tables in schema
}
```

### Custom LSN Sync Interval

The package syncs LSN to disk every minute by default. The sync happens automatically in a background goroutine. For manual control, you can call `lsn.Sync()` directly if you have access to the LSN instance.

### Handling Large Transactions

PostgreSQL 14+ supports streaming large transactions. The package automatically handles:

- `StreamStartMessageV2`: Begin streaming a large transaction
- `StreamCommitMessageV2`: Commit the streamed transaction
- `StreamAbortMessageV2`: Abort the streamed transaction

## Troubleshooting

### Replication Slot Already Exists

If you see errors about existing replication slots:

```sql
-- List all replication slots
SELECT * FROM pg_replication_slots;

-- Drop a specific slot
SELECT pg_drop_replication_slot('slot_name');
```

### Publication Already Exists

```sql
-- List all publications
SELECT * FROM pg_publication;

-- Drop a specific publication
DROP PUBLICATION publication_name;
```

### LSN Checksum Mismatch

This occurs when the configuration (database, schema, tables, etc.) changes but the LSN file remains. Solutions:

1. Delete the LSN file to force full resync
2. Call `lsn.Reset()` to clear the LSN
3. Ensure your configuration matches the previous run

### Permission Denied

Ensure your PostgreSQL user has the required privileges:

```sql
-- Grant replication privilege
ALTER USER your_user WITH REPLICATION;

-- Grant necessary table permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_user;
```

## Performance Considerations

1. **LSN Sync Frequency**: LSN is synced to disk every minute. More frequent syncs increase I/O but reduce potential data loss window.

2. **Event Callbacks**: Keep callbacks lightweight. Heavy processing should be done asynchronously:

   ```go
   eventChan := make(chan Event, 1000)

   repl.SetCallback(pglogrepl.EventInsert, func(table pglogrepl.Table, data map[string]any) {
       eventChan <- Event{Table: table, Data: data}
   })

   // Process events in separate goroutine
   go processEvents(eventChan)
   ```

3. **Network Latency**: Use a dedicated connection or connection pooling for replication to avoid blocking other operations.

4. **WAL Retention**: Monitor your WAL disk usage. Inactive replication slots prevent WAL cleanup.

## License

This package is part of the go-slim framework.

## Contributing

Contributions are welcome! Please ensure:

- Code follows Go conventions
- All tests pass
- Documentation is updated
- Commit messages are clear

## Support

For issues and questions:

- GitHub Issues: [pglogrepl issues](https://github.com/hupeh/pglogrepl/issues)
- Documentation: [Go package documentation](https://pkg.go.dev/github.com/hupeh/pglogrepl)
