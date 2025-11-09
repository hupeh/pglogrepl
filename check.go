package pglogrepl

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// CheckResult contains the result of PostgreSQL logical replication configuration check.
type CheckResult struct {
	// Supported indicates if logical replication is supported
	Supported bool
	// WALLevel is the current wal_level setting
	WALLevel string
	// WALLevelCorrect indicates if wal_level is set to "logical"
	WALLevelCorrect bool
	// MaxReplicationSlots is the current max_replication_slots setting
	MaxReplicationSlots int
	// MaxWalSenders is the current max_wal_senders setting
	MaxWalSenders int
	// Issues contains any configuration issues found
	Issues []string
	// Warnings contains any configuration warnings
	Warnings []string
}

// CheckLogicalReplication checks if PostgreSQL is properly configured for logical replication.
//
// This function verifies:
//   - PostgreSQL version (10+)
//   - wal_level is set to "logical"
//   - max_replication_slots > 0
//   - max_wal_senders > 0
//
// It returns a CheckResult with detailed information about the configuration.
//
// Example:
//
//	dsn := "host=localhost port=5432 user=postgres password=secret dbname=mydb"
//	result, err := pglogrepl.CheckLogicalReplication(context.Background(), dsn)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if !result.Supported {
//	    log.Println("Logical replication is not properly configured:")
//	    for _, issue := range result.Issues {
//	        log.Printf("  - %s", issue)
//	    }
//	}
func CheckLogicalReplication(ctx context.Context, dsn string) (*CheckResult, error) {
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer conn.Close(ctx)

	result := &CheckResult{
		Supported: true,
		Issues:    make([]string, 0),
		Warnings:  make([]string, 0),
	}

	// Check PostgreSQL version
	versionResult := conn.ExecParams(ctx, "SHOW server_version", nil, nil, nil, nil)
	row := versionResult.Read()
	if row.Err != nil {
		return nil, fmt.Errorf("failed to get PostgreSQL version: %w", row.Err)
	}
	if len(row.Rows) > 0 && len(row.Rows[0]) > 0 {
		version := string(row.Rows[0][0])
		// Extract major version (e.g., "14.5" -> 14, "10.21" -> 10)
		parts := strings.Split(version, ".")
		if len(parts) > 0 {
			majorVersion, _ := strconv.Atoi(parts[0])
			if majorVersion < 10 {
				result.Supported = false
				result.Issues = append(result.Issues, fmt.Sprintf("PostgreSQL version %s is not supported (requires 10+)", version))
			} else if majorVersion < 14 {
				result.Warnings = append(result.Warnings, fmt.Sprintf("PostgreSQL %s detected. Version 14+ is recommended for streaming large transactions", version))
			}
		}
	}

	// Check wal_level
	walLevelResult := conn.ExecParams(ctx, "SHOW wal_level", nil, nil, nil, nil)
	row = walLevelResult.Read()
	if row.Err != nil {
		return nil, fmt.Errorf("failed to get wal_level: %w", row.Err)
	}
	if len(row.Rows) > 0 && len(row.Rows[0]) > 0 {
		result.WALLevel = string(row.Rows[0][0])
		result.WALLevelCorrect = result.WALLevel == "logical"
		if !result.WALLevelCorrect {
			result.Supported = false
			result.Issues = append(result.Issues, fmt.Sprintf("wal_level is '%s', must be 'logical'", result.WALLevel))
		}
	}

	// Check max_replication_slots
	maxSlotsResult := conn.ExecParams(ctx, "SHOW max_replication_slots", nil, nil, nil, nil)
	row = maxSlotsResult.Read()
	if row.Err != nil {
		return nil, fmt.Errorf("failed to get max_replication_slots: %w", row.Err)
	}
	if len(row.Rows) > 0 && len(row.Rows[0]) > 0 {
		maxSlots, _ := strconv.Atoi(string(row.Rows[0][0]))
		result.MaxReplicationSlots = maxSlots
		if maxSlots == 0 {
			result.Supported = false
			result.Issues = append(result.Issues, "max_replication_slots is 0, must be greater than 0")
		} else if maxSlots < 4 {
			result.Warnings = append(result.Warnings, fmt.Sprintf("max_replication_slots is %d, recommended value is 4 or higher", maxSlots))
		}
	}

	// Check max_wal_senders
	maxSendersResult := conn.ExecParams(ctx, "SHOW max_wal_senders", nil, nil, nil, nil)
	row = maxSendersResult.Read()
	if row.Err != nil {
		return nil, fmt.Errorf("failed to get max_wal_senders: %w", row.Err)
	}
	if len(row.Rows) > 0 && len(row.Rows[0]) > 0 {
		maxSenders, _ := strconv.Atoi(string(row.Rows[0][0]))
		result.MaxWalSenders = maxSenders
		if maxSenders == 0 {
			result.Supported = false
			result.Issues = append(result.Issues, "max_wal_senders is 0, must be greater than 0")
		} else if maxSenders < 4 {
			result.Warnings = append(result.Warnings, fmt.Sprintf("max_wal_senders is %d, recommended value is 4 or higher", maxSenders))
		}
	}

	return result, nil
}

// String returns a formatted string representation of the check result.
func (r *CheckResult) String() string {
	var sb strings.Builder

	sb.WriteString("PostgreSQL Logical Replication Configuration Check\n")
	sb.WriteString("==================================================\n\n")

	if r.Supported {
		sb.WriteString("✓ Logical replication is properly configured\n\n")
	} else {
		sb.WriteString("✗ Logical replication is NOT properly configured\n\n")
	}

	sb.WriteString(fmt.Sprintf("WAL Level: %s", r.WALLevel))
	if r.WALLevelCorrect {
		sb.WriteString(" ✓\n")
	} else {
		sb.WriteString(" ✗ (should be 'logical')\n")
	}

	sb.WriteString(fmt.Sprintf("Max Replication Slots: %d", r.MaxReplicationSlots))
	if r.MaxReplicationSlots > 0 {
		sb.WriteString(" ✓\n")
	} else {
		sb.WriteString(" ✗ (should be > 0)\n")
	}

	sb.WriteString(fmt.Sprintf("Max WAL Senders: %d", r.MaxWalSenders))
	if r.MaxWalSenders > 0 {
		sb.WriteString(" ✓\n")
	} else {
		sb.WriteString(" ✗ (should be > 0)\n")
	}

	if len(r.Issues) > 0 {
		sb.WriteString("\nIssues Found:\n")
		for _, issue := range r.Issues {
			sb.WriteString(fmt.Sprintf("  - %s\n", issue))
		}
	}

	if len(r.Warnings) > 0 {
		sb.WriteString("\nWarnings:\n")
		for _, warning := range r.Warnings {
			sb.WriteString(fmt.Sprintf("  - %s\n", warning))
		}
	}

	if !r.Supported {
		sb.WriteString("\n=== Configuration Fix Options ===\n\n")

		sb.WriteString("Option 1: Edit postgresql.conf manually\n")
		sb.WriteString("----------------------------------------\n")
		if !r.WALLevelCorrect {
			sb.WriteString("  wal_level = logical\n")
		}
		if r.MaxReplicationSlots == 0 {
			sb.WriteString("  max_replication_slots = 4\n")
		}
		if r.MaxWalSenders == 0 {
			sb.WriteString("  max_wal_senders = 4\n")
		}
		sb.WriteString("\nThen restart PostgreSQL for changes to take effect.\n\n")

		sb.WriteString("Option 2: Use SQL commands (requires SUPERUSER)\n")
		sb.WriteString("------------------------------------------------\n")
		if !r.WALLevelCorrect {
			sb.WriteString("  ALTER SYSTEM SET wal_level = 'logical';\n")
		}
		if r.MaxReplicationSlots == 0 {
			sb.WriteString("  ALTER SYSTEM SET max_replication_slots = 4;\n")
		}
		if r.MaxWalSenders == 0 {
			sb.WriteString("  ALTER SYSTEM SET max_wal_senders = 4;\n")
		}
		sb.WriteString("  SELECT pg_reload_conf();\n")
		sb.WriteString("\nThen restart PostgreSQL for changes to take effect.\n")
	}

	return sb.String()
}

// ConfigSQL returns SQL statements to configure logical replication.
//
// These statements use ALTER SYSTEM to modify postgresql.conf, which requires:
//   - Superuser privileges
//   - PostgreSQL restart after execution
//
// This method returns the SQL even if the configuration is already correct,
// allowing you to see what would be needed.
//
// Example:
//
//	result, _ := pglogrepl.CheckLogicalReplication(ctx, dsn)
//	if !result.Supported {
//	    sql := result.ConfigSQL()
//	    log.Printf("Run these commands as superuser:\n%s", sql)
//	}
func (r *CheckResult) ConfigSQL() string {
	var sb strings.Builder

	sb.WriteString("-- PostgreSQL Logical Replication Configuration\n")
	sb.WriteString("-- These commands require superuser privileges\n")
	sb.WriteString("-- After running, restart PostgreSQL for changes to take effect\n\n")

	needsRestart := false

	if !r.WALLevelCorrect {
		sb.WriteString("ALTER SYSTEM SET wal_level = 'logical';\n")
		needsRestart = true
	}

	if r.MaxReplicationSlots < 4 {
		sb.WriteString("ALTER SYSTEM SET max_replication_slots = 4;\n")
		needsRestart = true
	}

	if r.MaxWalSenders < 4 {
		sb.WriteString("ALTER SYSTEM SET max_wal_senders = 4;\n")
		needsRestart = true
	}

	if needsRestart {
		sb.WriteString("\n-- Reload configuration (for non-restart parameters)\n")
		sb.WriteString("SELECT pg_reload_conf();\n\n")
		sb.WriteString("-- Then restart PostgreSQL:\n")
		sb.WriteString("-- sudo systemctl restart postgresql\n")
		sb.WriteString("-- or\n")
		sb.WriteString("-- pg_ctl restart -D /path/to/data\n")
	} else {
		sb.WriteString("-- No configuration changes needed\n")
	}

	return sb.String()
}
