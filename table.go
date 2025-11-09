package pglogrepl

import "strings"

// Table represents a qualified table name in the format "schema.table".
//
// It provides methods to extract schema and table names, validate format,
// and perform case-insensitive comparisons.
//
// Example:
//
//	table := pglogrepl.Table("public.users")
//	fmt.Println(table.Schema()) // "public"
//	fmt.Println(table.Name())   // "users"
type Table string

// newTable creates a new Table from schema and table name.
//
// Parameters:
//   - schema: The PostgreSQL schema name (e.g., "public")
//   - name: The table name (e.g., "users")
//
// Returns a Table in the format "schema.name".
func newTable(schema, name string) Table {
	return Table(schema + "." + name)
}

// Valid checks if the table name has the correct format.
//
// A valid table must contain exactly one dot separating schema and table name.
// Returns true if valid, false otherwise.
//
// Example:
//
//	Table("public.users").Valid()    // true
//	Table("users").Valid()            // false
//	Table("public.schema.users").Valid() // false
func (t Table) Valid() bool {
	return strings.Count(t.String(), ".") == 1
}

// Equal performs case-insensitive comparison with another Table.
//
// This is useful for matching tables from different sources that may have
// different casing conventions.
//
// Example:
//
//	t1 := Table("public.Users")
//	t2 := Table("PUBLIC.users")
//	t1.Equal(t2) // true
func (t Table) Equal(o Table) bool {
	return strings.EqualFold(t.String(), o.String())
}

// Schema extracts the schema name from the qualified table name.
//
// If no schema is specified (no dot in the name), returns "public" as the default.
//
// Example:
//
//	Table("public.users").Schema()  // "public"
//	Table("myschema.orders").Schema() // "myschema"
//	Table("users").Schema()          // "public"
func (t Table) Schema() string {
	s := t.String()
	i := strings.Index(s, ".")
	if i == -1 {
		return "public"
	}
	return s[:i]
}

// Name extracts the table name (without schema) from the qualified table name.
//
// If no schema is specified (no dot in the name), returns the entire string.
//
// Example:
//
//	Table("public.users").Name()  // "users"
//	Table("myschema.orders").Name() // "orders"
//	Table("users").Name()          // "users"
func (t Table) Name() string {
	s := t.String()
	i := strings.Index(s, ".")
	if i == -1 {
		return s
	}
	return s[i+1:]
}

// String returns the string representation of the table.
//
// This is the qualified name in "schema.table" format.
func (t Table) String() string {
	return string(t)
}
