package pglogrepl

import "strings"

type Table string

func newTable(schema, name string) Table {
	return Table(schema + "." + name)
}

func (t Table) Valid() bool {
	return strings.Count(t.String(), ".") == 1
}

func (t Table) Equal(o Table) bool {
	return strings.EqualFold(t.String(), o.String())
}

func (t Table) Schema() string {
	s := t.String()
	i := strings.Index(s, ".")
	if i == -1 {
		return "public"
	}
	return s[:i]
}

func (t Table) Name() string {
	s := t.String()
	i := strings.Index(s, ".")
	if i == -1 {
		return s
	}
	return s[i+1:]
}

func (t Table) String() string {
	return string(t)
}
