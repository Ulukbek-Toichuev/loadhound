/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package parse

import (
	"regexp"
	"strings"
	"text/template"

	"github.com/Ulukbek-Toichuev/loadhound/pkg"
)

const (
	QueryTypeExec    = "exec"
	QueryTypeRead    = "query"
	QueryTypeUnknown = "unknown"
)

// ParseError represents an error that occurred during query parsing.
type ParseError struct {
	Message string
	Err     error
}

func NewParseError(msg string, err error) *ParseError {
	return &ParseError{msg, err}
}

func (e *ParseError) Error() string {
	return e.Message
}

func (e *ParseError) Unwrap() error {
	return e.Err
}

// PreparedQuery holds cleaned query text and its type.
type PreparedQuery struct {
	RawSQL    string
	QueryType string
}

func NewPreparedQuery(sql string, typ string) *PreparedQuery {
	return &PreparedQuery{RawSQL: sql, QueryType: typ}
}

// GetPrepareQuery cleans the query and identify its type.
func GetPreparedQuery(query string) (*PreparedQuery, error) {
	clean := removeComments(query)
	if clean == "" {
		return nil, NewParseError("query contains only comments", nil)
	}
	return identifyQuery(clean), nil
}

func removeComments(sql string) string {
	reMultiline := regexp.MustCompile(`(?s)/\*.*?\*/`)
	sql = reMultiline.ReplaceAllString(sql, "")

	reSingleLine := regexp.MustCompile(`(?m)--.*$`)
	sql = reSingleLine.ReplaceAllString(sql, "")

	return strings.TrimSpace(sql)
}

func identifyQuery(sql string) *PreparedQuery {
	upper := strings.ToUpper(sql)
	switch {
	case strings.HasPrefix(upper, "INSERT"),
		strings.HasPrefix(upper, "UPDATE"),
		strings.HasPrefix(upper, "DELETE"):
		return NewPreparedQuery(sql, QueryTypeExec)
	case strings.HasPrefix(upper, "SELECT"),
		strings.HasPrefix(upper, "WITH"):
		return NewPreparedQuery(sql, QueryTypeRead)
	default:
		return NewPreparedQuery(sql, QueryTypeUnknown)
	}
}

// ParseQueryTemplate parses a SQL query with Go templating syntax.
func ParseQueryTemplate(query string) (*template.Template, error) {
	tmpl := template.New(query)
	tmpl, err := tmpl.Parse(query)
	if err != nil {
		return nil, NewParseError("failed to parse query template", err)
	}
	return tmpl, nil
}

// RenderTemplateQuery executes a parsed query template with data functions.
func RenderTemplateQuery(t *template.Template) (string, error) {
	sb := &strings.Builder{}

	data := struct {
		RandIntRange       func(min, max int) int
		RandFloat64InRange func(min, max float64) float64
		RandUUID           func() string
		RandStringInRange  func(min, max int) string
		GetTime            func() string
	}{
		RandIntRange:       pkg.RandIntRange,
		RandFloat64InRange: pkg.RandFloat64InRange,
		RandUUID:           pkg.RandUUID,
		RandStringInRange:  pkg.RandStringInRange,
		GetTime:            pkg.GetTime,
	}

	if err := t.Execute(sb, data); err != nil {
		return "", NewParseError("failed to execute query template", err)
	}
	return sb.String(), nil
}
