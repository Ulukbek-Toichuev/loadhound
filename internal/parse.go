/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"errors"
	"fmt"
	"strings"
	"text/template"

	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	"github.com/jmoiron/sqlx"
)

type QueryType int

const (
	QueryTypeUnknown QueryType = iota
	QueryTypeExec
	QueryTypeRead
)

func (qt QueryType) String() string {
	switch qt {
	case QueryTypeExec:
		return "exec"
	case QueryTypeRead:
		return "query"
	default:
		return "unknown"
	}
}

type PreparedQuery struct {
	RawSQL    string
	QueryType string
	Tmpl      *template.Template
}

func NewPreparedQuery(sql string, queryType string) *PreparedQuery {
	return &PreparedQuery{RawSQL: sql, QueryType: queryType}
}

func GetPreparedQuery(query string) (*PreparedQuery, error) {
	return IdentifyQuery(query), nil
}

func IdentifyQuery(sql string) *PreparedQuery {
	upper := strings.ToUpper(sql)
	switch {
	case strings.HasPrefix(upper, "INSERT"),
		strings.HasPrefix(upper, "UPDATE"),
		strings.HasPrefix(upper, "DELETE"):
		return NewPreparedQuery(sql, QueryTypeExec.String())

	case strings.HasPrefix(upper, "SELECT"),
		strings.HasPrefix(upper, "WITH"):
		return NewPreparedQuery(sql, QueryTypeRead.String())

	default:
		return NewPreparedQuery(sql, QueryTypeUnknown.String())
	}
}

type funcs struct {
	RandIntRange       func(min, max int) int
	RandFloat64InRange func(min, max float64) float64
	RandUUID           func() string
	RandStringInRange  func(min, max int) string
	RandBool           func() bool
	GetTime            func() string
}

func RenderQueryParams(t *template.Template) ([]interface{}, error) {
	sb := &strings.Builder{}
	params := make([]interface{}, 0)
	fs := funcs{
		RandIntRange: func(min, max int) int {
			params = append(params, pkg.RandIntRange(min, max))
			return 0
		},
		RandFloat64InRange: func(min, max float64) float64 {
			params = append(params, pkg.RandFloat64InRange(min, max))
			return 0
		},
		RandUUID: func() string {
			params = append(params, pkg.RandUUID())
			return ""
		},
		RandStringInRange: func(min, max int) string {
			params = append(params, pkg.RandStringInRange(min, max))
			return ""
		},
		RandBool: func() bool {
			params = append(params, pkg.RandBool())
			return false
		},
		GetTime: func() string {
			params = append(params, pkg.GetTime())
			return ""
		},
	}
	if err := t.Execute(sb, fs); err != nil {
		return nil, fmt.Errorf("failed to execute query template: %v", err)
	}
	return params, nil
}

func BuildQueryWithPlaceHolders(t *template.Template, driverType string) (string, error) {
	sb := &strings.Builder{}
	data := struct {
		Placeholder string
	}{
		Placeholder: "?",
	}
	if err := t.Execute(sb, data); err != nil {
		return "", fmt.Errorf("failed to execute template: %v", err)
	}
	switch driverType {
	case string(Postgres):
		return sqlx.Rebind(sqlx.DOLLAR, sb.String()), nil
	case string(Mysql):
		return sb.String(), nil
	default:
		return "", errors.New("unknown driver type")
	}
}

func GetQueryTemplate(queryTemplate *QueryTemplateConfig) (*template.Template, error) {
	tmpl := template.New(queryTemplate.Name).Funcs(getFuncMap())
	tmpl, err := tmpl.Parse(queryTemplate.Template)
	if err != nil {
		return nil, err
	}
	return tmpl, nil
}

func BuildQuery(t *template.Template) (string, error) {
	sb := &strings.Builder{}
	if err := t.Execute(sb, nil); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func getFuncMap() map[string]any {
	return template.FuncMap{
		"randBool":           pkg.RandBool,
		"randIntRange":       pkg.RandIntRange,
		"randFloatRange":     pkg.RandFloat64InRange,
		"randUUID":           pkg.RandUUID,
		"randStrRange":       pkg.RandStringInRange,
		"getCurrTime":        pkg.GetTime,
		"defaultPlaceholder": func() string { return "?" },
	}
}
