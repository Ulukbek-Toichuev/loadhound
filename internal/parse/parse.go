/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package parse

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
}

func NewPreparedQuery(sql string, queryType string) *PreparedQuery {
	return &PreparedQuery{RawSQL: sql, QueryType: queryType}
}

func GetPreparedQuery(query string) (*PreparedQuery, error) {
	clean := removeComments(query)
	if clean == "" {
		return nil, errors.New("query contains only comments")
	}
	return identifyQuery(clean), nil
}

func removeComments(sql string) string {
	var result strings.Builder
	inSingleLineComment := false
	inMultiLineComment := 0
	inSingleQuote := false
	inDoubleQuote := false

	i := 0
	for i < len(sql) {
		ch := sql[i]

		// Проверка на начало строки комментария -- (если не в строке и не в блоке)
		if !inSingleQuote && !inDoubleQuote && inMultiLineComment == 0 {
			if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
				inSingleLineComment = true
				i += 2
				// Пропускаем до конца строки
				for i < len(sql) && sql[i] != '\n' {
					i++
				}
				continue
			}
			// Начало многострочного комментария
			if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
				inMultiLineComment++
				i += 2
				continue
			}
		}

		// Проверка на конец многострочного комментария
		if inMultiLineComment > 0 {
			if ch == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				inMultiLineComment--
				i += 2
				continue
			}
			i++
			continue
		}

		// Выход из однострочного комментария
		if inSingleLineComment {
			if ch == '\n' {
				inSingleLineComment = false
				result.WriteByte(ch)
			}
			i++
			continue
		}

		// Обработка строк — отслеживаем кавычки
		if ch == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
		}
		if ch == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
		}

		// Добавляем символ в результат
		result.WriteByte(ch)
		i++
	}

	return strings.TrimSpace(result.String())
}

func identifyQuery(sql string) *PreparedQuery {
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

func GetQueryTemplate(queryTemplate string) (*template.Template, error) {
	tmpl := template.New(queryTemplate)
	tmpl, err := tmpl.Parse(queryTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query template: %v", err)
	}
	return tmpl, nil
}

type funcs struct {
	RandIntRange       func(min, max int) int
	RandFloat64InRange func(min, max float64) float64
	RandUUID           func() string
	RandStringInRange  func(min, max int) string
	RandBool           func() bool
	GetTime            func() string
}

func RenderTemplateQuery(t *template.Template) (string, error) {
	sb := &strings.Builder{}
	fs := funcs{
		RandIntRange:       pkg.RandIntRange,
		RandFloat64InRange: pkg.RandFloat64InRange,
		RandUUID:           pkg.RandUUID,
		RandStringInRange:  pkg.RandStringInRange,
		RandBool:           pkg.RandBool,
		GetTime:            pkg.GetTime,
	}

	if err := t.Execute(sb, fs); err != nil {
		return "", fmt.Errorf("failed to execute query template: %v", err)
	}
	return sb.String(), nil
}

func RenderQueryWithPlaceholders(t *template.Template, driverType string) (string, error) {
	sb := &strings.Builder{}

	data := struct {
		RandIntRange       func(min, max int) string
		RandFloat64InRange func(min, max float64) string
		RandUUID           func() string
		RandStringInRange  func(min, max int) string
		RandBool           func() string
		GetTime            func() string
	}{
		RandIntRange:       func(min, max int) string { return "?" },
		RandFloat64InRange: func(min, max float64) string { return "?" },
		RandUUID:           func() string { return "?" },
		RandStringInRange:  func(min, max int) string { return "?" },
		RandBool:           func() string { return "?" },
		GetTime:            func() string { return "?" },
	}

	if err := t.Execute(sb, data); err != nil {
		return "", fmt.Errorf("failed to execute query template: %v", err)
	}
	if driverType == string(pkg.Mysql) {
		return sb.String(), nil
	}

	switch driverType {
	case string(pkg.Postgres):
		return sqlx.Rebind(sqlx.DOLLAR, sb.String()), nil
	default:
		return "", errors.New("unknown driver type")
	}
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
