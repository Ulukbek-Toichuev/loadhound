/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"text/template"

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

func GetQueryTemplate(queryTemplate *QueryTemplateConfig, useStmt bool) (*template.Template, error) {
	var tmpl *template.Template

	if useStmt {
		tmpl = template.New(queryTemplate.Name).Funcs(template.FuncMap{
			"randBool":       func() string { return "?" },
			"randIntRange":   func(min, max int) string { return "?" },
			"randFloatRange": func(min, max float64) string { return "?" },
			"randUUID":       func() string { return "?" },
			"randStrRange":   func(min, max int) string { return "?" },
			"getCurrTime":    func() string { return "?" },
			"setBind":        func() string { return "?" },
		})
	} else {
		tmpl = template.New(queryTemplate.Name).Funcs(getFuncMap())
	}
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

func BuildQueryWithBinds(t *template.Template, driverType string) (string, error) {
	sb := &strings.Builder{}
	if err := t.Execute(sb, nil); err != nil {
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

type Func struct {
	Name string
	Args []string
}

var reFuncPattern = regexp.MustCompile(`\{\{\s*(.*?)\s*\}\}`)

// Return slice with Func structs from SQL query:
// [{randIntRange [5 10]} {randBool []}]
func GetFuncs(query string) []Func {
	var funcs []Func
	if len(query) == 0 {
		return funcs
	}

	matches := reFuncPattern.FindAllStringSubmatch(query, -1)
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}

		content := strings.TrimSpace(match[1])

		// Защита: запрещаем вложенные фигурные скобки внутри
		if strings.Contains(content, "{{") || strings.Contains(content, "}}") {
			continue // Игнорируем некорректный шаблон
		}

		// Разбиваем содержимое по токенам
		tokens := strings.Fields(content)
		if len(tokens) == 0 {
			continue
		}

		funcs = append(funcs, Func{
			Name: tokens[0],
			Args: tokens[1:],
		})
	}

	return funcs
}

func CollectFuncs(funcs []Func) ([]func() interface{}, error) {
	result := make([]func() interface{}, 0)

	fnMap := getFuncMap()
	for _, fn := range funcs {
		if value, ok := fnMap[fn.Name]; ok {
			switch fn.Name {
			case "randBool":
				if f, ok := value.(func() bool); ok {
					result = append(result, func() interface{} { return f() })
				}
			case "randIntRange":
				arg1, arg2, err := intArgsValidator(fn.Args)
				if err != nil {
					return nil, err
				}
				if f, ok := value.(func(int, int) int); ok {
					result = append(result, func() interface{} { return f(arg1, arg2) })
				}
			case "randFloatRange":
				arg1, arg2, err := float64ArgsValidator(fn.Args)
				if err != nil {
					return nil, err
				}
				if f, ok := value.(func(float64, float64) float64); ok {
					result = append(result, func() interface{} { return f(arg1, arg2) })
				}
			case "randUUID":
				if f, ok := value.(func() string); ok {
					result = append(result, func() interface{} { return f() })
				}
			case "randStrRange":
				arg1, arg2, err := intArgsValidator(fn.Args)
				if err != nil {
					return nil, err
				}
				if f, ok := value.(func(int, int) string); ok {
					result = append(result, func() interface{} { return f(arg1, arg2) })
				}
			case "getCurrTime":
				if f, ok := value.(func() string); ok {
					result = append(result, func() interface{} { return f() })
				}
			case "setBind":
				if f, ok := value.(func() string); ok {
					result = append(result, func() interface{} { return f() })
				}
			}
		}
	}

	return result, nil
}

func getFuncMap() map[string]any {
	return template.FuncMap{
		"randBool":       RandBool,
		"randIntRange":   RandIntRange,
		"randFloatRange": RandFloat64InRange,
		"randUUID":       RandUUID,
		"randStrRange":   RandStringInRange,
		"getCurrTime":    GetTime,
		"setBind":        func() string { return "?" },
	}
}

func float64ArgsValidator(args []string) (float64, float64, error) {
	if len(args) < 2 {
		return 0, 0, fmt.Errorf("count of transmitted args must be 2")
	}
	arg1, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		return 0, 0, err
	}
	arg2, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return 0, 0, err
	}
	if arg1 >= arg2 {
		return 0, 0, fmt.Errorf("arg1 must be less than arg2")
	}
	return arg1, arg2, nil
}

func intArgsValidator(args []string) (int, int, error) {
	if len(args) < 2 {
		return 0, 0, fmt.Errorf("count of transmitted args must be 2")
	}
	arg1, err := strconv.Atoi(args[0])
	if err != nil {
		return 0, 0, err
	}
	arg2, err := strconv.Atoi(args[1])
	if err != nil {
		return 0, 0, err
	}
	if err := ValidateIntArgs(arg1, arg2); err != nil {
		return 0, 0, err
	}
	return arg1, arg2, nil
}
