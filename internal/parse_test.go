/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateQuickRunFields(t *testing.T) {
	var testCases = []struct {
		name         string
		qr           *QuickRun
		expectErrMsg string
	}{
		{
			name:         "case: valid fields",
			qr:           NewQuickRun("dsn", "select * from users", 2, 5, 0, 0, ""),
			expectErrMsg: "",
		},
		{
			name:         "case: --dsn is required",
			qr:           NewQuickRun("", "select * from users", 2, 5, 0, 0, ""),
			expectErrMsg: "--dsn is required",
		},
		{
			name:         "case: --query is required",
			qr:           NewQuickRun("dsn", "", 2, 5, 0, 0, ""),
			expectErrMsg: "--query is required",
		},
		{
			name:         "case: --workers must be >= 0",
			qr:           NewQuickRun("dsn", "select * from users", -2, 5, 0, 0, ""),
			expectErrMsg: "--workers must be >= 0",
		},
		{
			name:         "case: --iterations must be >= 0",
			qr:           NewQuickRun("dsn", "select * from users", 2, -5, 0, 0, ""),
			expectErrMsg: "--iterations must be >= 0",
		},
		{
			name:         "case: --duration must be >= 0",
			qr:           NewQuickRun("dsn", "select * from users", 2, 0, -10, 0, ""),
			expectErrMsg: "--duration must be >= 0",
		},
		{
			name:         "either --iter or --duration must be set",
			qr:           NewQuickRun("dsn", "select * from users", 2, 0, 0, 0, ""),
			expectErrMsg: "either --iter or --duration must be set",
		},
		{
			name:         "--iter and --duration are mutually exclusive",
			qr:           NewQuickRun("dsn", "select * from users", 2, 10, 10, 0, ""),
			expectErrMsg: "--iter and --duration are mutually exclusive",
		},
		{
			name:         "--pacing must be > 0",
			qr:           NewQuickRun("dsn", "select * from users", 2, 10, 0, -10, ""),
			expectErrMsg: "--pacing must be > 0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateQuickRunFields(tc.qr)
			if tc.expectErrMsg != "" {
				var perr *ParseError
				assert.ErrorAs(t, err, &perr)
				assert.Equal(t, tc.expectErrMsg, perr.Message)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestRemoveComments(t *testing.T) {
	testCases := []struct {
		name        string
		query       string
		expectedMsg string
	}{
		{
			name:        "case: sql script without comments",
			query:       "select * from users",
			expectedMsg: "select * from users",
		},
		{
			name:        "case: sql script with '--' comment in head",
			query:       "-- some comment\nselect * from users",
			expectedMsg: "select * from users",
		},
		{
			name:        "case: sql script with '--' comment in head and tail",
			query:       "-- some comment\nselect * from users\n-- some comment",
			expectedMsg: "select * from users",
		},
		{
			name:        "case: sql script with '/**/' comment in head",
			query:       "/*some comments\nin two lines*/\nselect * from users",
			expectedMsg: "select * from users",
		},
		{
			name:        "case: sql script with '/**/' comment in head and tail",
			query:       "/*some comments\nin two lines*/\nselect * from users\n/*some comments\nin two lines*/",
			expectedMsg: "select * from users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualMsg := removeComments(tc.query)
			assert.Equal(t, tc.expectedMsg, actualMsg)
		})
	}
}

func TestClassifyQuery(t *testing.T) {
	var testCases = []struct {
		name        string
		query       string
		expectedMsg string
	}{
		{
			name:        "case: 'select' query",
			query:       "select * from users;",
			expectedMsg: QueryTypeRead,
		},
		{
			name:        "case: 'CTE' query",
			query:       "WITH temp AS (SELECT 1) SELECT * FROM temp;",
			expectedMsg: QueryTypeRead,
		},
		{
			name:        "case: 'insert' query",
			query:       "insert into users(username) values('uluk');",
			expectedMsg: QueryTypeExec,
		},
		{
			name:        "case: 'update' query",
			query:       "update users set username = Uluk where user_id = 1;",
			expectedMsg: QueryTypeExec,
		},
		{
			name:        "case: 'delete' query",
			query:       "delete from users where user_id = 1;",
			expectedMsg: QueryTypeExec,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := identifyQuery(tc.query)
			assert.Equal(t, tc.query, actual.RawSQL)
			assert.Equal(t, tc.expectedMsg, actual.QueryType)
		})
	}
}
