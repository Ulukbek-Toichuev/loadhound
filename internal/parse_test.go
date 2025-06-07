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
			err := ValidateQuickRunFields(tc.qr)
			if tc.expectErrMsg != "" {
				var perr *ParseErr
				assert.ErrorAs(t, err, &perr)
				assert.Equal(t, tc.expectErrMsg, perr.Message)
				return
			}
			assert.NoError(t, err)
		})
	}
}
