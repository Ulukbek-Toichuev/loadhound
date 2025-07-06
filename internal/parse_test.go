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

const sqlTemplate = `
-- *******************************************************************
-- 1) Prepare data using CTE
-- *******************************************************************
WITH recent_users AS (
    SELECT id, username, email, created_at
    FROM users
    WHERE active = {{randBool}}
      AND created_at > '{{getCurrTime}}'::timestamp - INTERVAL '{{randIntRange 1 365}} days'
),
avg_scores AS (
    SELECT user_id, AVG(score) AS avg_score
    FROM user_scores
    WHERE score BETWEEN {{randFloatRange 0 50}} AND {{randFloatRange 51 100}}
    GROUP BY user_id
),
-- *******************************************************************
-- 2) Update another part of users
-- *******************************************************************
random_updates AS (
    UPDATE users
    SET
        login_count = login_count + {{randIntRange 1 10}},
        last_login = '{{getCurrTime}}'
    WHERE id IN (
        SELECT id FROM recent_users
        ORDER BY created_at DESC
        LIMIT {{randIntRange 5 20}}
    )
    RETURNING id, username
)
-- *******************************************************************
-- 3) Logging result
-- *******************************************************************
INSERT INTO audit_logs (id, entity_id, action, changed_at, random_flag)
VALUES
    ('{{randUUID}}', (SELECT id FROM random_updates ORDER BY id   LIMIT 1), 'update', '{{getCurrTime}}', {{randBool}}),
    ('{{randUUID}}', (SELECT id FROM random_updates ORDER BY id DESC LIMIT 1), 'update', '{{getCurrTime}}', {{randBool}}),
    ('{{randUUID}}', '{{defaultPlaceholder}}', 'insert', '{{getCurrTime}}', {{randBool}}),
    ('{{randUUID}}', '{{randStrRange 10 15}}', 'delete', '{{getCurrTime}}', {{randBool}});
-- *******************************************************************
-- 4) Final SELECT with JOIN
-- *******************************************************************
SELECT
    u.id,
    u.username,
    a.action,
    ascore.avg_score
FROM users u
LEFT JOIN audit_logs    a     ON u.id = a.entity_id
LEFT JOIN avg_scores    ascore ON u.id = ascore.user_id
WHERE u.status = {{defaultPlaceholder}}
ORDER BY a.changed_at DESC
LIMIT {{randIntRange 10 100}};
`

func BenchmarkBuildQuery(b *testing.B) {
	q := QueryTemplateConfig{Name: "benchmark query", Template: sqlTemplate}
	tmpl, err := GetQueryTemplate(&q, false)
	if err != nil {
		return
	}
	for range b.N {
		BuildQuery(tmpl)
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
			expectedMsg: QueryTypeRead.String(),
		},
		{
			name:        "case: 'CTE' query",
			query:       "WITH temp AS (SELECT 1) SELECT * FROM temp;",
			expectedMsg: QueryTypeRead.String(),
		},
		{
			name:        "case: 'insert' query",
			query:       "insert into users(username) values('uluk');",
			expectedMsg: QueryTypeExec.String(),
		},
		{
			name:        "case: 'update' query",
			query:       "update users set username = Uluk where user_id = 1;",
			expectedMsg: QueryTypeExec.String(),
		},
		{
			name:        "case: 'delete' query",
			query:       "delete from users where user_id = 1;",
			expectedMsg: QueryTypeExec.String(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := IdentifyQuery(tc.query)
			assert.Equal(t, tc.query, actual.RawSQL)
			assert.Equal(t, tc.expectedMsg, actual.QueryType)
		})
	}
}
