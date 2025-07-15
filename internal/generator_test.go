package internal

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetGenerators(t *testing.T) {
	tests := []struct {
		name        string
		args        string
		expectError bool
		errorMsg    string
		expectCount int
	}{
		{
			name:        "empty args",
			args:        "",
			expectError: true,
			errorMsg:    "args is empty",
		},
		{
			name:        "single randBool",
			args:        "randBool",
			expectError: false,
			expectCount: 1,
		},
		{
			name:        "single randUUID",
			args:        "randUUID",
			expectError: false,
			expectCount: 1,
		},
		{
			name:        "single getTimestampNow",
			args:        "getTimestampNow",
			expectError: false,
			expectCount: 1,
		},
		{
			name:        "randIntRange valid",
			args:        "randIntRange 1 10",
			expectError: false,
			expectCount: 1,
		},
		{
			name:        "randFloat64InRange valid",
			args:        "randFloat64InRange 1.5 10.5",
			expectError: false,
			expectCount: 1,
		},
		{
			name:        "randStringInRange valid",
			args:        "randStringInRange 5 15",
			expectError: false,
			expectCount: 1,
		},
		{
			name:        "randStrRange alias",
			args:        "randStrRange 5 15",
			expectError: false,
			expectCount: 1,
		},
		{
			name:        "multiple generators",
			args:        "randBool, randUUID, randIntRange 1 10",
			expectError: false,
			expectCount: 3,
		},
		{
			name:        "multiple generators with extra spaces",
			args:        "  randBool  ,  randUUID  ,  randIntRange 1 10  ",
			expectError: false,
			expectCount: 3,
		},
		{
			name:        "empty function name",
			args:        " ",
			expectError: true,
			errorMsg:    "no valid generators found",
		},
		{
			name:        "unknown function",
			args:        "unknownFunc",
			expectError: true,
			errorMsg:    "unknown function: unknownFunc",
		},
		{
			name:        "randBool with args",
			args:        "randBool 123",
			expectError: true,
			errorMsg:    "randBool() does not support args",
		},
		{
			name:        "randUUID with args",
			args:        "randUUID 123",
			expectError: true,
			errorMsg:    "randUUID() does not support args",
		},
		{
			name:        "getTimestampNow with args",
			args:        "getTimestampNow 123",
			expectError: true,
			errorMsg:    "getTimestampNow() does not support args",
		},
		{
			name:        "randIntRange too few args",
			args:        "randIntRange 1",
			expectError: true,
			errorMsg:    "randIntRange() requires exactly 2 arguments, got 1",
		},
		{
			name:        "randIntRange too many args",
			args:        "randIntRange 1 2 3",
			expectError: true,
			errorMsg:    "randIntRange() requires exactly 2 arguments, got 3",
		},
		{
			name:        "randIntRange invalid integer",
			args:        "randIntRange abc 10",
			expectError: true,
			errorMsg:    "randIntRange() argument parsing error",
		},
		{
			name:        "randIntRange min >= max",
			args:        "randIntRange 10 5",
			expectError: true,
			errorMsg:    "min value 10 must be less than max value 5",
		},
		{
			name:        "randIntRange negative min",
			args:        "randIntRange -5 10",
			expectError: true,
			errorMsg:    "min value -5 cannot be negative",
		},
		{
			name:        "randFloat64InRange too few args",
			args:        "randFloat64InRange 1.5",
			expectError: true,
			errorMsg:    "randFloat64InRange() requires exactly 2 arguments, got 1",
		},
		{
			name:        "randFloat64InRange invalid float",
			args:        "randFloat64InRange abc 10.5",
			expectError: true,
			errorMsg:    "randFloat64InRange() argument parsing error",
		},
		{
			name:        "randFloat64InRange min >= max",
			args:        "randFloat64InRange 10.5 5.5",
			expectError: true,
			errorMsg:    "min value 10.50 must be less than max value 5.50",
		},
		{
			name:        "randStringInRange too few args",
			args:        "randStringInRange 5",
			expectError: true,
			errorMsg:    "randStringInRange() requires exactly 2 arguments, got 1",
		},
		{
			name:        "randStringInRange invalid integer",
			args:        "randStringInRange abc 10",
			expectError: true,
			errorMsg:    "randStringInRange() argument parsing error",
		},
		{
			name:        "randStringInRange min >= max",
			args:        "randStringInRange 10 5",
			expectError: true,
			errorMsg:    "min value 10 must be less than max value 5",
		},
		{
			name:        "mixed valid and invalid",
			args:        "randBool, unknownFunc",
			expectError: true,
			errorMsg:    "unknown function: unknownFunc",
		},
		{
			name:        "empty args between commas",
			args:        "randBool,, randUUID",
			expectError: false,
			expectCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generators, err := GetGenerators(tt.args)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				assert.Nil(t, generators)
			} else {
				require.NoError(t, err)
				assert.Len(t, generators, tt.expectCount)
				// Test that generators actually work
				for _, gen := range generators {
					result := gen()
					assert.NotNil(t, result)
				}
			}
		})
	}
}

func TestRandBool(t *testing.T) {
	// Test that it returns both true and false over many iterations
	trueCount := 0
	falseCount := 0
	iterations := 1000

	for i := 0; i < iterations; i++ {
		result := RandBool()
		if result {
			trueCount++
		} else {
			falseCount++
		}
	}

	// Both should be non-zero (very unlikely to be all true or all false)
	assert.Greater(t, trueCount, 0)
	assert.Greater(t, falseCount, 0)
	assert.Equal(t, iterations, trueCount+falseCount)
}

func TestRandIntRange(t *testing.T) {
	tests := []struct {
		name string
		min  int
		max  int
	}{
		{"small range", 1, 10},
		{"large range", 100, 1000},
		{"single value range", 5, 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				result := RandIntRange(tt.min, tt.max)
				assert.GreaterOrEqual(t, result, tt.min)
				assert.Less(t, result, tt.max)
			}
		})
	}

	t.Run("invalid range min >= max", func(t *testing.T) {
		result := RandIntRange(10, 5)
		assert.Equal(t, 10, result) // Should return min as fallback

		result = RandIntRange(5, 5)
		assert.Equal(t, 5, result) // Should return min as fallback
	})
}

func TestRandFloat64InRange(t *testing.T) {
	tests := []struct {
		name string
		min  float64
		max  float64
	}{
		{"small range", 1.0, 10.0},
		{"decimal range", 1.5, 10.5},
		{"negative range", -10.0, -1.0},
		{"cross zero", -5.0, 5.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				result := RandFloat64InRange(tt.min, tt.max)
				assert.GreaterOrEqual(t, result, tt.min)
				assert.Less(t, result, tt.max)
			}
		})
	}

	t.Run("invalid range min >= max", func(t *testing.T) {
		result := RandFloat64InRange(10.0, 5.0)
		assert.Equal(t, 10.0, result) // Should return min as fallback

		result = RandFloat64InRange(5.0, 5.0)
		assert.Equal(t, 5.0, result) // Should return min as fallback
	})
}

func TestRandUUID(t *testing.T) {
	// Test that it generates valid UUID format
	result := RandUUID()
	assert.NotEmpty(t, result)

	// UUID should be 36 characters with hyphens at positions 8, 13, 18, 23
	assert.Len(t, result, 36)
	assert.Equal(t, "-", string(result[8]))
	assert.Equal(t, "-", string(result[13]))
	assert.Equal(t, "-", string(result[18]))
	assert.Equal(t, "-", string(result[23]))

	// Test that multiple calls generate different UUIDs
	uuid1 := RandUUID()
	uuid2 := RandUUID()
	assert.NotEqual(t, uuid1, uuid2)
}

func TestRandStringInRange(t *testing.T) {
	tests := []struct {
		name string
		min  int
		max  int
	}{
		{"small range", 1, 10},
		{"medium range", 5, 15},
		{"large range", 10, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				result := RandStringInRange(tt.min, tt.max)
				assert.GreaterOrEqual(t, len(result), tt.min)
				assert.LessOrEqual(t, len(result), tt.max)

				// Check that all characters are from the allowed set
				for _, char := range result {
					assert.Contains(t, letters, string(char))
				}
			}
		})
	}

	t.Run("zero length range", func(t *testing.T) {
		result := RandStringInRange(0, 1)
		assert.GreaterOrEqual(t, len(result), 0)
		assert.LessOrEqual(t, len(result), 1)
	})

	t.Run("invalid range min >= max", func(t *testing.T) {
		result := RandStringInRange(10, 5)
		assert.Equal(t, "", result) // Should return empty string as fallback

		result = RandStringInRange(5, 5)
		assert.Equal(t, "", result) // Should return empty string as fallback
	})

	t.Run("negative min", func(t *testing.T) {
		result := RandStringInRange(-5, 10)
		assert.GreaterOrEqual(t, len(result), 0) // Should clamp min to 0
		assert.LessOrEqual(t, len(result), 10)
	})
}

func TestGetTimestampNow(t *testing.T) {
	result := GetTimestampNow()
	assert.NotEmpty(t, result)

	// Should be in format "2006-01-02 15:04:05.999999"
	assert.Len(t, result, 26)
	assert.Contains(t, result, " ")
	assert.Contains(t, result, ":")
	assert.Contains(t, result, ".")

	// Test that it can be parsed back
	_, err := time.Parse("2006-01-02 15:04:05.999999", result)
	assert.NoError(t, err)

	// Test that consecutive calls produce different timestamps
	time.Sleep(1 * time.Millisecond)
	result2 := GetTimestampNow()
	assert.NotEqual(t, result, result2)
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expected    []int
		expectError bool
	}{
		{
			name:     "single valid integer",
			args:     []string{"123"},
			expected: []int{123},
		},
		{
			name:     "multiple valid integers",
			args:     []string{"1", "2", "3"},
			expected: []int{1, 2, 3},
		},
		{
			name:     "negative integers",
			args:     []string{"-5", "10"},
			expected: []int{-5, 10},
		},
		{
			name:        "invalid integer",
			args:        []string{"abc"},
			expectError: true,
		},
		{
			name:        "mixed valid and invalid",
			args:        []string{"1", "abc"},
			expectError: true,
		},
		{
			name:     "empty args",
			args:     []string{},
			expected: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseInt(tt.args...)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseFloat64(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expected    []float64
		expectError bool
	}{
		{
			name:     "single valid float",
			args:     []string{"123.45"},
			expected: []float64{123.45},
		},
		{
			name:     "multiple valid floats",
			args:     []string{"1.5", "2.7", "3.14"},
			expected: []float64{1.5, 2.7, 3.14},
		},
		{
			name:     "negative floats",
			args:     []string{"-5.5", "10.0"},
			expected: []float64{-5.5, 10.0},
		},
		{
			name:     "integers as floats",
			args:     []string{"1", "2"},
			expected: []float64{1.0, 2.0},
		},
		{
			name:        "invalid float",
			args:        []string{"abc"},
			expectError: true,
		},
		{
			name:        "mixed valid and invalid",
			args:        []string{"1.5", "abc"},
			expectError: true,
		},
		{
			name:     "empty args",
			args:     []string{},
			expected: []float64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseFloat64(tt.args...)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestValidateIntArgs(t *testing.T) {
	tests := []struct {
		name        string
		arg1        int
		arg2        int
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid range",
			arg1: 1,
			arg2: 10,
		},
		{
			name:        "min >= max",
			arg1:        10,
			arg2:        5,
			expectError: true,
			errorMsg:    "min value 10 must be less than max value 5",
		},
		{
			name:        "min == max",
			arg1:        5,
			arg2:        5,
			expectError: true,
			errorMsg:    "min value 5 must be less than max value 5",
		},
		{
			name:        "negative min",
			arg1:        -5,
			arg2:        10,
			expectError: true,
			errorMsg:    "min value -5 cannot be negative",
		},
		{
			name: "zero min",
			arg1: 0,
			arg2: 10,
		},
		{
			name: "large max",
			arg1: 1,
			arg2: math.MaxInt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIntArgs(tt.arg1, tt.arg2)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateFloat64Args(t *testing.T) {
	tests := []struct {
		name        string
		arg1        float64
		arg2        float64
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid range",
			arg1: 1.5,
			arg2: 10.5,
		},
		{
			name:        "min >= max",
			arg1:        10.5,
			arg2:        5.5,
			expectError: true,
			errorMsg:    "min value 10.50 must be less than max value 5.50",
		},
		{
			name:        "min == max",
			arg1:        5.5,
			arg2:        5.5,
			expectError: true,
			errorMsg:    "min value 5.50 must be less than max value 5.50",
		},
		{
			name: "negative values",
			arg1: -10.5,
			arg2: -5.5,
		},
		{
			name: "cross zero",
			arg1: -5.5,
			arg2: 5.5,
		},
		{
			name:        "positive infinity min",
			arg1:        math.Inf(1),
			arg2:        10.5,
			expectError: true,
			errorMsg:    "min value +Inf is not a valid number",
		},
		{
			name:        "negative infinity min",
			arg1:        math.Inf(-1),
			arg2:        10.5,
			expectError: true,
			errorMsg:    "min value -Inf is not a valid number",
		},
		{
			name:        "positive infinity max",
			arg1:        1.5,
			arg2:        math.Inf(1),
			expectError: true,
			errorMsg:    "max value +Inf is not a valid number",
		},
		{
			name:        "NaN min",
			arg1:        math.NaN(),
			arg2:        10.5,
			expectError: true,
			errorMsg:    "min value NaN is not a valid number",
		},
		{
			name:        "NaN max",
			arg1:        1.5,
			arg2:        math.NaN(),
			expectError: true,
			errorMsg:    "max value NaN is not a valid number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFloat64Args(tt.arg1, tt.arg2)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGeneratorFuncIntegration(t *testing.T) {
	// Test that generators created by GetGenerators work correctly
	t.Run("integration test", func(t *testing.T) {
		generators, err := GetGenerators("randBool, randUUID, randIntRange 1 10, randFloat64InRange 1.5 10.5, randStringInRange 5 15, getTimestampNow")
		require.NoError(t, err)
		require.Len(t, generators, 6)

		// Test randBool generator
		boolResult := generators[0]()
		assert.IsType(t, true, boolResult)

		// Test randUUID generator
		uuidResult := generators[1]()
		assert.IsType(t, "", uuidResult)
		assert.Len(t, uuidResult.(string), 36)

		// Test randIntRange generator
		intResult := generators[2]()
		assert.IsType(t, 0, intResult)
		assert.GreaterOrEqual(t, intResult.(int), 1)
		assert.Less(t, intResult.(int), 10)

		// Test randFloat64InRange generator
		floatResult := generators[3]()
		assert.IsType(t, 0.0, floatResult)
		assert.GreaterOrEqual(t, floatResult.(float64), 1.5)
		assert.Less(t, floatResult.(float64), 10.5)

		// Test randStringInRange generator
		stringResult := generators[4]()
		assert.IsType(t, "", stringResult)
		assert.GreaterOrEqual(t, len(stringResult.(string)), 5)
		assert.LessOrEqual(t, len(stringResult.(string)), 15)

		// Test getTimestampNow generator
		timestampResult := generators[5]()
		assert.IsType(t, "", timestampResult)
		assert.Len(t, timestampResult.(string), 26)
	})
}
