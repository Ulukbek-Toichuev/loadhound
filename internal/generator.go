/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type GeneratorFunc func() any

func GetGenerators(args string) ([]GeneratorFunc, error) {
	if len(args) == 0 {
		return nil, errors.New("args is empty")
	}
	argsSplit := strings.Split(args, ",")

	generators := make([]GeneratorFunc, 0, len(argsSplit))
	for _, arg := range argsSplit {
		arg = strings.TrimSpace(arg)
		if arg == "" {
			continue // Skip empty arguments
		}

		funcSignature := strings.Fields(arg)
		funcName := funcSignature[0]
		if funcName == "" {
			return nil, errors.New("func name is empty")
		}

		switch funcName {
		case "randBool":
			if len(funcSignature) > 1 {
				return nil, fmt.Errorf("invalid function signature, randBool() does not support args: %v", funcSignature)
			}
			generators = append(generators, func() any { return RandBool() })

		case "randIntRange":
			if len(funcSignature) != 3 {
				return nil, fmt.Errorf("randIntRange() requires exactly 2 arguments, got %d: %v", len(funcSignature)-1, funcSignature)
			}
			parsedArgs, err := parseInt(funcSignature[1:]...)
			if err != nil {
				return nil, fmt.Errorf("randIntRange() argument parsing error: %w", err)
			}
			arg1, arg2 := parsedArgs[0], parsedArgs[1]
			if err := validateIntArgs(arg1, arg2); err != nil {
				return nil, fmt.Errorf("randIntRange() validation error: %w", err)
			}
			generators = append(generators, func() any { return RandIntRange(arg1, arg2) })

		case "randFloat64InRange":
			if len(funcSignature) != 3 {
				return nil, fmt.Errorf("randFloat64InRange() requires exactly 2 arguments, got %d: %v", len(funcSignature)-1, funcSignature)
			}
			parsedArgs, err := parseFloat64(funcSignature[1:]...)
			if err != nil {
				return nil, fmt.Errorf("randFloat64InRange() argument parsing error: %w", err)
			}
			arg1, arg2 := parsedArgs[0], parsedArgs[1]
			if err := validateFloat64Args(arg1, arg2); err != nil {
				return nil, fmt.Errorf("randFloat64InRange() validation error: %w", err)
			}
			generators = append(generators, func() any {
				return RandFloat64InRange(arg1, arg2)
			})

		case "randUUID":
			if len(funcSignature) > 1 {
				return nil, fmt.Errorf("invalid function signature, randUUID() does not support args: %v", funcSignature)
			}
			generators = append(generators, func() any { return RandUUID() })

		case "randStringInRange", "randStrRange": // Support both variants
			if len(funcSignature) != 3 {
				return nil, fmt.Errorf("randStringInRange() requires exactly 2 arguments, got %d: %v", len(funcSignature)-1, funcSignature)
			}
			parsedArgs, err := parseInt(funcSignature[1:]...)
			if err != nil {
				return nil, fmt.Errorf("randStringInRange() argument parsing error: %w", err)
			}
			arg1, arg2 := parsedArgs[0], parsedArgs[1]
			if err := validateIntArgs(arg1, arg2); err != nil {
				return nil, fmt.Errorf("randStringInRange() validation error: %w", err)
			}
			generators = append(generators, func() any { return RandStringInRange(arg1, arg2) })

		case "getTimestampNow":
			if len(funcSignature) > 1 {
				return nil, fmt.Errorf("invalid function signature, getTimestampNow() does not support args: %v", funcSignature)
			}
			generators = append(generators, func() any { return GetTimestampNow() })

		default:
			return nil, fmt.Errorf("unknown function: %s", funcName)
		}
	}

	if len(generators) == 0 {
		return nil, errors.New("no valid generators found")
	}

	return generators, nil
}

func parseInt(args ...string) ([]int, error) {
	result := make([]int, len(args))
	for idx, arg := range args {
		p, err := strconv.Atoi(arg)
		if err != nil {
			return nil, fmt.Errorf("invalid integer argument '%s': %w", arg, err)
		}
		result[idx] = p
	}
	return result, nil
}

func parseFloat64(args ...string) ([]float64, error) {
	result := make([]float64, len(args))
	for idx, arg := range args {
		p, err := strconv.ParseFloat(arg, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float64 argument '%s': %w", arg, err)
		}
		result[idx] = p
	}
	return result, nil
}

func RandBool() bool {
	return rand.IntN(2) == 1
}

func RandIntRange(min, max int) int {
	if min >= max {
		return min // Fallback for invalid range
	}
	return rand.IntN(max-min) + min
}

func RandFloat64InRange(min, max float64) float64 {
	if min >= max {
		return min // Fallback for invalid range
	}
	return rand.Float64()*(max-min) + min
}

func RandUUID() string {
	u, err := uuid.NewRandom()
	if err != nil {
		// Return a fallback UUID instead of empty string
		return "00000000-0000-0000-0000-000000000000"
	}
	return u.String()
}

func RandStringInRange(min, max int) string {
	if min >= max {
		return "" // Fallback for invalid range
	}
	if min < 0 {
		min = 0
	}

	n := rand.IntN(max-min+1) + min
	if n == 0 {
		return ""
	}

	b := make([]byte, n)
	for i := 0; i < len(b); i++ {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

func GetTimestampNow() string {
	return time.Now().Format("2006-01-02 15:04:05.999999")
}

func validateIntArgs(arg1, arg2 int) error {
	if arg1 >= arg2 {
		return fmt.Errorf("min value %d must be less than max value %d", arg1, arg2)
	}
	if arg1 < 0 {
		return fmt.Errorf("min value %d cannot be negative", arg1)
	}
	if arg2 > math.MaxInt {
		return fmt.Errorf("max value %d exceeds maximum integer value", arg2)
	}
	return nil
}

func validateFloat64Args(arg1, arg2 float64) error {
	if math.IsInf(arg1, 0) || math.IsNaN(arg1) {
		return fmt.Errorf("min value %.2f is not a valid number", arg1)
	}
	if math.IsInf(arg2, 0) || math.IsNaN(arg2) {
		return fmt.Errorf("max value %.2f is not a valid number", arg2)
	}
	if arg1 >= arg2 {
		return fmt.Errorf("min value %.2f must be less than max value %.2f", arg1, arg2)
	}
	return nil
}
