/*
LoadHound — Relentless SQL load testing tool.
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

	generators := make([]GeneratorFunc, 0)
	for _, arg := range argsSplit {
		arg = strings.TrimSpace(arg)
		funcSignature := strings.Fields(arg)
		funcName := funcSignature[0]
		if funcName == "" {
			return nil, errors.New("func name is empty")
		}

		switch funcName {
		case "randBool":
			if len(funcSignature) > 1 {
				return nil, fmt.Errorf("invalid function signature, randBool() not support args: %v", funcSignature)
			}
			generators = append(generators, func() any { return RandBool() })
		case "randIntRange":
			if len(funcSignature) != 3 {
				return nil, fmt.Errorf("randIntRange() invalide signature: %v", funcSignature)
			}
			parsedArgs, err := parseInt(funcSignature[:1]...)
			if err != nil {
				return nil, err
			}
			arg1, arg2 := parsedArgs[0], parsedArgs[1]
			if err := validateIntArgs(arg1, arg2); err != nil {
				return nil, err
			}
			generators = append(generators, func() any { return RandIntRange(arg1, arg2) })
		case "randFloat64InRange":
			if len(funcSignature) != 3 {
				return nil, fmt.Errorf("randIntRange invalide signature: %v", funcSignature)
			}
			parsedArgs, err := parseFloat64(funcSignature[:1]...)
			if err != nil {
				return nil, err
			}
			arg1, arg2 := parsedArgs[0], parsedArgs[1]
			if err := validateFloat64Args(arg1, arg2); err != nil {
				return nil, err
			}
			generators = append(generators, func() any {
				return RandFloat64InRange(arg1, arg2)
			})
		case "randUUID":
			if len(funcSignature) > 1 {
				return nil, fmt.Errorf("invalid function signature, randUUID() not support args: %v", funcSignature)
			}
			generators = append(generators, func() any { return RandUUID() })
		case "randStringInRange":
			if len(funcSignature) != 3 {
				return nil, fmt.Errorf("randStringInRange invalide signature: %v", funcSignature)
			}
			parsedArgs, err := parseInt(funcSignature[:1]...)
			if err != nil {
				return nil, err
			}
			arg1, arg2 := parsedArgs[0], parsedArgs[1]
			if err := validateIntArgs(arg1, arg2); err != nil {
				return nil, err
			}
			generators = append(generators, func() any { return RandStringInRange(arg1, arg2) })
		case "getTimestampNow":
			if len(funcSignature) > 1 {
				return nil, fmt.Errorf("invalid function signature, getTimestampNow() not support args: %v", funcSignature)
			}
			generators = append(generators, func() any { return GetTimestampNow() })
		}
	}
	return generators, nil
}

func parseInt(args ...string) ([]int, error) {
	result := make([]int, 0, len(args))
	for idx, arg := range args {
		p, err := strconv.Atoi(arg)
		if err != nil {
			return nil, err
		}
		result[idx] = p
	}
	return result, nil
}

func parseFloat64(args ...string) ([]float64, error) {
	result := make([]float64, 0, len(args))
	for idx, arg := range args {
		p, err := strconv.ParseFloat(arg, 64)
		if err != nil {
			return nil, err
		}
		result[idx] = p
	}
	return result, nil
}

func RandBool() bool {
	return rand.IntN(2) == 1
}

func RandIntRange(min, max int) int {
	return rand.IntN(max-min) + min
}

func RandFloat64InRange(min, max float64) float64 {
	return rand.Float64()*(max-min) + min
}

func RandUUID() string {
	u, err := uuid.NewRandom()
	if err != nil {
		return ""
	}
	return u.String()
}

func RandStringInRange(min, max int) string {
	n := rand.IntN(max-min+1) + min
	b := make([]byte, n)
	for i := 0; i < len(b); i++ {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

func validateIntArgs(arg1, arg2 int) error {
	if arg1 >= arg2 {
		return fmt.Errorf("arg: %d must be less than arg: %d", arg1, arg2)
	}
	if arg1 < 0 {
		return fmt.Errorf("arg %d cannot be less than 0", arg1)
	}
	if arg2 > math.MaxInt {
		return fmt.Errorf("arg %d more than go max int", arg2)
	}
	return nil
}

func validateFloat64Args(arg1, arg2 float64) error {
	if arg1 >= arg2 {
		return fmt.Errorf("arg: %.2f must be less than arg: %.2f", arg1, arg2)
	}
	if arg1 < math.SmallestNonzeroFloat64 {
		return fmt.Errorf("arg %.2f cannot be less than smallest positive, non-zero float64", arg1)
	}
	if arg2 > math.MaxFloat64 {
		return fmt.Errorf("arg %.2f more than go max float64", arg2)
	}
	return nil
}

func GetTimestampNow() string {
	return fmt.Sprintf("'%s'", time.Now().Format("2006-01-02 15:04:05.999999"))
}
