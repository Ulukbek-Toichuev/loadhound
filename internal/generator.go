/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package internal

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandBool() bool {
	return rand.IntN(2) == 1
}

func RandIntRange(min, max int) int {
	if err := ValidateIntArgs(min, max); err != nil {
		return 0
	}
	return rand.IntN(max-min) + min
}

func RandFloat64InRange(min, max float64) float64 {
	if min >= max {
		return 0
	}
	return rand.Float64()*(max-min) + min
}

func RandUUID() string {
	u, err := uuid.NewRandom()
	if err != nil {
		return ""
	}
	return fmt.Sprintf("'%s'", u.String())
}

func RandStringInRange(min, max int) string {
	if err := ValidateIntArgs(min, max); err != nil {
		return ""
	}
	if min < 0 {
		return ""
	}
	n := rand.IntN(max-min+1) + min
	b := make([]byte, n)
	for i := 0; i < len(b); i++ {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return fmt.Sprintf("'%s'", string(b))
}

func ValidateIntArgs(arg1, arg2 int) error {
	if arg1 >= arg2 {
		return fmt.Errorf("arg1 must be less than arg2")
	}
	if arg1 < math.MinInt {
		return fmt.Errorf("arg %d less than go min int", arg1)
	}
	if arg2 > math.MaxInt {
		return fmt.Errorf("arg %d more than go max int", arg2)
	}

	return nil
}

func GetTime() string {
	return fmt.Sprintf("'%s'", time.Now().Format("2006-01-02 15:04:05.999999"))
}
