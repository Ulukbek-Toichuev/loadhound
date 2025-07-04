/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package pkg

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/common-nighthawk/go-figure"
	"github.com/google/uuid"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

const Version string = "v0.0.1"

func RandBool() bool {
	return rand.IntN(2) == 1
}

func RandIntRange(min, max int) int {
	if min >= max {
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
	if min > max || min < 0 {
		return ""
	}
	n := rand.IntN(max-min+1) + min
	b := make([]byte, n)
	for i := 0; i < len(b); i++ {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return fmt.Sprintf("'%s'", string(b))
}

func GetTime() string {
	return fmt.Sprintf("'%s'", time.Now().Format("2006-01-02 15:04:05.999999"))
}

func PrintBanner() {
	myFigure := figure.NewColorFigure("LoadHound", "", "red", true)
	myFigure.Print()

	fmt.Printf("\nLoadHound — Relentless SQL load testing tool %s.\nCopyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com\n\n", Version)
}
