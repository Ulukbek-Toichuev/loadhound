/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package pkg

import (
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/common-nighthawk/go-figure"
	"github.com/google/uuid"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

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

func RandUUID() *uuid.UUID {
	u, err := uuid.NewRandom()
	if err != nil {
		return &uuid.Nil
	}
	return &u
}

func RandStringInRange(min, max int) string {
	if min > max || min < 0 {
		return ""
	}
	n := rand.IntN(max-min+1) + min
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

func GetTime() string {
	return time.Now().Format("2006-01-02 15:04:05.999999")
}

func LogWrapper(msg string) {
	fmt.Printf("==> %s\n", msg)
}

func PrintFatal(msg string, err error) {
	fmt.Printf("%s: %v", msg, err)
	os.Exit(1)
}

func PrintAsciiArtLogo() {
	myFigure := figure.NewColorFigure("LoadHound", "", "red", true)
	myFigure.Print()

	fmt.Printf("\nLoadHound — Relentless SQL load testing tool.\nCopyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com\n\n")
}
