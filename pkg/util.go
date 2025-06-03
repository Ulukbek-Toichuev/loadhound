package pkg

import (
	"fmt"
	"math/rand/v2"
	"os"
)

func randRange(min, max int) int {
	return rand.IntN(max-min) + min
}

func PrintFatal(msg string, err error) {
	fmt.Printf("%s: %v", msg, err)
	os.Exit(1)
}
