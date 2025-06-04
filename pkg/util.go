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

	"github.com/common-nighthawk/go-figure"
)

func randRange(min, max int) int {
	return rand.IntN(max-min) + min
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
