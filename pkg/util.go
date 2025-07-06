/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package pkg

import (
	"fmt"

	"github.com/common-nighthawk/go-figure"
)

const Version string = "v0.0.1"

func PrintBanner() {
	myFigure := figure.NewColorFigure("LoadHound", "", "red", true)
	myFigure.Print()

	fmt.Printf("\nLoadHound — Relentless SQL load testing tool %s.\nCopyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com\n\n", Version)
}
