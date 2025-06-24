/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ulukbek-Toichuev/loadhound/cmd"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"
)

func init() {
	pkg.PrintAsciiArtLogo()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	cmd.Execute(ctx)
}
