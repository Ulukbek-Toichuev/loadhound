/*
LoadHound — Relentless load testing tool for SQL databases.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ulukbek-Toichuev/loadhound/internal"

	"github.com/common-nighthawk/go-figure"
	"github.com/rs/zerolog"
)

const version string = "v0.2.0"

var (
	runFlag     = flag.String("run", "", "Path to *.toml file with test configuration")
	versionFlag = flag.Bool("version", false, "Get LoadHound current version")
	signals     = []os.Signal{os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
)

func main() {
	globalCtx, globalStop := signal.NotifyContext(context.Background(), signals...)
	defer globalStop()

	flag.Usage = usage
	flag.Parse()

	if len(os.Args) == 1 {
		flag.Usage()
		return
	}

	if *versionFlag {
		fmt.Printf("%s\n", version)
		return
	}

	// Get configuration from file
	cfg, err := internal.GetConfig(*runFlag)
	if err != nil {
		fatal(err)
	}

	// Get logger instance
	logger, err := internal.GetLogger(cfg.OutputConfig)
	if err != nil {
		fatal(err)
	}

	// Print welcome message
	printWelcome(logger, len(cfg.WorkflowConfig.Scenarios))

	// Get workflow instance
	workflow := internal.NewWorkflow(cfg, logger)

	// Run workflow
	if err := workflow.Run(globalCtx); err != nil {
		logger.Error().Err(err).Msg("Get error from workflow")
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func usage() {
	usage := `Usage of LoadHound:
  -run
      Path to your *.toml file for running test
  -version
      Get LoadHound version`
	fmt.Println(usage)
}

func printWelcome(logger *zerolog.Logger, sc int) {
	printBanner()
	logger.Info().Msg("LoadHound started")
	logger.Debug().Str("config_file", *runFlag).Int("scenarios_count", sc).Msg("Configuration loaded")
}

func printBanner() {
	myFigure := figure.NewColorFigure("LoadHound", "", "red", true)
	myFigure.Print()

	fmt.Printf("\nRelentless load testing tool for SQL databases.\nCopyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com.\n%s\n\n", version)
}
