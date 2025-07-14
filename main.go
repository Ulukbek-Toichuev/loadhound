/*
LoadHound — Relentless SQL load testing tool.
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
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"
)

var (
	runTest = flag.String("run-test", "", "Path to *.toml file with test configuration")
	version = flag.Bool("version", false, "Get LoadHound current version")
)

func main() {
	globalCtx, globalStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer globalStop()

	if err := Run(globalCtx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func Run(globalCtx context.Context) error {
	flag.Usage = Usage
	flag.Parse()

	if len(os.Args) == 1 {
		flag.Usage()
		return nil
	}

	if *version {
		fmt.Printf("%s\n", pkg.Version)
		return nil
	}

	pkg.PrintBanner()

	var runTestConfig internal.RunTestConfig
	if err := internal.ReadConfigFile(*runTest, &runTestConfig); err != nil {
		return err
	}

	logger, err := internal.GetLogger(runTestConfig.OutputConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	logger.Info().Msg("LoadHound started")
	logger.Debug().Str("config_file", *runTest).Int("scenarios_count", len(runTestConfig.WorkflowConfig.Scenarios)).Msg("Configuration loaded")

	client, err := internal.NewSQLClient(globalCtx, runTestConfig.DbConfig)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create SQL client")
		return err
	}

	logger.Info().Str("driver", runTestConfig.DbConfig.Driver).Str("dsn", runTestConfig.DbConfig.Dsn).Msg("Database connection established")

	workflow := internal.NewWorkflow(runTestConfig.WorkflowConfig.Scenarios)

	logger.Info().Msg("Starting load test execution")
	startTime := time.Now()

	err = workflow.RunTest(globalCtx, client, logger)

	duration := time.Since(startTime)
	if err != nil {
		logger.Error().Err(err).Str("total_duration", duration.String()).Msg("Load test failed")
		return err
	}

	logger.Info().Str("total_duration", duration.String()).Msg("Load test completed successfully")
	return nil
}

func Usage() {
	usage := `Usage of LoadHound:
  -run-test string
      Path to your *.toml file for running test
  -version
      Get LoadHound version
`
	fmt.Println(usage)
}
