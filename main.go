/*
LoadHound — Relentless load testing tool for SQL-oriented RDBMS.
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
	"github.com/common-nighthawk/go-figure"
)

const version string = "v0.0.1"

var (
	runTestFlag = flag.String("run-test", "", "Path to *.toml file with test configuration")
	versionFlag = flag.Bool("version", false, "Get LoadHound current version")
	signals     = []os.Signal{os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
)

func main() {
	globalCtx, globalStop := signal.NotifyContext(context.Background(), signals...)
	defer globalStop()

	if err := run(globalCtx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(globalCtx context.Context) error {
	flag.Usage = usage
	flag.Parse()

	if len(os.Args) == 1 {
		flag.Usage()
		return nil
	}

	if *versionFlag {
		fmt.Printf("%s\n", version)
		return nil
	}

	printBanner()

	// Read configuration from file
	var runTestConfig internal.RunTestConfig
	if err := internal.ReadConfigFile(*runTestFlag, &runTestConfig); err != nil {
		return err
	}
	// Validate configuration
	if err := internal.ValidateConfig(&runTestConfig); err != nil {
		return err
	}

	// Get logger instance
	logger, err := internal.GetLogger(runTestConfig.OutputConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	logger.Info().Msg("LoadHound started")
	logger.Debug().Str("config_file", *runTestFlag).Int("scenarios_count", len(runTestConfig.WorkflowConfig.Scenarios)).Msg("Configuration loaded")

	// Get SQL-client instance
	client, err := internal.NewSQLClient(globalCtx, runTestConfig.DbConfig)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create SQL client")
		return err
	}
	logger.Info().Str("driver", runTestConfig.DbConfig.Driver).Str("dsn", runTestConfig.DbConfig.Dsn).Msg("Database connection established")

	// Get workflow instance
	workflow := internal.NewWorkflow(runTestConfig.WorkflowConfig.Scenarios)
	logger.Info().Msg("Starting load test execution")

	// Run test
	globalMetric, err := workflow.RunTest(globalCtx, client, logger)

	if err != nil {
		logger.Error().Err(err).Str("total_duration", time.Since(globalMetric.GetStartTime()).String()).Msg("Load test failed")
		return err
	}
	logger.Info().Str("total_duration", time.Since(globalMetric.GetStartTime()).String()).Msg("Load test completed successfully")
	internal.GenerateReport(&runTestConfig, globalMetric)
	return nil
}

func usage() {
	usage := `Usage of LoadHound:
  -run-test string
      Path to your *.toml file for running test
  -version
      Get LoadHound version
`
	fmt.Println(usage)
}

func printBanner() {
	myFigure := figure.NewColorFigure("LoadHound", "", "red", true)
	myFigure.Print()

	fmt.Printf("\nLoadHound — Simple load testing cli tool for SQL-oriented RDBMS.\nCopyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com.\nVersion %s\n\n", version)
}
