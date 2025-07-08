/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/go-playground/validator/v10"
	"github.com/schollz/progressbar/v3"

	"github.com/Ulukbek-Toichuev/loadhound/internal"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"
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
	var runTestFlag string
	var versionFlag bool

	flag.StringVar(&runTestFlag, "run-test", "", "Path to your *.yaml file for running simple test")
	flag.BoolVar(&versionFlag, "version", false, "Get LoadHound current version")
	flag.Usage = Usage
	flag.Parse()

	if len(os.Args) == 1 {
		flag.Usage()
		return nil
	}

	if versionFlag {
		fmt.Printf("%s\n", pkg.Version)
		return nil
	}

	pkg.PrintBanner()

	// Read config from file
	var runTestConfig internal.RunTestConfig
	if err := readConfigFile(runTestFlag, &runTestConfig); err != nil {
		return err
	}

	// Validate config
	if err := validateConfig(&runTestConfig); err != nil {
		return err
	}

	// Init progress bar
	barCfg := internal.ProgressBarConfig{
		MaxValue:  getProgressBarMaxValue(runTestConfig.WorkflowConfig),
		EnableBar: true,
	}
	bar := internal.NewProgressBar(barCfg)
	if bar == nil {
		return fmt.Errorf("failed to init progress bar")
	}

	// Configure general event controller
	g, err := getGeneralEventController(bar, runTestConfig.OutputConfig)
	if err != nil {
		return err
	}

	if err := testHandler(globalCtx, &runTestConfig, g); err != nil {
		return err
	}
	return nil
}

func testHandler(globalCtx context.Context, cfg *internal.RunTestConfig, g *internal.GeneralEventController) error {
	defer g.Close()

	var useStmt bool
	if cfg.DbConfig.SQLConfig != nil {
		useStmt = cfg.DbConfig.SQLConfig.UseStmt
	}
	pQuery, err := prepareQuery(cfg.WorkflowConfig.QueryTemplateConfig, useStmt)
	if err != nil {
		return err
	}

	executor, err := internal.NewExecutor(globalCtx, cfg, pQuery, g)
	if err != nil {
		return err
	}

	// Call start()
	start(globalCtx, executor, g, cfg)
	return nil
}

func prepareQuery(q *internal.QueryTemplateConfig, useStmt bool) (*internal.PreparedQuery, error) {
	pQuery := internal.IdentifyQuery(q.Template)
	if q.Name == "" {
		q.Name = q.Template
	}
	tmpl, err := internal.GetQueryTemplate(q, useStmt)
	if err != nil {
		return nil, err
	}
	pQuery.Tmpl = tmpl
	return pQuery, nil
}

func start(globalCtx context.Context, exec *internal.Executor, g *internal.GeneralEventController, cfg *internal.RunTestConfig) {
	g.WriteWelcomeMsg(cfg.WorkflowConfig)

	// Run test
	exec.Run(globalCtx)
	g.WriteInfoMsgWithBar("end test")

	if err := internal.GenerateReport(cfg, nil); err != nil {
		g.WriteErrMsgWithBar("failed to generate report", err)
	}
}

func getGeneralEventController(bar *progressbar.ProgressBar, cfg *internal.OutputConfig) (*internal.GeneralEventController, error) {
	if cfg != nil {
		if cfg.LogConfig != nil {
			logCfg := cfg.LogConfig
			g, err := internal.NewGeneralEventController(bar, logCfg.ToConsole, logCfg.ToFile)
			if err != nil {
				return nil, fmt.Errorf("failed to init general event controller")
			}
			return g, nil
		}
	}
	g, err := internal.NewGeneralEventController(bar, false, false)
	if err != nil {
		return nil, fmt.Errorf("failed to init general event controller")
	}
	return g, nil
}

func validateConfig(cfg *internal.RunTestConfig) error {
	validate := validator.New(validator.WithRequiredStructEnabled())
	if err := validate.Struct(cfg); err != nil {
		return err
	}

	iterations := cfg.WorkflowConfig.Iterations
	duration := cfg.WorkflowConfig.Duration

	if d := internal.GetDriverType(cfg.DbConfig.Driver); d == internal.Unknown {
		return errors.New("unknown driver")
	}

	if iterations == 0 && duration == 0 {
		return errors.New("either iterations or duration must be set")
	}

	if iterations > 0 && duration > 0 {
		return errors.New("iterations and duration are mutually exclusive")
	}

	if duration > 0 && cfg.WorkflowConfig.Pacing > duration {
		return errors.New("pacing cannot be more than test duration")
	}
	return nil
}

func readConfigFile(path string, out interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := toml.Unmarshal(data, out); err != nil {
		return err
	}
	return nil
}

func getProgressBarMaxValue(cfg *internal.WorkflowConfig) int {
	if cfg.Iterations > 0 {
		return cfg.Iterations * cfg.Threads
	} else if cfg.Pacing > 0 && cfg.Duration > 0 {
		return int(cfg.Duration/cfg.Pacing) * cfg.Threads
	}
	return -1
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
