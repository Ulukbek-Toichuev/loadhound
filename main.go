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
	"strings"
	"syscall"
	"text/template"
	"time"

	"github.com/Ulukbek-Toichuev/loadhound/internal"
	"github.com/Ulukbek-Toichuev/loadhound/internal/executor"
	"github.com/Ulukbek-Toichuev/loadhound/internal/parse"
	"github.com/Ulukbek-Toichuev/loadhound/internal/stat"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"

	"github.com/go-playground/validator/v10"
	"github.com/schollz/progressbar/v3"
	"gopkg.in/yaml.v3"
)

var (
	validate     *validator.Validate
	generalEvent *internal.GeneralEventController
)

func init() {
	validate = validator.New(validator.WithRequiredStructEnabled())
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	if err := Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func Run(ctx context.Context) error {
	runTest := internal.NewRunTestFlag()
	version := internal.NewVersionFlag()

	flag.StringVar(&runTest.Value, runTest.Name, runTest.DefaultValue, runTest.Description)
	flag.BoolVar(&version.Value, version.Name, version.DefaultValue, version.Description)
	flag.Usage = Usage
	flag.Parse()

	if len(os.Args) == 1 {
		flag.Usage()
		return nil
	}

	if version.Value {
		fmt.Printf("%s\n", pkg.PrintVersion())
		return nil
	}
	pkg.PrintAsciiArtLogo()
	if runTest.Value == "" {
		return fmt.Errorf("path to config file is empty")
	}

	// Read config file
	var runTestConfig internal.RunTestConfig
	if err := readConfigFile(runTest.Value, &runTestConfig); err != nil {
		return err
	}

	// Init progress bar
	barCfg := internal.ProgressBarConfig{
		MaxValue:  getProgressBarMaxValue(&runTestConfig.TestConfig),
		EnableBar: true,
	}
	bar := internal.NewProgressBar(barCfg)
	if bar == nil {
		return fmt.Errorf("failed to init progress bar")
	}

	// Configure logger
	g, err := getGeneralEventController(bar, runTestConfig.OutputConfig.LogConfig)
	if err != nil {
		return err
	}
	generalEvent = g

	if runTestConfig.TestConfig.Type == "simple" {
		if err := validateSimpleTest(&runTestConfig); err != nil {
			return err
		}
		if err := simpleTestHandler(ctx, &runTestConfig); err != nil {
			return err
		}
	}
	return nil
}

func simpleTestHandler(ctx context.Context, runCfg *internal.RunTestConfig) error {
	pQuery, tmpl, err := prepareQuery(runCfg.QueryTemplateConfig)
	if err != nil {
		return err
	}

	executor, err := executor.BuildSimpleExecutor(ctx, runCfg, tmpl, pQuery.QueryType, generalEvent)
	if err != nil {
		return err
	}
	return Start(ctx, executor)
}

func prepareQuery(qc internal.QueryTemplateConfig) (*parse.PreparedQuery, *template.Template, error) {
	var rawSQL string
	var pQuery *parse.PreparedQuery
	var err error

	switch {
	case qc.PathToQuery != "":
		pQuery, err = getSQLFromFile(qc.PathToQuery)
		if err != nil {
			return nil, nil, fmt.Errorf("loading SQL file: %w", err)
		}
		rawSQL = pQuery.RawSQL
	case qc.InlineQuery != "":
		pQuery = parse.IdentifyQuery(qc.InlineQuery)
		rawSQL = pQuery.RawSQL
	default:
		return nil, nil, errors.New("either inline_query or path_to_query must be provided")
	}

	tmpl, err := parse.GetQueryTemplate(rawSQL)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing SQL template: %w", err)
	}
	return pQuery, tmpl, nil
}

type Executor interface {
	Run(ctx context.Context) *stat.Stat
}

func Start(ctx context.Context, exec Executor) error {
	generalEvent.WriteInfoMsgWithBar("start test")
	defer generalEvent.WriteInfoMsgWithBar("end test")

	start := time.Now()
	result := exec.Run(ctx)
	end := time.Now()
	if result == nil {
		return errors.New("unexpected situation, stat pointer is null")
	}

	stat.PrintResultPretty(stat.GetReport(start, end, result))
	return nil
}

func getGeneralEventController(bar *progressbar.ProgressBar, cfg internal.LogConfig) (*internal.GeneralEventController, error) {
	filename := fmt.Sprintf("loadhound_%s.log", time.Now().Format(time.RFC3339))
	g, err := internal.NewGeneralEventController(bar, cfg.ConsoleWriter, cfg.FileWriter, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to init general event controller")
	}
	return g, nil
}

func validateSimpleTest(runCfg *internal.RunTestConfig) error {
	if err := validate.Struct(runCfg); err != nil {
		return err
	}

	pathToQuery := runCfg.QueryTemplateConfig.PathToQuery
	inlineQuery := runCfg.QueryTemplateConfig.InlineQuery
	iterations := runCfg.TestConfig.Iterations
	duration := runCfg.TestConfig.Duration

	if d := pkg.GetDriverType(runCfg.DbConfig.Driver); d == pkg.Unknown {
		return errors.New("unknown driver")
	}

	if iterations == 0 && duration == 0 {
		return errors.New("either iterations or duration must be set")
	}

	if iterations > 0 && duration > 0 {
		return errors.New("iterations and duration are mutually exclusive")
	}

	if pathToQuery == "" && inlineQuery == "" {
		return errors.New("either query or sql-file must be set")
	}

	if pathToQuery != "" && inlineQuery != "" {
		return errors.New("query and sql-file are mutually exclusive")
	}
	return nil
}

func getSQLFromFile(pathToFile string) (*parse.PreparedQuery, error) {
	if !strings.HasSuffix(pathToFile, ".sql") {
		return nil, errors.New("file is not a .sql file")
	}
	data, err := os.ReadFile(pathToFile)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("file from path is empty: %s", pathToFile)
	}
	pQuery, err := parse.GetPreparedQuery(string(data))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", "failed to get prepared query", err)
	}
	return pQuery, nil
}

func readConfigFile(path string, out interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		return err
	}
	return nil
}

func getProgressBarMaxValue(cfg *internal.TestConfig) int {
	switch {
	case cfg.Iterations > 0:
		return cfg.Iterations
	case cfg.Pacing > 0 && cfg.Duration > 0:
		return int(cfg.Duration/cfg.Pacing) * cfg.Workers
	default:
		return -1
	}
}

func Usage() {
	usage := `Usage of LoadHound:

  -simple-test string
      Path to your *.yaml file for running simple test

  -extend-test string
      Path to your *.yaml file for running extend test

  -version
      Get LoadHound version
`
	fmt.Println(usage)
}
