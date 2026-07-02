package runner

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/rpc-tests/internal/compare"
	"github.com/erigontech/rpc-tests/internal/config"
	"github.com/erigontech/rpc-tests/internal/filter"
	internalrpc "github.com/erigontech/rpc-tests/internal/rpc"
	"github.com/erigontech/rpc-tests/internal/testdata"
)

const (
	// latestBlockMaxSkew is the largest head difference tolerated between the node under test and
	// the reference when resolving the block to pin: two independently-synced nodes are routinely
	// a block or two apart, and that is not a "not synced" condition.
	latestBlockMaxSkew uint64 = 4
	// latestRepinRetries bounds how many times a failed pinnable latest test is retried against a
	// freshly-resolved block, so a long run doesn't fail when the node under test prunes the state
	// of the originally-pinned block. A genuine discrepancy reproduces at the fresh block and fails.
	latestRepinRetries = 2
)

// Run executes the full test suite matching v1 runMain behavior.
func Run(ctx context.Context, cancelCtx context.CancelFunc, cfg *config.Config) (int, error) {
	startTime := time.Now()

	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return -1, err
	}

	// Print server endpoints
	if cfg.Parallel {
		fmt.Printf("Run tests in parallel on %s\n", cfg.ServerEndpoints())
	} else {
		fmt.Printf("Run tests in serial on %s\n", cfg.ServerEndpoints())
	}

	if strings.Contains(cfg.TransportType, "_comp") {
		fmt.Println("Run tests using compression")
	}

	// Handle latest block sync for verify mode
	if cfg.VerifyWithDaemon && cfg.TestsOnLatestBlock {
		server1 := fmt.Sprintf("%s:%d", cfg.DaemonOnHost, cfg.ServerPort)
		latestBlock, err := internalrpc.GetConsistentLatestBlock(
			cfg.VerboseLevel, server1, cfg.ExternalProviderURL, 30, 2*time.Second, latestBlockMaxSkew)
		if err != nil {
			fmt.Println("sync on latest block number failed ", err)
			return -1, err
		}
		if cfg.VerboseLevel > 0 {
			fmt.Printf("Latest block number for %s, %s: %d\n", server1, cfg.ExternalProviderURL, latestBlock)
		}
		// Pin this agreed block so latest-block tests query it explicitly (see pinLatestBlock),
		// making them deterministic instead of racing on each node's live head.
		cfg.PinnedLatestBlock = latestBlock
	}

	if err := cfg.CleanOutputDir(); err != nil {
		return -1, err
	}

	resultsAbsDir, err := cfg.ResultsAbsDir()
	if err != nil {
		return -1, err
	}
	fmt.Printf("Result directory: %s\n", resultsAbsDir)

	// Create filter
	f := filter.New(filter.FilterConfig{
		Net:                cfg.Net,
		ReqTestNum:         cfg.ReqTestNum,
		TestingAPIs:        cfg.TestingAPIs,
		TestingAPIsWith:    cfg.TestingAPIsWith,
		ExcludeAPIList:     cfg.ExcludeAPIList,
		ExcludeTestList:    cfg.ExcludeTestList,
		TestsOnLatestBlock: cfg.TestsOnLatestBlock,
		DoNotCompareError:  cfg.DoNotCompareError,
	})

	// Discover tests
	discovery, err := testdata.DiscoverTests(cfg.JSONDir, cfg.ResultsDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading directory %s: %v\n", cfg.JSONDir, err)
		return -1, err
	}

	numWorkers := 1
	if cfg.Parallel {
		numWorkers = runtime.NumCPU()
	}

	// Pre-create one RPC client per transport type (Client is goroutine-safe)
	clients := make(map[string]*internalrpc.Client)
	for _, tt := range cfg.TransportTypes() {
		clients[tt] = internalrpc.NewClient(tt, "", cfg.VerboseLevel)
	}

	availableTestedAPIs := discovery.TotalAPIs
	globalTestNumber := 0
	stats := &Stats{}

	var reportEntries []reportEntry
	var reportMu sync.Mutex

	// Each loop iteration runs as a complete batch: all tests are scheduled,
	// workers drain the channel, results are collected, then the next iteration starts.
	for loopNum := range cfg.LoopNumber {
		if ctx.Err() != nil {
			break
		}

		if cfg.LoopNumber != 1 {
			fmt.Printf("\nTest iteration: %d\n", loopNum+1)
		}

		testsChan := make(chan *testdata.TestDescriptor, 2000)
		resultsChan := make(chan testdata.TestResult, 2000)

		var wg sync.WaitGroup
		for range numWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case test := <-testsChan:
						if test == nil {
							return
						}
						testOutcome := RunTest(ctx, test, cfg, clients[test.TransportType])
						resultsChan <- testdata.TestResult{Outcome: testOutcome, Test: test}
					case <-ctx.Done():
						return
					}
				}
			}()
		}

		var resultsWg sync.WaitGroup
		resultsWg.Add(1)
		go func() {
			defer resultsWg.Done()
			w := bufio.NewWriterSize(os.Stdout, 64*1024)
			defer w.Flush()
			pending := make(map[int]testdata.TestResult)
			nextIndex := 0
			for {
				select {
				case result, ok := <-resultsChan:
					if !ok {
						return
					}
					pending[result.Test.Index] = result
					// Flush all consecutive results starting from nextIndex
					for {
						r, exists := pending[nextIndex]
						if !exists {
							break
						}
						delete(pending, nextIndex)
						nextIndex++
						printResult(w, &r, stats, cfg, cancelCtx, &reportEntries, &reportMu)
						if cfg.ExitOnFail && stats.FailedTests > 0 {
							return
						}
						if maxFailuresReached(cfg, stats) {
							return
						}
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		// Schedule all tests for this iteration
		scheduledIndex := 0
		transportTypes := cfg.TransportTypes()
	transportLoop:
		for _, transportType := range transportTypes {
			testNumberInAnyLoop := 1
			globalTestNumber = 0

			for _, tc := range discovery.Tests {
				if ctx.Err() != nil {
					break transportLoop
				}

				globalTestNumber = tc.Number
				currAPI := tc.APIName
				jsonTestFullName := tc.Name
				testName := strings.TrimPrefix(jsonTestFullName, currAPI+"/")
				if idx := strings.LastIndex(jsonTestFullName, "/"); idx >= 0 {
					testName = jsonTestFullName[idx+1:]
				}

				if f.APIUnderTest(currAPI, jsonTestFullName) {
					if f.IsSkipped(currAPI, jsonTestFullName, testNumberInAnyLoop) ||
						(!cfg.ArchiveNode && !testdata.HasTag(filepath.Join(cfg.JSONDir, jsonTestFullName), testdata.TagFull)) ||
						(cfg.PrunedNode && !testdata.HasTag(filepath.Join(cfg.JSONDir, jsonTestFullName), testdata.TagPruned)) ||
						(cfg.VerifyWithDaemon && testdata.HasTag(filepath.Join(cfg.JSONDir, jsonTestFullName), testdata.TagNoReference)) {
						if IsStartTestReached(cfg, testNumberInAnyLoop) {
							if !cfg.DisplayOnlyFail && cfg.ReqTestNum == -1 {
								file := fmt.Sprintf("%-60s", jsonTestFullName)
								tt := fmt.Sprintf("%-15s", transportType)
								fmt.Printf("%04d. %s::%s   skipped\n", testNumberInAnyLoop, tt, file)
							}
							stats.SkippedTests++
							if cfg.VerboseLevel == 1 || cfg.ReportFile != "" {
								reportMu.Lock()
								reportEntries = append(reportEntries, reportEntry{
									TestNumber:    testNumberInAnyLoop,
									TransportType: transportType,
									TestName:      jsonTestFullName,
									Result:        "SKIPPED",
									ErrorMessage:  "",
								})
								reportMu.Unlock()
							}
						}
					} else {
						shouldRun := ShouldRunTest(cfg, testName, testNumberInAnyLoop)

						if shouldRun && IsStartTestReached(cfg, testNumberInAnyLoop) {
							testDesc := &testdata.TestDescriptor{
								Name:          jsonTestFullName,
								Number:        testNumberInAnyLoop,
								TransportType: transportType,
								Index:         scheduledIndex,
							}
							scheduledIndex++
							select {
							case <-ctx.Done():
								break transportLoop
							case testsChan <- testDesc:
							}
							stats.ScheduledTests++

							if cfg.WaitingTime > 0 {
								time.Sleep(time.Duration(cfg.WaitingTime) * time.Millisecond)
							}
						}
					}
				}

				testNumberInAnyLoop++
			}
		}

		// Wait for this iteration to fully complete before starting the next
		close(testsChan)
		wg.Wait()
		close(resultsChan)
		resultsWg.Wait()
	}

	if stats.ScheduledTests == 0 && cfg.TestingAPIsWith != "" {
		fmt.Printf("WARN: API filter %s selected no tests\n", cfg.TestingAPIsWith)
	}

	if cfg.ExitOnFail && stats.FailedTests > 0 {
		fmt.Println("WARN: test sequence interrupted by failure (ExitOnFail)")
	}

	// Clean empty subfolders
	if entries, err := os.ReadDir(cfg.OutputDir); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			subfolder := fmt.Sprintf("%s/%s", cfg.OutputDir, entry.Name())
			if subEntries, err := os.ReadDir(subfolder); err == nil && len(subEntries) == 0 {
				_ = os.Remove(subfolder)
			}
		}
	}

	// Clean temp dir
	_ = os.RemoveAll(config.TempDirName)

	// Print summary
	elapsed := time.Since(startTime)
	stats.PrintSummary(startTime, elapsed, cfg.LoopNumber, availableTestedAPIs, globalTestNumber)

	reportMu.Lock()
	entries := reportEntries
	reportMu.Unlock()

	// Generate JSON report when verbose == 1 (mirrors Python behaviour)
	if cfg.VerboseLevel == 1 {
		reportFile := filepath.Join(cfg.OutputDir, "test_report.json")
		if err := generateReport(reportFile, startTime, elapsed, stats, globalTestNumber, availableTestedAPIs, cfg.LoopNumber, entries); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to generate report: %v\n", err)
		} else {
			fmt.Printf("\nJSON report generated: %s\n", reportFile)
		}
	}

	// Generate CSV summary report when -R / --report-file is specified
	if cfg.ReportFile != "" {
		if err := generateCSVReport(cfg.ReportFile, entries); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to generate report: %v\n", err)
		} else {
			fmt.Printf("\nReport generated: %s\n", cfg.ReportFile)
		}
	}

	if maxFailuresReached(cfg, stats) {
		fmt.Printf("\nABORTED: too many failures (%d), test sequence stopped early\n", cfg.MaxFailures)
	}

	if stats.FailedTests > 0 {
		return 1, nil
	}
	return 0, nil
}

func maxFailuresReached(cfg *config.Config, stats *Stats) bool {
	return cfg.MaxFailures > 0 && stats.FailedTests >= cfg.MaxFailures
}

func printResult(w *bufio.Writer, result *testdata.TestResult, stats *Stats, cfg *config.Config, cancelCtx context.CancelFunc, reportEntries *[]reportEntry, reportMu *sync.Mutex) {
	file := fmt.Sprintf("%-60s", result.Test.Name)
	tt := fmt.Sprintf("%-15s", result.Test.TransportType)
	fmt.Fprintf(w, "%04d. %s::%s   ", result.Test.Number, tt, file)

	kf, isKnownFailure := cfg.KnownFailures[config.NormalizeTestKey(result.Test.Name)]
	isFlaky := filter.IsFlakyLatest(cfg.Net, result.Test.Name)

	addReport := func(res string, errField any) {
		if cfg.VerboseLevel == 1 || cfg.ReportFile != "" {
			reportMu.Lock()
			*reportEntries = append(*reportEntries, reportEntry{
				TestNumber:    result.Test.Number,
				TransportType: result.Test.TransportType,
				TestName:      result.Test.Name,
				Result:        res,
				ErrorMessage:  errField,
			})
			reportMu.Unlock()
		}
	}

	switch {
	case result.Outcome.Success && isKnownFailure:
		// A known failure that now passes: flag it so the entry gets removed.
		// In strict mode it fails the run to force the cleanup.
		stats.AddUnexpectedPass()
		msg := fmt.Sprintf("known failure %s appears resolved — remove it from known-failures.json", kf.PR)
		if cfg.StrictKnownFailures {
			stats.AddFailure()
			fmt.Fprintf(w, "failed: UNEXPECTED PASS (%s)\n", msg)
		} else {
			stats.AddSuccess(result.Outcome.Metrics)
			fmt.Fprintf(w, "OK (UNEXPECTED PASS: %s)\n", msg)
		}
		addReport("UNEXPECTED_PASS", kf.PR)

	case result.Outcome.Success:
		stats.AddSuccess(result.Outcome.Metrics)
		if cfg.VerboseLevel > 0 {
			fmt.Fprintln(w, "OK")
		} else {
			fmt.Fprint(w, "OK\r")
		}
		addReport("OK", "")

	case isKnownFailure:
		// Expected failure: report but do not fail the run.
		stats.AddKnownFailure()
		errMsg := "no error"
		if result.Outcome.Error != nil {
			errMsg = result.Outcome.Error.Error()
		}
		fmt.Fprintf(w, "KNOWN FAIL (%s)\n", kf.PR)
		addReport("KNOWN_FAIL", kf.PR+": "+errMsg)

	case isFlaky:
		// No-param latest method (e.g. eth_baseFee) that can't be pinned: a head-skew diff
		// is expected and does not fail the run. A pass is reported normally above.
		stats.AddFlakyFailure()
		errMsg := "no error"
		if result.Outcome.Error != nil {
			errMsg = result.Outcome.Error.Error()
		}
		fmt.Fprintln(w, "FLAKY (latest, not comparable head)")
		addReport("FLAKY", errMsg)

	default:
		stats.AddFailure()
		errMsg := "no error"
		if result.Outcome.Error != nil {
			errMsg = result.Outcome.Error.Error()
			fmt.Fprintf(w, "failed: %s\n", errMsg)
			if errors.Is(result.Outcome.Error, compare.ErrDiffMismatch) && result.Outcome.ColoredDiff != "" {
				fmt.Fprint(w, result.Outcome.ColoredDiff)
			}
		} else {
			fmt.Fprintf(w, "failed: %s\n", errMsg)
		}
		var errField any = errMsg
		if result.Outcome.ErrorDetails != nil {
			errField = result.Outcome.ErrorDetails
		}
		addReport("FAILED", errField)
		if maxFailuresReached(cfg, stats) {
			fmt.Fprintf(w, "\nABORTED: too many failures (%d), test sequence stopped early\n", cfg.MaxFailures)
			w.Flush()
			cancelCtx()
		} else if cfg.ExitOnFail {
			w.Flush()
			cancelCtx()
		}
	}
	w.Flush()
}
