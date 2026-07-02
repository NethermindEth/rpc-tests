package runner

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/erigontech/rpc-tests/internal/compare"
	"github.com/erigontech/rpc-tests/internal/config"
	"github.com/erigontech/rpc-tests/internal/filter"
	internalrpc "github.com/erigontech/rpc-tests/internal/rpc"
	"github.com/erigontech/rpc-tests/internal/testdata"
)

// RunTest executes a single test and returns the outcome.
// This is the v2 equivalent of v1's runTest + run methods.
// The client parameter is a pre-created RPC client shared across tests (goroutine-safe).
func RunTest(ctx context.Context, descriptor *testdata.TestDescriptor, cfg *config.Config, client *internalrpc.Client) testdata.TestOutcome {
	jsonFilename := filepath.Join(cfg.JSONDir, descriptor.Name)

	outcome := testdata.TestOutcome{}

	var commands []testdata.JsonRpcCommand
	var err error
	if testdata.IsArchive(jsonFilename) {
		commands, err = testdata.LoadFixture(jsonFilename, cfg.SanitizeArchiveExt, &outcome.Metrics)
	} else {
		commands, err = testdata.LoadFixture(jsonFilename, false, &outcome.Metrics)
	}
	if err != nil {
		outcome.Error = err
		return outcome
	}

	if len(commands) != 1 {
		outcome.Error = errors.New("expected exactly one JSON RPC command in " + jsonFilename)
		return outcome
	}

	runCommand(ctx, cfg, &commands[0], descriptor, &outcome, client)
	return outcome
}

// enrichErrorDetails fills in Target and Request on an existing or newly created ErrorDetails.
func enrichErrorDetails(outcome *testdata.TestOutcome, target string, request []byte) {
	if outcome.ErrorDetails == nil {
		if outcome.Error != nil {
			outcome.ErrorDetails = &testdata.ErrorDetails{Message: outcome.Error.Error()}
		} else {
			return
		}
	}
	if outcome.ErrorDetails.Target == "" {
		outcome.ErrorDetails.Target = target
	}
	if outcome.ErrorDetails.Request == nil && len(request) > 0 {
		var req any
		if err := json.Unmarshal(request, &req); err == nil {
			outcome.ErrorDetails.Request = req
		}
	}
}

// runCommand executes a single JSON-RPC command against the target.
func runCommand(ctx context.Context, cfg *config.Config, cmd *testdata.JsonRpcCommand, descriptor *testdata.TestDescriptor, outcome *testdata.TestOutcome, baseClient *internalrpc.Client) {
	transportType := descriptor.TransportType
	jsonFile := descriptor.Name
	request := cmd.Request

	target := cfg.GetTarget(cfg.DaemonUnderTest, descriptor.Name)

	// Use pre-created client; create per-test client only when JWT is needed (fresh iat per request)
	client := baseClient
	if cfg.JWTSecret != "" {
		secretBytes, _ := hex.DecodeString(cfg.JWTSecret)
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"iat": time.Now().Unix(),
		})
		tokenString, _ := token.SignedString(secretBytes)
		client = internalrpc.NewClient(transportType, "Bearer "+tokenString, cfg.VerboseLevel)
	}

	outputAPIFilename, outputDirName, diffFile, daemonFile, expRspFile := compare.OutputFilePaths(cfg.OutputDir, jsonFile)

	checkFields := testdata.CheckFields(filepath.Join(cfg.JSONDir, jsonFile))
	ignoreFields := testdata.IgnoreFields(filepath.Join(cfg.JSONDir, jsonFile))

	if !cfg.VerifyWithDaemon {
		var result any
		metrics, err := client.Call(ctx, target, request, &result)
		outcome.Metrics.RoundTripTime += metrics.RoundTripTime
		outcome.Metrics.UnmarshallingTime += metrics.UnmarshallingTime
		if err != nil {
			outcome.Error = err
			enrichErrorDetails(outcome, target, request)
			return
		}
		if cfg.VerboseLevel > 2 {
			fmt.Printf("%s: [%v]\n", cfg.DaemonUnderTest, result)
		}

		compare.ProcessResponse(result, nil, cmd.Response, cfg, outputDirName, daemonFile, expRspFile, diffFile, outcome, ignoreFields, checkFields...)
		if !outcome.Success {
			enrichErrorDetails(outcome, target, request)
		}
	} else {
		target = cfg.GetTarget(config.DaemonOnDefaultPort, descriptor.Name)
		target1 := cfg.GetTarget(cfg.DaemonAsReference, descriptor.Name)
		referenceClient := client
		if len(cfg.ExternalProviderHeaders) > 0 {
			referenceClient = internalrpc.NewClientWithHeaders(transportType, "", cfg.VerboseLevel, cfg.ExternalProviderHeaders)
		}

		// Pin latest-block tests to a concrete block so both nodes compare the same immutable
		// block instead of racing on their live heads. No-op for requests without a latest/pending tag.
		if cfg.TestsOnLatestBlock && cfg.PinnedLatestBlock > 0 {
			request = pinLatestBlock(request, cfg.PinnedLatestBlock)
		}

		var result, result1 any
		if cfg.TestsOnLatestBlock && filter.IsFlakyLatest(cfg.Net, jsonFile) {
			// No-param head methods (eth_baseFee, eth_gasPrice, ...) have no block arg to pin,
			// so use a best-effort head guard; a residual failure is classified FLAKY (non-fatal).
			metrics, stable, err := callBothAtStableHead(ctx, cfg, client, referenceClient, target, target1, request, &result, &result1)
			outcome.Metrics.RoundTripTime += metrics.RoundTripTime
			outcome.Metrics.UnmarshallingTime += metrics.UnmarshallingTime
			if err != nil {
				outcome.Error = err
				enrichErrorDetails(outcome, target, request)
				return
			}
			if !stable {
				outcome.Error = fmt.Errorf("could not obtain a consistent latest head across %d attempts (node under test lagging the reference?)", cfg.LatestRetries)
				enrichErrorDetails(outcome, target, request)
				return
			}
		} else {
			metrics, err := client.Call(ctx, target, request, &result)
			outcome.Metrics.RoundTripTime += metrics.RoundTripTime
			outcome.Metrics.UnmarshallingTime += metrics.UnmarshallingTime
			if err != nil {
				outcome.Error = err
				enrichErrorDetails(outcome, target, request)
				return
			}
			metrics1, err := referenceClient.Call(ctx, target1, request, &result1)
			outcome.Metrics.RoundTripTime += metrics1.RoundTripTime
			outcome.Metrics.UnmarshallingTime += metrics1.UnmarshallingTime
			if err != nil {
				outcome.Error = err
				enrichErrorDetails(outcome, target1, request)
				return
			}
		}
		if cfg.VerboseLevel > 2 {
			fmt.Printf("%s: [%v]\n%s: [%v]\n", cfg.DaemonUnderTest, result, cfg.DaemonAsReference, result1)
		}

		daemonFile = outputAPIFilename + config.GetJSONFilenameExt(config.DaemonOnDefaultPort, target)
		expRspFile = outputAPIFilename + config.GetJSONFilenameExt(cfg.DaemonAsReference, target1)

		compare.ProcessResponse(result, result1, nil, cfg, outputDirName, daemonFile, expRspFile, diffFile, outcome, ignoreFields, checkFields...)
		if !outcome.Success {
			enrichErrorDetails(outcome, target, request)
		}

		// A pinnable latest test can fail if the node under test pruned the pinned block's state
		// during a long run. Re-resolve a fresh block both nodes have and retry before accepting
		// the failure; a genuine discrepancy reproduces at the fresh block and still fails. Uses
		// cmd.Request (the un-pinned original) to re-pin. Skipped for flaky no-param methods.
		if !outcome.Success && cfg.TestsOnLatestBlock && cfg.PinnedLatestBlock > 0 && !filter.IsFlakyLatest(cfg.Net, jsonFile) {
			server1 := fmt.Sprintf("%s:%d", cfg.DaemonOnHost, cfg.ServerPort)
			pinBlock := cfg.PinnedLatestBlock
			for attempt := 0; !outcome.Success && attempt < latestRepinRetries; attempt++ {
				fresh, ferr := internalrpc.GetConsistentLatestBlock(cfg.VerboseLevel, server1, cfg.ExternalProviderURL, 10, 1*time.Second, latestBlockMaxSkew)
				if ferr != nil || fresh == pinBlock {
					break
				}
				pinBlock = fresh
				retryReq := pinLatestBlock(cmd.Request, fresh)
				*outcome = testdata.TestOutcome{}
				var retryResult, retryResult1 any
				if m, e := client.Call(ctx, target, retryReq, &retryResult); e != nil {
					outcome.Error = e
					enrichErrorDetails(outcome, target, retryReq)
					continue
				} else {
					outcome.Metrics.RoundTripTime += m.RoundTripTime
					outcome.Metrics.UnmarshallingTime += m.UnmarshallingTime
				}
				if m, e := referenceClient.Call(ctx, target1, retryReq, &retryResult1); e != nil {
					outcome.Error = e
					enrichErrorDetails(outcome, target1, retryReq)
					continue
				} else {
					outcome.Metrics.RoundTripTime += m.RoundTripTime
					outcome.Metrics.UnmarshallingTime += m.UnmarshallingTime
				}
				compare.ProcessResponse(retryResult, retryResult1, nil, cfg, outputDirName, daemonFile, expRspFile, diffFile, outcome, ignoreFields, checkFields...)
				if !outcome.Success {
					enrichErrorDetails(outcome, target, retryReq)
				}
			}
		}
	}
}

// pinLatestBlock rewrites "latest"/"pending" block tags in a JSON-RPC request to a concrete
// block number, so a node-vs-node comparison resolves the same immutable block on both sides
// instead of racing on their live heads. It is a no-op for requests that carry no such tag
// (e.g. eth_baseFee, which takes no block argument). Fixtures use these tokens only as block
// parameters, so a literal token replacement is safe.
func pinLatestBlock(request []byte, block uint64) []byte {
	pinned := fmt.Sprintf("\"0x%x\"", block)
	s := strings.ReplaceAll(string(request), "\"latest\"", pinned)
	s = strings.ReplaceAll(s, "\"pending\"", pinned)
	return []byte(s)
}

// callBothAtStableHead issues request to both the node under test and the reference,
// retrying until both served it at the same, unchanged latest block. This keeps
// latest-block comparisons (eth_baseFee, eth_gasPrice, eth_blockNumber, ...) from flaking
// on a one-block head skew that arrives between the two calls. testResult and refResult
// are pointers the caller owns for the subsequent comparison. It reports stable=false when
// no consistent window is found within cfg.LatestRetries attempts (e.g. the node under test
// is persistently lagging the reference).
func callBothAtStableHead(
	ctx context.Context,
	cfg *config.Config,
	testClient, refClient *internalrpc.Client,
	testTarget, refTarget string,
	request []byte,
	testResult, refResult any,
) (metrics internalrpc.Metrics, stable bool, err error) {
	const headRetryDelay = 500 * time.Millisecond
	attempts := cfg.LatestRetries
	if attempts < 1 {
		attempts = 1
	}

	for attempt := range attempts {
		// Both nodes must already be on the same head before we issue the request.
		testHeadBefore, _, errTB := internalrpc.GetLatestBlockNumber(ctx, testClient, testTarget)
		refHeadBefore, _, errRB := internalrpc.GetLatestBlockNumber(ctx, refClient, refTarget)
		if errTB != nil || errRB != nil || testHeadBefore != refHeadBefore {
			if attempt < attempts-1 {
				time.Sleep(headRetryDelay)
			}
			continue
		}

		var m internalrpc.Metrics
		if m, err = testClient.Call(ctx, testTarget, request, testResult); err != nil {
			return metrics, false, err
		}
		metrics.RoundTripTime += m.RoundTripTime
		metrics.UnmarshallingTime += m.UnmarshallingTime

		if m, err = refClient.Call(ctx, refTarget, request, refResult); err != nil {
			return metrics, false, err
		}
		metrics.RoundTripTime += m.RoundTripTime
		metrics.UnmarshallingTime += m.UnmarshallingTime

		// Accept only if neither node advanced while serving the request; otherwise the
		// two responses may be from different heads, so retry a fresh window.
		testHeadAfter, _, errTA := internalrpc.GetLatestBlockNumber(ctx, testClient, testTarget)
		refHeadAfter, _, errRA := internalrpc.GetLatestBlockNumber(ctx, refClient, refTarget)
		if errTA == nil && errRA == nil && testHeadAfter == testHeadBefore && refHeadAfter == testHeadBefore {
			return metrics, true, nil
		}
		if attempt < attempts-1 {
			time.Sleep(headRetryDelay)
		}
	}
	return metrics, false, nil
}

// mustAtoi converts a string to int, returning 0 on failure.
func mustAtoi(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// IsStartTestReached checks if we've reached the start-from-test threshold.
// Uses cfg.StartTestNum which is cached at config init time for zero-alloc lookup.
func IsStartTestReached(cfg *config.Config, testNumber int) bool {
	return cfg.StartTest == "" || testNumber >= cfg.StartTestNum
}

// ShouldRunTest determines if a specific test should actually be executed.
// This encapsulates the v1 scheduling logic.
func ShouldRunTest(cfg *config.Config, testName string, testNumberInAnyLoop int) bool {
	if cfg.TestingAPIsWith == "" && cfg.TestingAPIs == "" && (cfg.ReqTestNum == -1 || cfg.ReqTestNum == testNumberInAnyLoop) {
		return true
	}
	if cfg.TestingAPIsWith != "" && checkTestNameForNumber(testName, cfg.ReqTestNum) {
		return true
	}
	if cfg.TestingAPIs != "" && checkTestNameForNumber(testName, cfg.ReqTestNum) {
		return true
	}
	return false
}

// checkTestNameForNumber checks if a test filename like "test_01.json" matches a requested
// test number. Zero-alloc: extracts the number after the last "_" without regex.
func checkTestNameForNumber(testName string, reqTestNumber int) bool {
	if reqTestNumber == -1 {
		return true
	}
	// Find the last "_" to locate the number portion (e.g. "test_01.json" -> "01.json")
	idx := strings.LastIndex(testName, "_")
	if idx < 0 || idx+1 >= len(testName) {
		return false
	}
	// Extract digits after "_", skip leading zeros
	numStr := testName[idx+1:]
	// Strip file extension and any non-digit suffix
	end := 0
	for end < len(numStr) && numStr[end] >= '0' && numStr[end] <= '9' {
		end++
	}
	if end == 0 {
		return false
	}
	n, err := strconv.Atoi(numStr[:end])
	if err != nil {
		return false
	}
	return n == reqTestNumber
}
