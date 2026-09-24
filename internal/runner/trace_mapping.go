package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/erigontech/rpc-tests/internal/compare"
	"github.com/erigontech/rpc-tests/internal/config"
	internalrpc "github.com/erigontech/rpc-tests/internal/rpc"
	"github.com/erigontech/rpc-tests/internal/testdata"
)

const traceCallFlatV1 = "trace-call-flat-v1"

// mappedTraceRequest translates only the documented trace-only trace_call contract.
// Root gas and reverted child return data are outside this version's projection;
// the full native response, including those native fields, is checked separately.
func mappedTraceRequest(mapping string, request []byte) ([]byte, error) {
	if mapping != traceCallFlatV1 {
		return nil, fmt.Errorf("unsupported reference mapping %q", mapping)
	}
	var req map[string]any
	if err := json.Unmarshal(request, &req); err != nil {
		return nil, err
	}
	params, ok := req["params"].([]any)
	if req["method"] != "trace_call" || !ok || (len(params) != 3 && len(params) != 4) {
		return nil, fmt.Errorf("%s requires trace_call with 3 or 4 arguments", mapping)
	}
	if !reflect.DeepEqual(params[1], []any{"trace"}) {
		return nil, fmt.Errorf("%s supports only [trace], not VM traces or state diffs", mapping)
	}
	options := map[string]any{"tracer": "flatCallTracer", "tracerConfig": map[string]any{"convertParityErrors": true}}
	if len(params) == 4 {
		options["stateOverrides"] = params[3]
	}
	req["method"] = "debug_traceCall"
	req["params"] = []any{params[0], params[2], options}
	return json.Marshal(req)
}

func projectTraceCall(response any, reference bool) (any, error) {
	// Work on a copy so saved native/reference artifacts remain untouched.
	raw, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	var envelope map[string]any
	if err = json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	if _, failed := envelope["error"]; failed {
		return nil, fmt.Errorf("RPC error is not a trace: %s", raw)
	}
	var frames []any
	output := any("0x")
	if reference {
		frames, _ = envelope["result"].([]any)
	} else {
		result, ok := envelope["result"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("native trace_call result must be an object")
		}
		frames, _ = result["trace"].([]any)
		output = result["output"]
		if result["vmTrace"] != nil || result["stateDiff"] != nil {
			return nil, fmt.Errorf("projection does not cover VM traces or state diffs")
		}
	}
	if len(frames) == 0 {
		return nil, fmt.Errorf("trace must contain a root frame")
	}
	for i, value := range frames {
		frame, ok := value.(map[string]any)
		if !ok || frame["type"] != "call" {
			return nil, fmt.Errorf("frame %d: mapping supports call frames only", i)
		}
		action, ok := frame["action"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("frame %d has no action", i)
		}
		address, ok := frame["traceAddress"].([]any)
		if !ok || (i == 0 && len(address) != 0) {
			return nil, fmt.Errorf("frame %d has invalid traceAddress", i)
		}
		result, _ := frame["result"].(map[string]any)
		if i == 0 {
			if reference && result != nil {
				output = result["output"]
			}
			// Parity reports execution gas; Geth's root includes intrinsic gas and
			// receipt refund accounting. Native fixtures still check both gas fields.
			delete(action, "gas")
			delete(result, "gasUsed")
		}
		if frame["error"] != nil {
			// Geth retains a result for REVERT; Parity reports the frame's error only.
			delete(frame, "result")
		}
		if reference {
			for _, key := range []string{"blockHash", "blockNumber", "transactionHash", "transactionPosition"} {
				v, exists := frame[key]
				if !exists || (v != nil && v != float64(0)) {
					return nil, fmt.Errorf("unexpected trace_call context %s: %v", key, v)
				}
				delete(frame, key)
			}
		}
	}
	if _, ok := output.(string); !ok {
		return nil, fmt.Errorf("trace output must be a hex string")
	}
	return map[string]any{"jsonrpc": envelope["jsonrpc"], "id": envelope["id"], "result": map[string]any{"output": output, "trace": frames}}, nil
}

func runMappedTrace(ctx context.Context, cfg *config.Config, cmd *testdata.JsonRpcCommand, descriptor *testdata.TestDescriptor, outcome *testdata.TestOutcome, client *internalrpc.Client) {
	request := cmd.Request
	if cfg.VerifyWithDaemon {
		if !cfg.TestsOnLatestBlock || cfg.PinnedLatestBlock == 0 {
			outcome.Error = fmt.Errorf("mapped live traces require -L and a pinned common block")
			return
		}
		request = pinLatestBlock(request, cfg.PinnedLatestBlock)
	}
	referenceRequest, err := mappedTraceRequest(cmd.ReferenceMapping, request)
	if err != nil {
		outcome.Error = err
		return
	}
	target := cfg.GetTarget(cfg.DaemonUnderTest, descriptor.Name)
	var actual any
	metrics, err := client.Call(ctx, target, request, &actual)
	outcome.Metrics.RoundTripTime += metrics.RoundTripTime
	outcome.Metrics.UnmarshallingTime += metrics.UnmarshallingTime
	if err != nil {
		outcome.Error = err
		enrichErrorDetails(outcome, target, request)
		return
	}
	nativeMatches := reflect.DeepEqual(actual, cmd.Response)
	actualProjection, projectionErr := projectTraceCall(actual, false)
	evidence := map[string]any{"mapping": cmd.ReferenceMapping, "nativeRequest": json.RawMessage(request), "nativeResponse": actual, "nativeExpected": cmd.Response, "nativeSchemaMatched": nativeMatches, "nativeProjection": actualProjection}
	mappedMatches := !cfg.VerifyWithDaemon
	var referenceProjection any
	if cfg.VerifyWithDaemon {
		refClient := client
		if len(cfg.ExternalProviderHeaders) > 0 {
			refClient = internalrpc.NewClientWithHeaders(descriptor.TransportType, "", cfg.VerboseLevel, cfg.ExternalProviderHeaders)
		}
		refTarget := cfg.GetTarget(cfg.DaemonAsReference, descriptor.Name)
		var reference any
		hashes := make([]string, 0, 2)
		blockRequest, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": []any{fmt.Sprintf("0x%x", cfg.PinnedLatestBlock), false}})
		for _, endpoint := range []struct {
			client       *internalrpc.Client
			target, name string
		}{{client, target, "nativeBlock"}, {refClient, refTarget, "referenceBlock"}} {
			var block map[string]any
			m, e := endpoint.client.Call(ctx, endpoint.target, blockRequest, &block)
			outcome.Metrics.RoundTripTime += m.RoundTripTime
			outcome.Metrics.UnmarshallingTime += m.UnmarshallingTime
			evidence[endpoint.name] = block
			if e != nil {
				err = e
				break
			}
			value, _ := block["result"].(map[string]any)
			hash, _ := value["hash"].(string)
			if hash == "" {
				err = fmt.Errorf("cannot verify common block hash")
				break
			}
			hashes = append(hashes, hash)
		}
		if err == nil && (len(hashes) != 2 || hashes[0] != hashes[1]) {
			err = fmt.Errorf("reference and native block hashes differ")
		}
		evidence["commonBlockVerified"] = err == nil
		m, callErr := refClient.Call(ctx, refTarget, referenceRequest, &reference)
		outcome.Metrics.RoundTripTime += m.RoundTripTime
		outcome.Metrics.UnmarshallingTime += m.UnmarshallingTime
		evidence["referenceRequest"] = json.RawMessage(referenceRequest)
		evidence["referenceResponse"] = reference
		if callErr != nil {
			err = callErr
		} else {
			var referenceErr error
			referenceProjection, referenceErr = projectTraceCall(reference, true)
			if referenceErr != nil {
				err = referenceErr
			}
			mappedMatches = err == nil && projectionErr == nil && reflect.DeepEqual(actualProjection, referenceProjection)
		}
		evidence["referenceProjection"] = referenceProjection
		evidence["mappedReferenceMatched"] = mappedMatches
	}
	if projectionErr != nil {
		err = projectionErr
	}
	if err != nil {
		evidence["error"] = err.Error()
	}
	base, dir, _, _, _ := compare.OutputFilePaths(cfg.OutputDir, descriptor.Name)
	artifact := base + "-mapping.json"
	if err := os.MkdirAll(dir, 0755); err != nil {
		outcome.Error = err
		return
	}
	encoded, encodeErr := json.MarshalIndent(evidence, "", "  ")
	if encodeErr != nil {
		outcome.Error = encodeErr
		return
	}
	if writeErr := os.WriteFile(artifact, encoded, 0644); writeErr != nil {
		outcome.Error = writeErr
		return
	}
	outcome.Metrics.ComparisonCount++
	if err != nil {
		outcome.Error = err
	} else if !nativeMatches {
		outcome.Error = fmt.Errorf("native trace schema/value mismatch (see %s)", filepath.Base(artifact))
	} else if !mappedMatches {
		outcome.Error = fmt.Errorf("mapped trace mismatch (see %s)", filepath.Base(artifact))
	}
	if outcome.Error != nil {
		outcome.ErrorDetails = &testdata.ErrorDetails{Message: outcome.Error.Error(), Target: target, ActualResponse: actual, ExpectedResponse: cmd.Response}
		enrichErrorDetails(outcome, target, request)
		return
	}
	outcome.Success = true
	outcome.Metrics.EqualCount++
}
