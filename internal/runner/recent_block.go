package runner

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/rpc-tests/internal/compare"
	"github.com/erigontech/rpc-tests/internal/config"
	internalrpc "github.com/erigontech/rpc-tests/internal/rpc"
	"github.com/erigontech/rpc-tests/internal/testdata"
)

const recentBlockV1 = "recent-block-v1"

type recentBlock struct {
	Number       string   `json:"number"`
	Hash         string   `json:"hash"`
	ParentHash   string   `json:"parentHash"`
	Timestamp    string   `json:"timestamp"`
	Transactions []string `json:"transactions"`
}

func rpcResult(response any, id any) (any, error) {
	envelope, ok := response.(map[string]any)
	if !ok || envelope["jsonrpc"] != "2.0" || !reflect.DeepEqual(envelope["id"], id) {
		return nil, fmt.Errorf("invalid JSON-RPC envelope or response id")
	}
	if rpcError, exists := envelope["error"]; exists {
		return nil, fmt.Errorf("RPC error is not a successful result: %v", rpcError)
	}
	result := envelope["result"]
	if result == nil {
		return nil, fmt.Errorf("missing or null RPC result")
	}
	return result, nil
}

func hexBytes(value any, size int) bool {
	s, ok := value.(string)
	if !ok || !strings.HasPrefix(s, "0x") {
		return false
	}
	b, err := hex.DecodeString(s[2:])
	return err == nil && len(b) > 0 && (size < 0 || len(b) == size)
}

func parseRecentBlock(value any) (recentBlock, error) {
	var block recentBlock
	raw, err := json.Marshal(value)
	if err == nil {
		err = json.Unmarshal(raw, &block)
	}
	if err != nil || !hexBytes(block.Hash, 32) || !hexBytes(block.ParentHash, 32) || block.Transactions == nil {
		return block, fmt.Errorf("missing or malformed block metadata")
	}
	for _, tx := range block.Transactions {
		if !hexBytes(tx, 32) {
			return block, fmt.Errorf("invalid transaction hash")
		}
	}
	stamp, err := strconv.ParseUint(strings.TrimPrefix(block.Timestamp, "0x"), 16, 64)
	now := time.Now().Unix()
	if err != nil || stamp > uint64(now+15) || stamp < uint64(now-300) {
		return block, fmt.Errorf("block timestamp is stale or invalid")
	}
	return block, nil
}

func validateRecentResult(method string, result any, block recentBlock) error {
	switch method {
	case "debug_traceTransaction":
		if _, ok := result.(map[string]any); ok {
			return nil
		}
	case "debug_traceBlockByNumber", "debug_traceBlockByHash":
		traces, ok := result.([]any)
		if ok && len(traces) == len(block.Transactions) {
			for i, trace := range traces {
				entry, ok := trace.(map[string]any)
				if !ok || entry["txHash"] != block.Transactions[i] || entry["error"] != nil {
					return fmt.Errorf("block trace %d has an error or wrong transaction hash/order", i)
				}
				if _, ok := entry["result"].(map[string]any); !ok {
					return fmt.Errorf("block trace %d has no tracer result", i)
				}
			}
			return nil
		}
	case "debug_getRawBlock", "debug_getRawHeader":
		if hexBytes(result, -1) {
			return nil
		}
	case "debug_getRawReceipts":
		receipts, ok := result.([]any)
		if ok && len(receipts) == len(block.Transactions) {
			for _, receipt := range receipts {
				if !hexBytes(receipt, -1) {
					return fmt.Errorf("invalid raw receipt")
				}
			}
			return nil
		}
	}
	return fmt.Errorf("unexpected %s result shape or transaction count", method)
}

func runRecentBlockTest(ctx context.Context, cfg *config.Config, cmd *testdata.JsonRpcCommand, descriptor *testdata.TestDescriptor, outcome *testdata.TestOutcome, client *internalrpc.Client) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	evidence := map[string]any{"context": cmd.ReferenceContext, "template": json.RawMessage(cmd.Request)}
	base, dir, _, _, _ := compare.OutputFilePaths(cfg.OutputDir, descriptor.Name)
	defer func() {
		if outcome.Error != nil {
			evidence["error"] = outcome.Error.Error()
		}
		evidence["success"] = outcome.Success
		encoded, err := json.MarshalIndent(evidence, "", "  ")
		if err == nil {
			err = os.MkdirAll(dir, 0755)
		}
		if err == nil {
			err = os.WriteFile(base+"-context.json", encoded, 0644)
		}
		if err != nil {
			outcome.Success = false
			outcome.Error = fmt.Errorf("save recent-block evidence: %w", err)
		}
	}()
	fail := func(err error) { outcome.Error = err }
	if cmd.ReferenceContext != recentBlockV1 || cmd.ReferenceMapping != "" || !cfg.VerifyWithDaemon || !cfg.TestsOnLatestBlock || cfg.PinnedLatestBlock <= 4 {
		fail(fmt.Errorf("recent-block-v1 requires a reference, -L and a recent pin; reference mappings cannot be combined"))
		return
	}
	var request map[string]json.RawMessage
	if err := json.Unmarshal(cmd.Request, &request); err != nil {
		fail(err)
		return
	}
	var method string
	var params []json.RawMessage
	var id any
	if json.Unmarshal(request["method"], &method) != nil || json.Unmarshal(request["params"], &params) != nil || len(params) == 0 || json.Unmarshal(request["id"], &id) != nil || id == nil {
		fail(fmt.Errorf("invalid recent-block request"))
		return
	}
	placeholder := map[string]string{
		"debug_traceTransaction": "$transactionHash", "debug_traceBlockByNumber": "$blockNumber", "debug_traceBlockByHash": "$blockHash",
		"debug_getRawBlock": "$blockNumber", "debug_getRawHeader": "$blockNumber", "debug_getRawReceipts": "$blockNumber",
	}[method]
	var selector string
	if placeholder == "" || json.Unmarshal(params[0], &selector) != nil || selector != placeholder {
		fail(fmt.Errorf("unsupported method or misplaced recent-block selector"))
		return
	}
	targets := []string{cfg.GetTarget(config.DaemonOnDefaultPort, descriptor.Name), cfg.GetTarget(cfg.DaemonAsReference, descriptor.Name)}
	clients := []*internalrpc.Client{client, client}
	if len(cfg.ExternalProviderHeaders) > 0 {
		clients[1] = internalrpc.NewClientWithHeaders(descriptor.TransportType, "", cfg.VerboseLevel, cfg.ExternalProviderHeaders)
	}
	var calls []map[string]any
	call := func(side int, req []byte) (any, error) {
		var response any
		m, err := clients[side].Call(ctx, targets[side], req, &response)
		outcome.Metrics.RoundTripTime += m.RoundTripTime
		outcome.Metrics.UnmarshallingTime += m.UnmarshallingTime
		record := map[string]any{"side": side, "request": json.RawMessage(req), "response": response}
		if err != nil {
			record["error"] = err.Error()
		}
		calls = append(calls, record)
		evidence["calls"] = calls
		return response, err
	}
	query := func(side int, method string, args ...any) (any, error) {
		if args == nil {
			args = []any{}
		}
		req, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": args})
		response, err := call(side, req)
		if err != nil {
			return nil, err
		}
		return rpcResult(response, float64(1))
	}
	readBlock := func(side int, number string) (recentBlock, error) {
		value, err := query(side, "eth_getBlockByNumber", number, false)
		if err != nil {
			return recentBlock{}, err
		}
		return parseRecentBlock(value)
	}
	for side := range targets {
		version, err := query(side, "web3_clientVersion")
		versionText, ok := version.(string)
		if err != nil || !ok || versionText == "" {
			fail(fmt.Errorf("cannot record client version: %v", err))
			return
		}
		syncing, err := query(side, "eth_syncing")
		if err != nil || syncing != false {
			fail(fmt.Errorf("client is syncing or unavailable: %v", err))
			return
		}
		head, err := readBlock(side, "latest")
		number, parseErr := strconv.ParseUint(strings.TrimPrefix(head.Number, "0x"), 16, 64)
		if err != nil || parseErr != nil || number < cfg.PinnedLatestBlock {
			fail(fmt.Errorf("client has no fresh head at the requested pin: %v", err))
			return
		}
	}
	var block recentBlock
	// Select before tracing, allowing a bounded walk past empty blocks. Never
	// replace a failed comparison with a different block's successful result.
	for offset := uint64(4); offset < 12 && offset < cfg.PinnedLatestBlock; offset++ {
		number := fmt.Sprintf("0x%x", cfg.PinnedLatestBlock-offset)
		actual, err := readBlock(0, number)
		if err != nil {
			fail(err)
			return
		}
		reference, err := readBlock(1, number)
		if err != nil || actual.Number != number || !reflect.DeepEqual(actual, reference) {
			fail(fmt.Errorf("common block metadata differs or is unavailable: %v", err))
			return
		}
		if len(actual.Transactions) > 0 {
			block = actual
			break
		}
	}
	if len(block.Transactions) == 0 {
		fail(fmt.Errorf("no nonempty common block in the recent selection window"))
		return
	}
	evidence["block"] = block
	resolved := map[string]string{"$blockNumber": block.Number, "$blockHash": block.Hash, "$transactionHash": block.Transactions[0]}[placeholder]
	params[0], _ = json.Marshal(resolved)
	request["params"], _ = json.Marshal(params)
	raw, _ := json.Marshal(request)
	evidence["request"] = json.RawMessage(raw)
	responses := make([]any, 2)
	var callErrors [2]error
	for side := range targets {
		responses[side], callErrors[side] = call(side, raw)
	}
	evidence["responses"] = responses
	for side, response := range responses {
		value, err := rpcResult(response, id)
		if callErrors[side] != nil {
			err = callErrors[side]
		}
		if err == nil {
			err = validateRecentResult(method, value, block)
		}
		if err != nil {
			fail(fmt.Errorf("client %d: %w", side, err))
			return
		}
	}
	for side := range targets {
		after, err := readBlock(side, block.Number)
		if err != nil || !reflect.DeepEqual(after, block) {
			fail(fmt.Errorf("canonical block changed during comparison: %v", err))
			return
		}
	}
	evidence["commonBlockVerified"] = true
	outcome.Metrics.ComparisonCount++
	if !reflect.DeepEqual(responses[0], responses[1]) {
		outcome.Error = fmt.Errorf("recent-block responses differ (see %s-context.json)", base)
		outcome.ErrorDetails = &testdata.ErrorDetails{Message: outcome.Error.Error(), ActualResponse: responses[0], ExpectedResponse: responses[1]}
		enrichErrorDetails(outcome, targets[0], raw)
		return
	}
	outcome.Success = true
	outcome.Metrics.EqualCount++
}
