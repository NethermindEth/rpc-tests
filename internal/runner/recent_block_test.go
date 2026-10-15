package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/rpc-tests/internal/config"
	"github.com/erigontech/rpc-tests/internal/filter"
	internalrpc "github.com/erigontech/rpc-tests/internal/rpc"
	"github.com/erigontech/rpc-tests/internal/testdata"
)

func TestRecentBlockContext(t *testing.T) {
	selectors := []struct{ method, selector string }{
		{"debug_traceTransaction", "$transactionHash"},
		{"debug_getRawTransaction", "$transactionHash"},
		{"debug_traceBlockByNumber", "$blockNumber"},
		{"debug_traceBlockByHash", "$blockHash"},
		{"debug_intermediateRoots", "$blockHash"},
		{"debug_traceBlock", "$rawBlock"},
		{"debug_getRawBlock", "$blockNumber"},
		{"debug_getRawBlock", "$blockHash"},
		{"debug_getRawHeader", "$blockNumber"},
		{"debug_getRawHeader", "$blockHash"},
		{"debug_getRawReceipts", "$blockNumber"},
		{"debug_getRawReceipts", "$blockHash"},
	}
	cases := []string{"match", "older retained pin", "different responses", "both RPC errors", "both null", "wrong response id", "HTTP error", "different blocks", "different transactions", "reorg", "empty block", "all empty", "stale head", "lagging head", "syncing", "unknown context", "missing pin", "no reference", "not latest", "mapping conflict", "wrong selector", "unsupported method", "different raw blocks", "cancelled"}
	for _, selected := range selectors {
		method := selected.method
		for _, name := range cases {
			t.Run(method+"/"+selected.selector+"/"+name, func(t *testing.T) {
				t.Parallel()
				var traceCalls atomic.Int32
				var requests [2]atomic.Value
				timestamp := time.Now().Unix()
				txs := []string{fmt.Sprintf("0x%064x", 1), fmt.Sprintf("0x%064x", 2)}
				serve := func(side int) *httptest.Server {
					return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var request struct {
							Method string `json:"method"`
							Params []any  `json:"params"`
						}
						var raw json.RawMessage
						if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
							t.Error(err)
							return
						}
						if err := json.Unmarshal(raw, &request); err != nil {
							t.Error(err)
							return
						}
						response := map[string]any{"jsonrpc": "2.0", "id": 1}
						switch request.Method {
						case "web3_clientVersion":
							response["result"] = "test/v1"
						case "eth_syncing":
							response["result"] = name == "syncing"
						case "eth_getBlockByNumber":
							number := request.Params[0].(string)
							latest := number == "latest"
							if number == "latest" {
								number = "0x100"
								if name == "lagging head" {
									number = "0xff"
								}
							}
							n, _ := strconv.ParseUint(number[2:], 16, 64)
							hash := fmt.Sprintf("0x%064x", n)
							if (name == "different blocks" && side == 1) || (name == "reorg" && traceCalls.Load() > 0) {
								hash = fmt.Sprintf("0x%064x", n+1000)
							}
							transactions := append([]string{}, txs...)
							if name == "all empty" || (name == "empty block" && number == "0xfc") {
								transactions = []string{}
							}
							if name == "different transactions" && side == 1 {
								transactions[0], transactions[1] = transactions[1], transactions[0]
							}
							stamp := timestamp
							if name == "stale head" {
								stamp -= 600
							} else if name == "older retained pin" && !latest {
								stamp -= 600
							}
							response["result"] = recentBlock{Number: number, Hash: hash, ParentHash: fmt.Sprintf("0x%064x", n-1), Timestamp: fmt.Sprintf("0x%x", stamp), Transactions: transactions}
						case "debug_getRawBlock":
							if method == "debug_traceBlock" {
								response["result"] = "0xabcd"
								if name == "different raw blocks" && side == 1 {
									response["result"] = "0xabce"
								}
								break
							}
							fallthrough
						default:
							traceCalls.Add(1)
							requests[side].Store(append([]byte(nil), raw...))
							result := any(map[string]any{"type": "CALL", "gasUsed": "0x1"})
							switch method {
							case "debug_traceBlockByNumber", "debug_traceBlockByHash", "debug_traceBlock":
								result = []any{map[string]any{"txHash": txs[0], "result": result}, map[string]any{"txHash": txs[1], "result": []any{}}}
							case "debug_intermediateRoots":
								result = []any{fmt.Sprintf("0x%064x", 7), fmt.Sprintf("0x%064x", 8)}
							case "debug_getRawBlock", "debug_getRawHeader", "debug_getRawTransaction":
								result = "0x1234"
							case "debug_getRawReceipts":
								result = []any{"0x1234", "0x5678"}
							}
							response["result"] = result
							switch name {
							case "different responses":
								if side == 1 {
									response["extra"] = true
								}
							case "both RPC errors":
								delete(response, "result")
								response["error"] = map[string]any{"code": -32000, "message": "historical state unavailable"}
							case "both null":
								response["result"] = nil
							case "wrong response id":
								response["id"] = 42
							case "HTTP error":
								w.WriteHeader(http.StatusServiceUnavailable)
								return
							}
						}
						json.NewEncoder(w).Encode(response)
					}))
				}
				native, reference := serve(0), serve(1)
				defer native.Close()
				defer reference.Close()
				cfg := config.NewConfig()
				cfg.VerifyWithDaemon = name != "no reference"
				cfg.TestsOnLatestBlock = name != "not latest"
				cfg.PinnedLatestBlock = 256
				cfg.DaemonAsReference = config.ExternalProvider
				cfg.ExternalProviderURL = reference.URL
				cfg.OutputDir = t.TempDir()
				host, port, _ := net.SplitHostPort(strings.TrimPrefix(native.URL, "http://"))
				cfg.DaemonOnHost = host
				cfg.ServerPort, _ = strconv.Atoi(port)
				selector := selected.selector
				if name == "wrong selector" {
					selector = "latest"
				}
				requestMethod := method
				if name == "unsupported method" {
					requestMethod = "eth_getBalance"
				}
				raw := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":[%q,{"tracer":"callTracer","tracerConfig":{"text":"$blockNumber"}}]}`, requestMethod, selector))
				original := append([]byte(nil), raw...)
				command := &testdata.JsonRpcCommand{Request: raw, ReferenceContext: recentBlockV1}
				if name == "unknown context" {
					command.ReferenceContext = "future"
				} else if name == "mapping conflict" {
					command.ReferenceMapping = traceCallFlatV1
				} else if name == "missing pin" {
					cfg.PinnedLatestBlock = 0
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if name == "cancelled" {
					cancel()
				}
				outcome := testdata.TestOutcome{}
				runCommand(ctx, cfg, command, &testdata.TestDescriptor{Name: method + "/test_recent.json", TransportType: config.TransportHTTP}, &outcome, internalrpc.NewClient(config.TransportHTTP, "", 0))
				want := name == "match" || name == "older retained pin" || name == "empty block" || (name == "different raw blocks" && method != "debug_traceBlock")
				if outcome.Success != want || (!want && outcome.Error == nil) {
					t.Fatalf("success=%v want=%v err=%v", outcome.Success, want, outcome.Error)
				}
				if !bytes.Equal(command.Request, original) {
					t.Fatal("fixture template was mutated")
				}
				artifact, err := os.ReadFile(filepath.Join(cfg.OutputDir, method, "test_recent-context.json"))
				if err != nil {
					t.Fatal(err)
				}
				var evidence map[string]any
				if err = json.Unmarshal(artifact, &evidence); err != nil || evidence["success"] != want {
					t.Fatalf("invalid evidence: %v %s", err, artifact)
				}
				if want {
					if evidence["commonBlockVerified"] != true || traceCalls.Load() != 2 || len(evidence["responses"].([]any)) != 2 {
						t.Fatal("successful test lacks responses/common-block evidence")
					}
					for side := range requests {
						sent := requests[side].Load().([]byte)
						var actual map[string]any
						json.Unmarshal(sent, &actual)
						args := actual["params"].([]any)
						number := "0xfc"
						if name == "empty block" {
							number = "0xfb"
						}
						expected := number
						switch selector {
						case "$transactionHash":
							expected = txs[0]
						case "$blockHash":
							n, _ := strconv.ParseUint(number[2:], 16, 64)
							expected = fmt.Sprintf("0x%064x", n)
						case "$rawBlock":
							expected = "0xabcd"
						}
						if args[0] != expected || args[1].(map[string]any)["tracerConfig"].(map[string]any)["text"] != "$blockNumber" {
							t.Fatalf("wrong selector or unrelated option changed: %s", sent)
						}
					}
				}
			})
		}
	}
}

func TestRecentBlockResultValidation(t *testing.T) {
	txs := []string{"first", "second"}
	block := recentBlock{Transactions: txs}
	for _, result := range []any{
		[]any{},
		[]any{map[string]any{"txHash": "first", "result": map[string]any{}}},
		[]any{map[string]any{"txHash": "second", "result": map[string]any{}}, map[string]any{"txHash": "first", "result": map[string]any{}}},
		[]any{map[string]any{"txHash": "first", "error": "missing state"}, map[string]any{"txHash": "second", "result": map[string]any{}}},
	} {
		if validateRecentResult("debug_traceBlockByNumber", result, block) == nil {
			t.Fatalf("accepted incomplete/error/reordered block trace: %v", result)
		}
	}
	if validateRecentResult("debug_getRawReceipts", []any{"0x1234", "0x"}, block) == nil {
		t.Fatal("accepted empty receipt")
	}
	root := fmt.Sprintf("0x%064x", 1)
	for _, result := range []any{[]any{root}, []any{root, "0x1234"}, []any{root, nil}} {
		if validateRecentResult("debug_intermediateRoots", result, block) == nil {
			t.Fatalf("accepted incomplete or malformed intermediate roots: %v", result)
		}
	}
	for _, result := range []any{nil, "0x", 1} {
		if validateRecentResult("debug_traceTransaction", result, block) == nil {
			t.Fatalf("accepted a non-tracer result: %v", result)
		}
	}
	if validateRecentResult("debug_traceBlock", []any{map[string]any{"txHash": "first", "result": []any{}}, map[string]any{"txHash": "second", "result": nil}}, block) == nil {
		t.Fatal("accepted a block trace without a tracer result")
	}
}

func TestRecentBlockFixturesSelectedOnlyOnLatestPass(t *testing.T) {
	files, err := filepath.Glob("../../integration/mainnet/debug_*/test_*.json")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, file := range files {
		commands, err := testdata.LoadFixture(file, false, &testdata.TestMetrics{})
		if err != nil {
			t.Fatal(err)
		}
		for _, cmd := range commands {
			if cmd.ReferenceContext == "" {
				continue
			}
			count++
			api := filepath.Base(filepath.Dir(file))
			name := api + "/" + filepath.Base(file)
			cfg := filter.FilterConfig{Net: "mainnet", ReqTestNum: -1, TestingAPIsWith: "debug_"}
			if !testdata.HasTag(file, testdata.TagFull) || !testdata.HasTag(file, testdata.TagPruned) || !filter.New(cfg).IsSkipped(api, name, 0) {
				t.Fatalf("recent fixture not restricted to pruned-compatible latest pass: %s", name)
			}
			cfg.TestsOnLatestBlock = true
			if !filter.New(cfg).APIUnderTest(api, name) || filter.New(cfg).IsSkipped(api, name, 0) {
				t.Fatalf("recent fixture missing from latest pass: %s", name)
			}
			var req map[string]any
			json.Unmarshal(cmd.Request, &req)
			if !reflect.DeepEqual(req["method"], api) || cmd.ReferenceContext != recentBlockV1 {
				t.Fatalf("invalid recent fixture: %s", name)
			}
		}
	}
	if count != 33 {
		t.Fatalf("selected %d recent-block fixtures, want 33", count)
	}
}
