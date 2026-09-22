package runner

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/erigontech/rpc-tests/internal/config"
	internalrpc "github.com/erigontech/rpc-tests/internal/rpc"
	"github.com/erigontech/rpc-tests/internal/testdata"
)

func traceMappingFixture(t *testing.T) (*testdata.JsonRpcCommand, map[string]any) {
	t.Helper()
	commands, err := testdata.LoadFixture("../../integration/mainnet/trace_call/test_34.json", false, &testdata.TestMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/trace-call-flat-reference.json")
	if err != nil {
		t.Fatal(err)
	}
	var reference map[string]any
	if err = json.Unmarshal(data, &reference); err != nil {
		t.Fatal(err)
	}
	return &commands[0], reference
}

func TestMappedTraceNativeSchemaAndReference(t *testing.T) {
	for _, name := range []string{"match", "native schema mismatch despite matching projection", "nested gas mismatch", "reference error", "both return errors", "empty reference", "wrong reference context", "unknown frame type", "unknown mapping", "missing pin", "different block hash", "regression only"} {
		t.Run(name, func(t *testing.T) {
			command, reference := traceMappingFixture(t)
			raw, _ := json.Marshal(command.Response)
			var actual map[string]any
			json.Unmarshal(raw, &actual)
			frames := reference["result"].([]any)
			switch name {
			case "native schema mismatch despite matching projection":
				actual["extraNativeField"] = true
			case "nested gas mismatch":
				frames[1].(map[string]any)["action"].(map[string]any)["gas"] = "0x1"
			case "reference error":
				reference = map[string]any{"jsonrpc": "2.0", "id": float64(1), "error": map[string]any{"code": float64(-32000), "message": "unavailable"}}
			case "both return errors":
				reference = map[string]any{"jsonrpc": "2.0", "id": float64(1), "error": map[string]any{"code": float64(-32000), "message": "unavailable"}}
				actual = reference
				command.Response = reference
			case "empty reference":
				reference["result"] = []any{}
			case "wrong reference context":
				frames[0].(map[string]any)["blockNumber"] = float64(42)
			case "unknown frame type":
				frames[0].(map[string]any)["type"] = "create"
			case "unknown mapping":
				command.ReferenceMapping = "unknown-v2"
			}
			cfg := config.NewConfig()
			cfg.VerifyWithDaemon = name != "regression only"
			cfg.TestsOnLatestBlock = true
			cfg.PinnedLatestBlock = 100
			cfg.DaemonUnderTest = config.DaemonOnDefaultPort
			cfg.DaemonAsReference = config.ExternalProvider
			cfg.OutputDir = t.TempDir()
			if name == "missing pin" {
				cfg.PinnedLatestBlock = 0
			}
			var nativeRequest, referenceRequest map[string]any
			var requestsLock sync.Mutex
			native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if request["method"] == "eth_getBlockByNumber" {
					json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"hash": "0x1234"}})
					return
				}
				requestsLock.Lock()
				nativeRequest = request
				requestsLock.Unlock()
				json.NewEncoder(w).Encode(actual)
			}))
			defer native.Close()
			ref := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if request["method"] == "eth_getBlockByNumber" {
					hash := "0x1234"
					if name == "different block hash" {
						hash = "0x5678"
					}
					json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"hash": hash}})
					return
				}
				requestsLock.Lock()
				referenceRequest = request
				requestsLock.Unlock()
				json.NewEncoder(w).Encode(reference)
			}))
			defer ref.Close()
			host, port, _ := net.SplitHostPort(strings.TrimPrefix(native.URL, "http://"))
			cfg.DaemonOnHost = host
			cfg.ServerPort, _ = strconv.Atoi(port)
			cfg.ExternalProviderURL = ref.URL
			outcome := testdata.TestOutcome{}
			runCommand(context.Background(), cfg, command, &testdata.TestDescriptor{Name: "trace_call/test_34.json", TransportType: config.TransportHTTP}, &outcome, internalrpc.NewClient(config.TransportHTTP, "", 0))
			want := name == "match" || name == "regression only"
			if outcome.Success != want {
				t.Fatalf("success=%v want=%v error=%v", outcome.Success, want, outcome.Error)
			}
			if !want && outcome.Error == nil {
				t.Fatal("failure without error")
			}
			if name == "match" {
				requestsLock.Lock()
				defer requestsLock.Unlock()
				np := nativeRequest["params"].([]any)
				rp := referenceRequest["params"].([]any)
				if nativeRequest["method"] != "trace_call" || referenceRequest["method"] != "debug_traceCall" || np[2] != "0x64" || rp[1] != "0x64" {
					t.Fatalf("requests not translated/pinned: %v / %v", nativeRequest, referenceRequest)
				}
				opts := rp[2].(map[string]any)
				if !reflect.DeepEqual(np[0], rp[0]) || !reflect.DeepEqual(np[3], opts["stateOverrides"]) {
					t.Fatal("call or state overrides changed")
				}
			}
			if name != "unknown mapping" && name != "missing pin" {
				files, err := filepath.Glob(filepath.Join(cfg.OutputDir, "trace_call", "*-mapping.json"))
				if err != nil || len(files) != 1 {
					t.Fatalf("missing evidence: %v %v", files, err)
				}
				b, err := os.ReadFile(files[0])
				if err != nil {
					t.Fatal(err)
				}
				var evidence map[string]any
				if err = json.Unmarshal(b, &evidence); err != nil {
					t.Fatal(err)
				}
				if name == "native schema mismatch despite matching projection" && (evidence["nativeSchemaMatched"] != false || evidence["mappedReferenceMatched"] != true) {
					t.Fatalf("schema guard not independent: %s", b)
				}
			}
		})
	}
}

func TestTraceMappingRejectsUnsupportedRequests(t *testing.T) {
	for _, request := range []string{
		`{"method":"debug_traceCall","params":[{},["trace"],"latest"]}`,
		`{"method":"trace_call","params":[{},["trace","vmTrace"],"latest"]}`,
		`{"method":"trace_call","params":[{},["stateDiff"],"latest"]}`,
		`{"method":"trace_call","params":[{},["trace"]]}`,
	} {
		if _, err := mappedTraceRequest(traceCallFlatV1, []byte(request)); err == nil {
			t.Errorf("accepted %s", request)
		}
	}
}
