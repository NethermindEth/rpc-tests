package runner

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/erigontech/rpc-tests/internal/compare"
	"github.com/erigontech/rpc-tests/internal/config"
	internalrpc "github.com/erigontech/rpc-tests/internal/rpc"
	"github.com/erigontech/rpc-tests/internal/testdata"
)

func TestPinnedComparisonPreservesOriginalOutcome(t *testing.T) {
	const success = `{"jsonrpc":"2.0","id":1,"result":{"gas":1,"failed":false}}`
	for _, tc := range []struct {
		name, actual, reference string
		wantSuccess             bool
	}{
		{"different results", `{"jsonrpc":"2.0","id":1,"result":{"gas":2,"failed":false}}`, success, false},
		{"state unavailable", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"historical state unavailable"}}`, success, false},
		{"different errors", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"first error"}}`, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"second error"}}`, false},
		{"matching results", success, success, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var headCalls, traceCalls atomic.Int32
			serve := func(pinnedResponse string) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var request struct {
						Method string            `json:"method"`
						Params []json.RawMessage `json:"params"`
					}
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					switch request.Method {
					case "eth_blockNumber":
						headCalls.Add(1)
						w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x65"}`))
					case "debug_traceCall":
						traceCalls.Add(1)
						if len(request.Params) != 2 {
							t.Errorf("unexpected params: %s", request.Params)
							return
						}
						switch string(request.Params[1]) {
						case `"0x64"`:
							w.Write([]byte(pinnedResponse))
						case `"0x65"`:
							w.Write([]byte(success))
						default:
							t.Errorf("unexpected block: %s", request.Params[1])
						}
					default:
						t.Errorf("unexpected method: %s", request.Method)
					}
				}))
			}
			native, reference := serve(tc.actual), serve(tc.reference)
			defer native.Close()
			defer reference.Close()
			cfg := config.NewConfig()
			cfg.VerifyWithDaemon = true
			cfg.TestsOnLatestBlock = true
			cfg.PinnedLatestBlock = 100
			cfg.DaemonAsReference = config.ExternalProvider
			cfg.ExternalProviderURL = reference.URL
			cfg.OutputDir = t.TempDir()
			host, port, err := net.SplitHostPort(strings.TrimPrefix(native.URL, "http://"))
			if err != nil {
				t.Fatal(err)
			}
			cfg.DaemonOnHost = host
			cfg.ServerPort, err = strconv.Atoi(port)
			if err != nil {
				t.Fatal(err)
			}
			command := &testdata.JsonRpcCommand{Request: []byte(`{"jsonrpc":"2.0","id":1,"method":"debug_traceCall","params":[{},"latest"]}`)}
			descriptor := &testdata.TestDescriptor{Name: "debug_traceCall/test_pinned.json", TransportType: config.TransportHTTP}
			outcome := testdata.TestOutcome{}
			runCommand(context.Background(), cfg, command, descriptor, &outcome, internalrpc.NewClient(config.TransportHTTP, "", 0))
			if outcome.Success != tc.wantSuccess {
				t.Fatalf("success=%v want=%v error=%v", outcome.Success, tc.wantSuccess, outcome.Error)
			}
			if headCalls.Load() != 0 || traceCalls.Load() != 2 {
				t.Fatalf("comparison changed its context: %d head calls, %d trace calls", headCalls.Load(), traceCalls.Load())
			}
			if tc.wantSuccess {
				return
			}
			if !errors.Is(outcome.Error, compare.ErrDiffMismatch) || outcome.ErrorDetails == nil {
				t.Fatalf("original failure not retained: %+v", outcome)
			}
			decode := func(raw string) any {
				t.Helper()
				var value any
				if err := json.Unmarshal([]byte(raw), &value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			if !reflect.DeepEqual(outcome.ErrorDetails.ActualResponse, decode(tc.actual)) || !reflect.DeepEqual(outcome.ErrorDetails.ExpectedResponse, decode(tc.reference)) {
				t.Fatalf("original responses not retained: %+v", outcome.ErrorDetails)
			}
			if !reflect.DeepEqual(outcome.ErrorDetails.Request, decode(`{"jsonrpc":"2.0","id":1,"method":"debug_traceCall","params":[{},"0x64"]}`)) {
				t.Fatalf("original request not retained: %+v", outcome.ErrorDetails.Request)
			}
			prefix, _, _, _, _ := compare.OutputFilePaths(cfg.OutputDir, descriptor.Name)
			for file, want := range map[string]string{
				prefix + config.GetJSONFilenameExt(config.DaemonOnDefaultPort, cfg.GetTarget(config.DaemonOnDefaultPort, descriptor.Name)): tc.actual,
				prefix + config.GetJSONFilenameExt(config.ExternalProvider, reference.URL):                                                 tc.reference,
			} {
				raw, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(decode(string(raw)), decode(want)) {
					t.Fatalf("original response artifact changed: %s", raw)
				}
			}
		})
	}
}
