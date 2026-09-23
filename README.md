# rpc-tests

Collection of JSON-RPC black-box testing tools for Ethereum node implementations.

## Table of Contents
1. [Installation](#installation)
2. [Integration Testing](#integration-testing)
3. [Performance Testing](#performance-testing)
4. [Standalone Tools](#standalone-tools)
5. [Development](#development)
6. [Legacy Python Tools](#legacy-python-tools)

## Installation

### Prerequisites

* [Go](https://go.dev/) >= 1.24

### Getting Started

```
git clone https://github.com/erigontech/rpc-tests.git
cd rpc-tests
make
```

## Integration Testing

Check out the dedicated guide in [Integration Tests](./integration/README.md).

### Known (expected) failures

`known-failures.json` (repo root) lists tests that are expected to fail while a fix
is pending, keyed by `"<api>/<test>"` (extension-insensitive) with a linked PR:

```json
{
  "eth_getBalance/test_40": { "pr": "ethereum/go-ethereum#35271", "note": "geth -32000 vs NM -32602; geth-side fix" }
}
```

Listed tests still **run**, but:

- a listed test that **fails** is reported as `KNOWN_FAIL (<pr>)` and does **not** fail the run;
- a listed test that **passes** is reported as `UNEXPECTED_PASS` (a warning to remove the entry) — with `--strict-known-failures` this fails the run to force the cleanup.

Flags: `--known-failures <path>` (default `known-failures.json`), `--strict-known-failures`.
Prefer this over excluding/skipping a test: coverage is preserved, and you're told the moment the fix lands.

## Performance Testing

Check out the dedicated guide in [Performance Tests](./perf/README.md).

## Standalone Tools

The `rpc_int` binary includes several standalone subcommands for targeted testing and diagnostics. Each subcommand has its own `--help` flag.

### Get Latest/Safe/Finalized Blocks

Query block numbers for latest, safe, and finalized tags via WebSocket every 2 seconds.

```bash
./build/bin/rpc_int block-by-number --url ws://127.0.0.1:8545
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `ws://127.0.0.1:8545` | WebSocket URL of the Ethereum node |

### Find Empty Blocks

Search backward from the latest block for N empty blocks (no transactions).

```bash
./build/bin/rpc_int empty-blocks --url http://localhost:8545 --count 10
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `http://localhost:8545` | HTTP URL of the Ethereum node |
| `--count` | `10` | Number of empty blocks to find |
| `--ignore-withdrawals` | `false` | Ignore withdrawals when determining if a block is empty |
| `--compare-state-root` | `false` | Compare state root with parent block |

### Query Filter Changes

Create an ERC20 Transfer filter and poll for changes/logs via WebSocket.

```bash
./build/bin/rpc_int filter-changes --url ws://127.0.0.1:8545
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `ws://127.0.0.1:8545` | WebSocket URL of the Ethereum node |

### Get Latest Block Logs

Monitor the latest block and validate `eth_getLogs` results against the block's `receiptsRoot`.

```bash
./build/bin/rpc_int latest-block-logs --url http://localhost:8545
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `http://127.0.0.1:8545` | HTTP URL of the Ethereum node |
| `--interval` | `0.1` | Sleep interval between queries in seconds |

### Subscribe and Listen for Notifications

Subscribe to `newHeads` and USDT Transfer logs via WebSocket.

```bash
./build/bin/rpc_int subscriptions --url ws://127.0.0.1:8545
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `ws://127.0.0.1:8545` | WebSocket URL of the Ethereum node |

### GraphQL

Execute GraphQL queries or run GraphQL test suites downloaded from GitHub.

```bash
# Single query
./build/bin/rpc_int graphql --http-url http://127.0.0.1:8545/graphql --query '{block{number}}'

# Run test suite from GitHub
./build/bin/rpc_int graphql --http-url http://127.0.0.1:8545/graphql --tests-url https://api.github.com/repos/.../git/trees/...
```

| Flag | Default | Description |
|------|---------|-------------|
| `--http-url` | `http://127.0.0.1:8545/graphql` | GraphQL endpoint URL |
| `--query` | | GraphQL query string (mutually exclusive with `--tests-url`) |
| `--tests-url` | | GitHub tree URL with test files (mutually exclusive with `--query`) |
| `--stop-at-first-error` | `false` | Stop at first test error |
| `--test-number` | `-1` | Run only the test at this index (0-based) |

### Replay Engine API Requests

Replay JSON-RPC Engine API requests extracted from log files.

```bash
./build/bin/rpc_int replay-request --path /path/to/logs --url http://localhost:8551 --jwt /path/to/jwt.hex
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `http://localhost:8551` | HTTP URL of Engine API endpoint |
| `--method` | `engine_newPayloadV3` | JSON-RPC method to replay |
| `--index` | `1` | Ordinal index of method occurrence (-1 for all) |
| `--jwt` | `$HOME/prysm/jwt.hex` | Path to JWT secret file |
| `--path` | platform-specific | Path to Engine API log directory |
| `--pretend` | `false` | Dry run: do not send any HTTP request |
| `-v, --verbose` | `false` | Print verbose output |

### Replay Transactions

Scan blocks and compare trace responses between two servers.

```bash
./build/bin/rpc_int replay-tx --start 1000000:0 --method 0
```

| Flag | Default | Description |
|------|---------|-------------|
| `--start` | `0:0` | Starting point as `block:tx` |
| `-c, --continue` | `false` | Continue scanning, don't stop at first diff |
| `-n, --number` | `0` | Max number of failed txs before stopping |
| `-m, --method` | `0` | 0: `trace_replayTransaction`, 1: `debug_traceTransaction` |

### Scan Block Receipts

Verify receipts root integrity by computing the MPT root from actual receipts and comparing against the block header.

```bash
# Scan a specific block range
./build/bin/rpc_int scan-block-receipts --url http://localhost:8545 --start-block 100 --end-block 200

# Continuously monitor latest blocks
./build/bin/rpc_int scan-block-receipts --url http://localhost:8545
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | `http://127.0.0.1:8545` | HTTP URL of the Ethereum node |
| `--start-block` | `-1` | Starting block number (inclusive) |
| `--end-block` | `-1` | Ending block number (inclusive) |
| `--beyond-latest` | `false` | Scan next-after-latest blocks |
| `--stop-at-reorg` | `false` | Stop at first chain reorg |
| `--interval` | `0.1` | Sleep interval between queries in seconds |

## Development

### Run Go Tests
```bash
make test
```

### Lint
```bash
make lint
```

## Legacy Python Tools

The previous Python-based implementation is still available under `src/rpctests/`.

<details>
<summary>Python setup and tools</summary>

### Prerequisites

* [Python](https://www.python.org/) >= 3.10
* [`Vegeta`](https://github.com/tsenart/vegeta) >= 12.8.4 (for performance testing only)
* [`json-diff`](https://www.npmjs.com/package/json-diff) (via npm, for integration testing)
* [`python3-jsonpatch`](https://python-json-patch.readthedocs.io/) >= 1.32 (for integration testing)

### Create and activate Python virtual environment

Create your local virtual environment:
```bash
python3 -m venv .venv
```

Activate it:
```bash
source .venv/bin/activate        # Linux/macOS
```
```bash
.\.venv\Scripts\activate         # Windows
```

After you've updated to the latest code with `git pull`, update the dependencies:
```bash
pip3 install -r requirements.txt
```

### Python Standalone Tools

```bash
cd src
```

| Tool | Command |
|------|---------|
| Get Latest/Safe/Finalized Blocks | `python3 -m rpctests.block_by_number` |
| Find Empty Blocks | `python3 -m rpctests.empty_blocks` |
| Query Filter Changes | `python3 -m rpctests.filter_changes` |
| Get Latest Block Logs | `python3 -m rpctests.latest_block_logs` |
| GraphQL | `python3 -m rpctests.graphql` |
| Replay Request | `python3 -m rpctests.replay_request` |
| Replay Tx | `python3 -m rpctests.replay_tx` |
| Scan Block Receipts | `python3 -m rpctests.scan_block_receipts` |
| Send Raw Transaction Sync | `python3 -m rpctests.send_raw_transaction_sync` |
| Subscribe and Call Fee History | `python3 -m rpctests.subscribe_and_call_fee_history` |
| Subscribe and Check Receipts | `python3 -m rpctests.subscribe_and_check_receipts` |
| Subscriptions | `python3 -m rpctests.subscriptions` |

### Python Unit Tests
```bash
pytest
```

</details>

### Explicit Parity trace comparisons against Geth

A fixture can opt in to `"referenceMapping": "trace-call-flat-v1"`. This maps
`trace_call(call, ["trace"], block, overrides)` to Geth's
`debug_traceCall(call, block, {tracer: "flatCallTracer", tracerConfig:
{convertParityErrors: true}, stateOverrides: overrides})`. The APIs are not
identical renames. Live mapped cases require `-L`, use the same pinned block
number, and verify that both nodes return the same block hash.

Each case must pass **both** checks: its complete native response matches the
committed fixture, and its projected call frames match Geth. The projection
compares call actions, successful results, errors, ordered trace addresses and
subtrace counts. It removes Geth's empty transaction/block metadata and omits
root `action.gas` / `result.gasUsed`: Parity reports execution gas while Geth's
root includes intrinsic gas and receipt refund accounting. It also omits results
on failed frames, which Geth retains for REVERT and Parity omits; root return data
is still compared through the native `output` envelope. The complete native
fixture independently checks native gas values, null fields and envelopes.

Version 1 supports call frames only, including CALL, CALLCODE, DELEGATECALL and
STATICCALL. CREATE, SELFDESTRUCT, VM traces, state diffs and transaction/block
replay mappings remain outside its coverage. Unsupported mapping versions,
unpinned comparisons and JSON-RPC errors fail rather than count as parity.
Mapped cases always retain requests, raw responses, projections, block hashes
and both assertion outcomes in `*-mapping.json`, including successful cases.
No error suppression or fixture ignore-fields apply to these strict checks.

The first ten cases are `mainnet/trace_call/test_30.json` through `test_39.json`.
They cover success, return data, root/child reverts, invalid opcodes, nested calls,
static/delegate/callcode calls and precompile filtering. Run them with:

```sh
./build/bin/rpc_int --pruned -H NETHERMIND_HOST -p 8545 \
  -e http://GETH_HOST:8545 -A trace_call -L -c -f -M 0
```


`debug_traceCall/test_133.json`–`test_136.json` exercise `txIndex` 0 and 1 with callTracer and opcode traces. Their probe reads the real block fee recipient's balance; only the synthetic caller and probe contract are overridden. Run them with `-L` and a live Geth reference so both clients use the same recent block. Recorded responses capture the fixture-generation block and are not timeless balance assertions. Local client regressions separately cover multi-transaction state changes, fee charging, override ordering, invalid quantities, empty/genesis blocks and log offsets. An index1 case needs a nonempty block to exercise prefix replay; inspect the saved block/response evidence when reporting that coverage.

`debug_traceCall/test_137.json`–`test_139.json` compare JavaScript tracer block context with a block-number override, with `txIndex` omitted, zero, and one. The expected block and zero gas price are deterministic; use `-L` to replay the indexed calls against a recent block on pruned nodes.
