# Archive-index probe tests

`eth_call` tests that stress a node's **historical state (archive) read path**. Each test reads a set of accounts and storage slots touched in some historical block, replaying them *as of* an older block — forcing many value-at-block-N lookups. The call returns a 4-word fingerprint; a matching response makes it highly likely the archive index resolved the full read set identically.

### Naming

```
test_archive-index-<baseBlock>-<queryBlock>.json
```

- `baseBlock` — block whose touched set was sourced (which accounts/slots to read).
- `queryBlock` — block at which the read set is replayed (`queryBlock <= baseBlock`).

## What a single request does

One `eth_call` at `queryBlock` with a `stateOverride`-injected driver contract. Returns a 4-word struct:

| Word | Meaning |
|------|---------|
| 0 | `accountFingerprint` — XOR of every account balance read |
| 1 | `storageFingerprint` — XOR of every storage slot value read |
| 2 | `nonZeroAccounts` — count of non-zero balances |
| 3 | `nonZeroSlots` — count of non-zero storage slots |

**Accounts.** The driver runs `BALANCE` on each sourced account, XOR-folding results into `accountFingerprint`.

**Storage.** Since `SLOAD` only reads the executing contract's own storage, each sourced contract's code is replaced (via `stateOverride`) with a slot-reader stub; the driver `STATICCALL`s into each one. Only code is replaced — historical storage is preserved, so every `SLOAD` resolves through the archive index at `queryBlock`.

The bytecode is injected as runtime code at a synthetic address, so EIP-170/3860 size caps don't apply and a full block's read set fits in one call.

## How the tests were generated

1. **Source the touched set.** Accounts and storage slots accessed in `baseBlock`, collected via `prestateTracer` (`debug_traceBlockByNumber`) when available, otherwise via `eth_createAccessList` against `baseBlock - 1`. `from`/`to` participants are always included.
2. **Cap and shuffle.** Shuffled (seeded by block number, so reproducible) and capped to fixed limits (~1000 accounts, ~6000 slots per call).
3. **Build the ladder.** The same read set is turned into one `eth_call` per query-depth rung, each at a different `queryBlock`.
4. **Capture the oracle.** Each request is sent to a reference archive node; the response is recorded verbatim as the expected `result`.
