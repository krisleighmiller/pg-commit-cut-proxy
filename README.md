# pg-commit-cut-proxy

Deterministic wire-protocol TCP proxy for testing PostgreSQL ambiguous commits and network partition recovery.

Zero runtime dependencies (pure Node.js `node:net`).

---

## Why this exists

Testing ambiguous database commits is notoriously difficult in distributed systems, transactional outbox workers, two-phase commits, and data-storage reconcilers.

When a client issues `COMMIT` over a network connection, two distinct failure modes occur during a partition:

1. **The commit landed, but the response was lost:** The database server processed and committed the transaction, but the network connection died before the server's acknowledgment packet (`CommandComplete` / `ReadyForQuery`) reached the client. The client experiences an abrupt connection termination and has no way of knowing whether the transaction committed or rolled back.
2. **The commit was never delivered:** The connection dropped before the database server received `COMMIT`. The server automatically rolled back when the socket closed.

Most test suites attempt to simulate these conditions using flaky `sleep()` timeouts, mock drivers that bypass real network behavior, or protocol-blind network proxies (such as Toxiproxy) that drop random byte streams without knowing which query is in flight.

`CutProxy` solves this deterministically. It sits between your test writer and PostgreSQL, decodes the PostgreSQL frontend wire protocol frame by frame, intercepts the exact simple query frame (`0x51`, 'Q') whose SQL payload is `COMMIT`, and severs the client connection.

No sleeps, no timing probabilities. The cut is synchronized on the wire protocol `COMMIT` frame itself.

---

## How it works

```
[ Client / Driver ]
        │
        ▼ (Port: proxy.port)
┌────────────────────────────────────────────────────────┐
│                      CutProxy                          │
│                                                        │
│  Parses wire protocol frames:                          │
│  - Negotiates plaintext startup (handles SSLRequest)   │
│  - Bounds per-frame lengths & unparsed buffers         │
│  - Passes normal queries (BEGIN, SELECT, etc.) through │
│  - Intercepts standalone Simple Query 'Q' "COMMIT"     │
└────────────────────────────────────────────────────────┘
        │
        ▼ (Port: upstreamPort)
[ Real PostgreSQL Server ]
```

### Modes and Outcomes

* **`after_forward` (Ambiguous Commit):**  
  The `COMMIT` frame is delivered to PostgreSQL, and the client socket is immediately severed before any server response can return. The client experiences an abrupt disconnection. Simultaneously, the proxy monitors PostgreSQL's upstream response cycle until both `CommandComplete 'COMMIT'` and `ReadyForQuery 'I'` are received, establishing authoritative server settlement (`proxy.serverSettled` confirms `{ status: "committed", tag: "COMMIT", completedCycle: true }`).
* **`before_forward` (Lost Commit):**  
  Both connections are severed immediately without forwarding the `COMMIT` frame. PostgreSQL never receives `COMMIT` and automatically rolls back when the connection drops. `proxy.serverSettled` confirms `{ status: "rolled_back" }`.

---

## Multiple Cuts Accounting

A single `CutProxy` instance accurately tracks multiple cuts across sequential connections and retries:

* **`proxy.cuts`**: Array of all `CutRecord` entries executed by this proxy instance.
* **`proxy.waitForCut(index)`**: Await cut `#index` (0-indexed) if not yet triggered.
* **`proxy.stop(gracePeriodMs?)`**: Gracefully coordinates shutdown by waiting for **all** pending in-flight cuts to settle before closing upstream sockets.

---

## Scope & Limitations

* **Simple Query Protocol Only:** Matches standalone Simple Query (`'Q'`) frames containing `COMMIT`, `COMMIT;`, `COMMIT WORK`, or `COMMIT TRANSACTION` (case-insensitive, trailing semicolons and whitespace stripped).
* **No Extended Query Protocol:** Does not parse Extended Query Protocol frames (`Parse` `'P'`, `Bind` `'B'`, `Execute` `'E'`, `Sync` `'S'`). Most Node.js drivers (such as `pg` / `node-postgres`) use Simple Query Protocol for `client.query("COMMIT")` or transaction blocks.
* **No Multi-Statement Splitting:** Does not parse or split compound multi-statement frames (e.g. `COMMIT; SELECT 1`).
* **Unencrypted Plaintext Only:** The proxy must inspect plaintext SQL frames. It handles PostgreSQL startup negotiation by relaying or responding with `'N'` (SSL refused) so standard drivers fall back to plaintext. It cannot inspect TLS-encrypted sessions.
* **Client Disconnection:** The client socket is severed immediately (using `resetAndDestroy()` where supported on Node 18.3+, or abrupt teardown). Depending on OS kernel TCP buffers and driver handling, the client surfaces this as `ECONNRESET`, `EPIPE`, or an unexpected connection termination.

---

## Node.js Compatibility

* **Production Runtime:** Node.js `>= 18.0.0`. Pure `node:net` with zero runtime dependencies.
* **Development & Test Suite:** Node.js `>= 20.0.0` or `>= 22.0.0` (required by modern Vitest 3/5 and Vite toolchains).

---

## Installation

```bash
npm install --save-dev pg-commit-cut-proxy
```

---

## Usage Example

### Testing Idempotent Replay with Vitest / Jest and `pg`

```typescript
import { describe, it, expect } from "vitest";
import { Client } from "pg";
import { CutProxy } from "pg-commit-cut-proxy";

describe("Ambiguous Commit Recovery", () => {
  it("recovers safely when COMMIT lands but client connection drops", async () => {
    // 1. Start the proxy targeting your test PostgreSQL database
    const proxy = await CutProxy.start({
      mode: "after_forward",
      upstreamPort: 5432,
    });

    try {
      // 2. Connect client through the proxy port instead of Postgres directly
      const client = new Client({
        host: "127.0.0.1",
        port: proxy.port,
        user: "postgres",
        database: "testdb",
      });
      client.on("error", () => {}); // Handle expected connection drop
      await client.connect();

      await client.query("BEGIN");
      await client.query("INSERT INTO ledger (id, amount) VALUES ('tx-123', 100)");

      // 3. Issue COMMIT: proxy forwards it, then severs the client socket
      const commitPromise = client.query("COMMIT");

      // Await deterministic client-side severance
      await proxy.commitCut;

      // The client query must fail due to connection termination
      await expect(commitPromise).rejects.toThrow();

      // Confirm PostgreSQL processed and committed the full query cycle
      const outcome = await proxy.serverSettled;
      expect(outcome.status).toBe("committed");
      expect(outcome.completedCycle).toBe(true);
    } finally {
      await proxy.stop();
    }

    // 4. Verify from a fresh direct connection that the row committed
    const verifyClient = new Client({
      host: "127.0.0.1",
      port: 5432,
      user: "postgres",
      database: "testdb",
    });
    await verifyClient.connect();

    const res = await verifyClient.query("SELECT * FROM ledger WHERE id = 'tx-123'");
    expect(res.rows.length).toBe(1);

    await verifyClient.end();
  });

  it("handles clean rollback when COMMIT is cut before delivery", async () => {
    const proxy = await CutProxy.start({
      mode: "before_forward",
      upstreamPort: 5432,
    });

    try {
      const client = new Client({
        host: "127.0.0.1",
        port: proxy.port,
        user: "postgres",
        database: "testdb",
      });
      client.on("error", () => {});
      await client.connect();

      await client.query("BEGIN");
      await client.query("INSERT INTO ledger (id, amount) VALUES ('tx-456', 200)");

      const commitPromise = client.query("COMMIT");
      await proxy.commitCut;
      await expect(commitPromise).rejects.toThrow();

      const outcome = await proxy.serverSettled;
      expect(outcome.status).toBe("rolled_back");
    } finally {
      await proxy.stop();
    }

    // 4. Verify from a fresh connection that the row was rolled back
    const verifyClient = new Client({
      host: "127.0.0.1",
      port: 5432,
      user: "postgres",
      database: "testdb",
    });
    await verifyClient.connect();

    const res = await verifyClient.query("SELECT * FROM ledger WHERE id = 'tx-456'");
    expect(res.rows.length).toBe(0);

    await verifyClient.end();
  });
});
```

---

## API Reference

### `CutProxy.start(modeOrOptions, upstreamPort?, upstreamHost?)`

Creates and starts a proxy listener.

* **Overload 1:** `CutProxy.start(mode: "after_forward" | "before_forward", upstreamPort?: number, upstreamHost?: string)`
* **Overload 2:** `CutProxy.start(options: CutProxyOptions)`

#### `CutProxyOptions`

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `mode` | `"after_forward" \| "before_forward"` | *Required* | Whether to deliver `COMMIT` before cutting client, or drop immediately. |
| `upstreamPort` | `number` | `5432` | Port of the target PostgreSQL server. |
| `upstreamHost` | `string` | `"127.0.0.1"` | Host of the target PostgreSQL server. |
| `listenHost` | `string` | `"127.0.0.1"` | Local network interface for the proxy listener. |
| `listenPort` | `number` | `0` | Port to bind proxy to (`0` assigns an ephemeral free port). |
| `maxFrameSize` | `number` | `16777216` | Maximum allowed individual client frame length (16MB). |
| `maxBufferSize` | `number` | `67108864` | Maximum allowed unparsed buffer accumulation (64MB). |
| `sslMode` | `"forward" \| "refuse"` | `"forward"` | Whether to forward `SSLRequest` upstream or refuse directly with `'N'`. |

### Properties & Methods

* **`proxy.port`**: `number`  
  The assigned local port the proxy is listening on.
* **`proxy.cuts`**: `readonly CutRecord[]`  
  All cuts executed by this proxy instance in chronological order.
* **`proxy.cut(index)`**: `CutRecord | undefined`  
  Retrieves an executed cut record by index.
* **`proxy.waitForCut(index)`**: `Promise<CutRecord>`  
  Returns a promise for cut `#index`, resolving when that cut is triggered.
* **`proxy.commitCut`**: `Promise<void>`  
  Resolves the exact instant the first client socket cut is executed. (Alias: `proxy.commitSeen`).
* **`proxy.serverSettled`**: `Promise<ServerCommitOutcome>`  
  Resolves with the server-side outcome of the first cut:
  * `{ status: "committed", tag: "COMMIT", completedCycle: true }` in `after_forward` when PostgreSQL acknowledges the commit.
  * `{ status: "rolled_back" }` in `before_forward`.
  * `{ status: "refused", error: string }` if upstream PostgreSQL was unreachable.
  * `{ status: "error", error: string }` if PostgreSQL returned an `ErrorResponse`.
* **`proxy.cutCount`**: `number`  
  Total number of times a `COMMIT` frame was intercepted and cut.
* **`proxy.stop(gracePeriodMs?)`**: `Promise<void>`  
  Closes the proxy server and terminates active sockets. If any `after_forward` commits are in flight, waits up to `gracePeriodMs` (default 2000ms) for all pending cuts to settle before closing upstream.

---

## License

MIT (c) 2026 Kris Leigh Miller
