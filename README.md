# pg-commit-cut-proxy

Deterministic wire-protocol TCP proxy for testing PostgreSQL ambiguous commits and network partition recovery.

Zero dependencies (pure Node.js `node:net`).

---

## Why this exists

Testing ambiguous database commits is notoriously difficult in distributed systems, transactional outbox workers, two-phase commits, and data-storage reconcilers.

When a client issues `COMMIT` over a network connection, two distinct failure modes occur during a partition:

1. **The commit landed, but the response was lost:** The database server processed and committed the transaction, but the network connection died before the server's acknowledgment packet (`CommandComplete` / `ReadyForQuery`) reached the client. The client receives a connection error (`ECONNRESET`) and has no way of knowing whether the transaction committed or rolled back.
2. **The commit was never delivered:** The connection dropped before the database server received `COMMIT`. The server automatically rolled back when the socket closed.

Most test suites attempt to simulate these conditions using flaky `sleep()` timeouts, mock drivers that bypass real network behavior, or protocol-blind network proxies (such as Toxiproxy) that drop random packets without knowing which query is in flight.

`CutProxy` solves this deterministically. It sits between your test writer and PostgreSQL, decodes the PostgreSQL frontend wire protocol frame by frame, intercepts the exact simple query frame (`0x51`, 'Q') whose SQL payload is `COMMIT`, and cuts the connection instantly.

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
│  - Startup packet forwarded transparently              │
│  - Normal queries (BEGIN, SELECT, INSERT) forwarded    │
│  - Intercepts 'Q' frame matching "COMMIT"              │
└────────────────────────────────────────────────────────┘
        │
        ▼ (Port: upstreamPort)
[ Real PostgreSQL Server ]
```

### Modes

* **`after_forward` (Ambiguous Commit):**  
  The `COMMIT` frame is forwarded to PostgreSQL and the upstream socket is gracefully shut down with `FIN` so the server commits. Simultaneously, the client-facing socket is immediately destroyed (`RST`) before any response packet can return. The transaction **lands** in the database, but the client experiences an abrupt disconnection.
* **`before_forward` (Lost Commit):**  
  Both sockets are destroyed immediately without forwarding the `COMMIT` frame. PostgreSQL never sees the `COMMIT` and automatically rolls back the transaction when the connection closes.

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
      await client.connect();

      await client.query("BEGIN");
      await client.query("INSERT INTO ledger (id, amount) VALUES ('tx-123', 100)");

      // 3. Issue COMMIT: the proxy forwards it, then severs the client socket
      const commitPromise = client.query("COMMIT");

      // Await deterministic cut synchronization
      await proxy.commitSeen;

      // The client query must fail due to connection termination
      await expect(commitPromise).rejects.toThrow();
    } finally {
      await proxy.stop();
    }

    // 4. Verify from a fresh direct connection that the transaction committed
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
      await client.connect();

      await client.query("BEGIN");
      await client.query("INSERT INTO ledger (id, amount) VALUES ('tx-456', 200)");

      const commitPromise = client.query("COMMIT");
      await proxy.commitSeen;
      await expect(commitPromise).rejects.toThrow();
    } finally {
      await proxy.stop();
    }

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

### Properties

* **`proxy.port`**: `number`  
  The assigned local port the proxy is listening on.
* **`proxy.commitSeen`**: `Promise<void>`  
  Resolves the exact instant the `COMMIT` frame has been intercepted and the cut has been executed.
* **`proxy.cutCount`**: `number`  
  Number of times a `COMMIT` frame was intercepted and cut.
* **`proxy.stop()`**: `Promise<void>`  
  Closes the proxy server and terminates all active client and upstream sockets immediately.

---

## Supported Queries

Matches simple query protocol (`'Q'`) frames containing:
* `COMMIT`
* `COMMIT;`
* `COMMIT WORK`
* `COMMIT TRANSACTION`

Case-insensitive with trailing whitespace and semicolons automatically stripped.

---

## License

MIT (c) 2026 Kris Leigh Miller
