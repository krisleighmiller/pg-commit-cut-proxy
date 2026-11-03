import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { setTimeout as delay } from "node:timers/promises";
import pg from "pg";
import { CutProxy } from "../src/cut-proxy.js";
import { livePgConfig, proxiedPgConfig, setupFailure } from "./support/pg-config.js";

const { Client } = pg;

// Support custom PostgreSQL connection via environment variables
const PG_CONFIG = livePgConfig(process.env);
let fixtureCreated = false;

// Unique fixture table per test file execution to prevent cross-test interference
const FIXTURE_TABLE = `cut_proxy_fixture_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

const frontendMessage = (type: string, payload: Buffer | string = Buffer.alloc(0)) => {
  const body = typeof payload === "string" ? Buffer.from(payload) : payload;
  const frame = Buffer.alloc(body.length + 5);
  frame[0] = type.charCodeAt(0);
  frame.writeUInt32BE(body.length + 4, 1);
  body.copy(frame, 5);
  return frame;
};

const simpleQuery = (sql: string) => frontendMessage("Q", `${sql}\0`);

const parse = (sql: string) => {
  const parameterCount = Buffer.alloc(2);
  return frontendMessage("P", Buffer.concat([Buffer.from([0]), Buffer.from(`${sql}\0`), parameterCount]));
};

const functionCall = (oid: number) => {
  const payload = Buffer.alloc(10);
  payload.writeUInt32BE(oid, 0);
  // Zero argument-format codes, zero arguments, text result format.
  payload.writeUInt16BE(0, 4);
  payload.writeUInt16BE(0, 6);
  payload.writeUInt16BE(0, 8);
  return frontendMessage("F", payload);
};

const rawStream = (client: pg.Client) => (client as unknown as {
  connection: {
    stream: NodeJS.WritableStream;
    once(event: "readyForQuery", listener: () => void): void;
    removeAllListeners(event: string): void;
  };
}).connection;

describe("CutProxy Real PostgreSQL Integration", () => {
  beforeAll(async () => {
    let probeClient: pg.Client | undefined;
    try {
      probeClient = new Client(PG_CONFIG);
      await probeClient.connect();
      await probeClient.query("SELECT 1");
      await probeClient.query(`
        CREATE TABLE ${FIXTURE_TABLE} (
          id TEXT PRIMARY KEY,
          amount INT NOT NULL
        )
      `);
      fixtureCreated = true;
    } catch {
      throw setupFailure();
    } finally {
      await probeClient?.end().catch(() => {});
    }
  });

  afterAll(async () => {
    if (!fixtureCreated) return;
    const cleanupClient = new Client(PG_CONFIG);
    try {
      await cleanupClient.connect();
      await cleanupClient.query(`DROP TABLE IF EXISTS ${FIXTURE_TABLE}`);
    } finally {
      await cleanupClient.end().catch(() => {});
    }
  });

  beforeEach(async () => {
    const client = new Client(PG_CONFIG);
    try {
      await client.connect();
      await client.query(`DELETE FROM ${FIXTURE_TABLE}`);
    } finally {
      await client.end().catch(() => {});
    }
  });

  for (const mode of ["before_forward", "after_forward"] as const) {
    it(`${mode}: PostgreSQL rejects Unicode COMMIT lookalikes normally, without a proxy cut`, async () => {
      const proxy = await CutProxy.start({ mode, upstreamHost: PG_CONFIG.host, upstreamPort: PG_CONFIG.port });
      const direct = new Client(PG_CONFIG);
      const proxied = new Client(proxiedPgConfig(PG_CONFIG, proxy.port));
      try {
        await direct.connect();
        await proxied.connect();
        for (const sql of ["COMM\u0131T", "\u00a0COMMIT", "COMMIT\u00a0", "\u000bCOMMIT", "\ufeffCOMMIT", "COMMIT TRAN\u017fACTION", "COMMIT\u2028", "COMMIT\u2029"]) {
          await expect(direct.query(sql)).rejects.toMatchObject({ code: "42601" });
          await expect(proxied.query(sql)).rejects.toMatchObject({ code: "42601" });
        }
        expect((await proxied.query("SELECT 1 AS alive")).rows[0].alive).toBe(1);
        expect(proxy.cutCount).toBe(0);
        expect(proxy.cuts).toHaveLength(0);
      } finally {
        await Promise.all([direct.end(), proxied.end()]);
        await proxy.stop(0);
      }
    });
  }

  it("after_forward: delivers COMMIT to real PostgreSQL, cuts client, row is visible from fresh connection", async () => {
    const proxy = await CutProxy.start({
      mode: "after_forward",
      upstreamPort: PG_CONFIG.port,
      upstreamHost: PG_CONFIG.host,
    });

    try {
      const client = new Client(proxiedPgConfig(PG_CONFIG, proxy.port));
      client.on("error", () => {}); // Handle expected connection drop error

      await client.connect();
      await client.query("BEGIN");
      await client.query(`INSERT INTO ${FIXTURE_TABLE} (id, amount) VALUES ('tx-1', 100)`);

      // Issue COMMIT: proxy forwards it to PostgreSQL, then severs client connection
      const commitRejection = expect(client.query("COMMIT")).rejects.toThrow();

      // Await deterministic client-side cut
      await proxy.commitCut;

      // The client MUST experience a connection drop
      await commitRejection;

      // Verify server settlement confirms COMMIT landed and full query cycle completed
      const outcome = await proxy.serverSettled;
      expect(outcome.status).toBe("committed");
      expect(outcome.tag).toBe("COMMIT");
      expect(outcome.completedCycle).toBe(true);
    } finally {
      await proxy.stop();
    }

    // Verify through a completely fresh, independent connection to PostgreSQL
    const freshClient = new Client(PG_CONFIG);
    try {
      await freshClient.connect();
      const res = await freshClient.query(`SELECT * FROM ${FIXTURE_TABLE} WHERE id = 'tx-1'`);
      expect(res.rows.length).toBe(1);
      expect(res.rows[0].amount).toBe(100);
    } finally {
      await freshClient.end();
    }
  });

  it("before_forward: severs connection before COMMIT is delivered, row is NOT visible from fresh connection", async () => {
    const proxy = await CutProxy.start({
      mode: "before_forward",
      upstreamPort: PG_CONFIG.port,
      upstreamHost: PG_CONFIG.host,
    });

    try {
      const client = new Client(proxiedPgConfig(PG_CONFIG, proxy.port));
      client.on("error", () => {}); // Handle expected connection drop error

      await client.connect();
      await client.query("BEGIN");
      await client.query(`INSERT INTO ${FIXTURE_TABLE} (id, amount) VALUES ('tx-2', 200)`);

      // Issue COMMIT: proxy severs connection BEFORE forwarding COMMIT
      const commitRejection = expect(client.query("COMMIT")).rejects.toThrow();

      await proxy.commitCut;
      await commitRejection;

      const outcome = await proxy.serverSettled;
      expect(outcome.status).toBe("rolled_back");
    } finally {
      await proxy.stop();
    }

    // Verify through a fresh independent connection: row must NOT exist (rolled back)
    const freshClient = new Client(PG_CONFIG);
    try {
      await freshClient.connect();
      const res = await freshClient.query(`SELECT * FROM ${FIXTURE_TABLE} WHERE id = 'tx-2'`);
      expect(res.rows.length).toBe(0);
    } finally {
      await freshClient.end();
    }
  });

  it("before_forward: serverSettled waits for a running query to finish and release transaction locks", async () => {
    const proxy = await CutProxy.start({ mode: "before_forward", upstreamHost: PG_CONFIG.host, upstreamPort: PG_CONFIG.port });
    const client = new Client(proxiedPgConfig(PG_CONFIG, proxy.port));
    const observer = new Client(PG_CONFIG);
    const lockKey = 1_900_000_000 + Math.floor(Math.random() * 100_000_000);
    client.on("error", () => {});
    try {
      await client.connect();
      await observer.connect();
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock($1)", [lockKey]);

      rawStream(client).stream.write(Buffer.concat([
        simpleQuery("SELECT pg_sleep(0.4)"),
        simpleQuery("COMMIT"),
      ]));
      await proxy.commitCut;

      const earlySettlement = await Promise.race([
        proxy.serverSettled.then(() => true),
        delay(100).then(() => false),
      ]);
      expect(earlySettlement).toBe(false);
      expect((await observer.query("SELECT pg_try_advisory_lock($1) AS acquired", [lockKey])).rows[0].acquired).toBe(false);

      const stopping = proxy.stop(1000);
      expect((await proxy.serverSettled).status).toBe("rolled_back");
      await stopping;
      expect((await observer.query("SELECT pg_try_advisory_lock($1) AS acquired", [lockKey])).rows[0].acquired).toBe(true);
      await observer.query("SELECT pg_advisory_unlock($1)", [lockKey]);
    } finally {
      await client.end().catch(() => {});
      await observer.end().catch(() => {});
      await proxy.stop(0);
    }
  });

  it("after_forward: ordered extended-protocol recovery does not hide a later successful COMMIT", async () => {
    const proxy = await CutProxy.start({ mode: "after_forward", upstreamHost: PG_CONFIG.host, upstreamPort: PG_CONFIG.port });
    const client = new Client(proxiedPgConfig(PG_CONFIG, proxy.port));
    client.on("error", () => {});
    try {
      await client.connect();
      const connection = rawStream(client);
      // Drive this one sequence at wire level. node-postgres correctly treats
      // unsolicited ParseComplete/ErrorResponse messages as client misuse and
      // would otherwise close the test connection before recovery can be tested.
      connection.removeAllListeners("parseComplete");
      connection.removeAllListeners("errorMessage");
      connection.stream.write(Buffer.concat([
        // First extended segment errors and discards its Query through Sync.
        parse("SELECT FROM"),
        simpleQuery("SELECT 1"),
        frontendMessage("S"),
        // A later segment in the same pipeline remains executable and must keep
        // its own Query + Sync response cycles.
        parse("SELECT 2"),
        simpleQuery("SELECT 2"),
        frontendMessage("S"),
        simpleQuery("BEGIN"),
        simpleQuery(`INSERT INTO ${FIXTURE_TABLE} (id, amount) VALUES ('recovery-cycle', 301)`),
        simpleQuery("COMMIT"),
      ]));
      await proxy.commitCut;
      expect(await Promise.race([
        proxy.serverSettled,
        delay(1000).then(() => ({ status: "timeout" as const })),
      ])).toMatchObject({ status: "committed" });
    } finally {
      await client.end().catch(() => {});
      await proxy.stop(0);
    }
  });

  it("after_forward: a FunctionCall response is not mistaken for COMMIT completion", async () => {
    const oidClient = new Client(PG_CONFIG);
    await oidClient.connect();
    const oid = Number((await oidClient.query("SELECT 'pg_backend_pid()'::regprocedure::oid AS oid")).rows[0].oid);
    await oidClient.end();

    const proxy = await CutProxy.start({ mode: "after_forward", upstreamHost: PG_CONFIG.host, upstreamPort: PG_CONFIG.port });
    const client = new Client(proxiedPgConfig(PG_CONFIG, proxy.port));
    client.on("error", () => {});
    try {
      await client.connect();
      await client.query("BEGIN");
      await client.query(`INSERT INTO ${FIXTURE_TABLE} (id, amount) VALUES ('function-cycle', 302)`);
      rawStream(client).stream.write(functionCall(oid));
      void client.query("COMMIT").catch(() => {});
      await proxy.commitCut;
      expect(await proxy.serverSettled).toMatchObject({ status: "committed", tag: "COMMIT", completedCycle: true });

      const observer = new Client(PG_CONFIG);
      try {
        await observer.connect();
        expect((await observer.query(`SELECT amount FROM ${FIXTURE_TABLE} WHERE id = 'function-cycle'`)).rows[0].amount).toBe(302);
      } finally {
        await observer.end().catch(() => {});
      }
    } finally {
      await client.end().catch(() => {});
      await proxy.stop(0);
    }
  });
});
