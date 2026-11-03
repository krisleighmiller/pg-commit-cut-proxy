import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import pg from "pg";
import { CutProxy } from "../src/cut-proxy.js";

const { Client } = pg;

// Support custom PostgreSQL connection via environment variables
const PG_CONFIG = process.env.TEST_PG_URL
  ? { connectionString: process.env.TEST_PG_URL }
  : {
      host: process.env.TEST_PG_HOST ?? "127.0.0.1",
      port: Number(process.env.TEST_PG_PORT ?? 55432),
      user: process.env.TEST_PG_USER ?? "postgres",
      password: process.env.TEST_PG_PASSWORD ?? "postgres",
      database: process.env.TEST_PG_DATABASE ?? "postgres",
    };

// Unique fixture table per test file execution to prevent cross-test interference
const FIXTURE_TABLE = `cut_proxy_fixture_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

describe("CutProxy Real PostgreSQL Integration", () => {
  beforeAll(async () => {
    const probeClient = new Client(PG_CONFIG);
    try {
      await probeClient.connect();
      await probeClient.query("SELECT 1");
      await probeClient.query(`
        CREATE TABLE ${FIXTURE_TABLE} (
          id TEXT PRIMARY KEY,
          amount INT NOT NULL
        )
      `);
    } catch (err: any) {
      throw new Error(
        `PostgreSQL integration test requires a reachable PostgreSQL instance.\n` +
          `Failed to connect to ${JSON.stringify(PG_CONFIG)}: ${err.message}\n` +
          `Start a test container (e.g. docker run -p 55432:5432 -e POSTGRES_PASSWORD=postgres postgres:16) ` +
          `or configure TEST_PG_URL / TEST_PG_PORT.`
      );
    } finally {
      await probeClient.end().catch(() => {});
    }
  });

  afterAll(async () => {
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

  it("after_forward: delivers COMMIT to real PostgreSQL, cuts client, row is visible from fresh connection", async () => {
    const proxy = await CutProxy.start({
      mode: "after_forward",
      upstreamPort: "port" in PG_CONFIG ? PG_CONFIG.port : 5432,
      upstreamHost: "host" in PG_CONFIG ? PG_CONFIG.host : "127.0.0.1",
    });

    try {
      const client = new Client({
        ...PG_CONFIG,
        port: proxy.port,
        host: "127.0.0.1",
      });
      client.on("error", () => {}); // Handle expected connection drop error

      await client.connect();
      await client.query("BEGIN");
      await client.query(`INSERT INTO ${FIXTURE_TABLE} (id, amount) VALUES ('tx-1', 100)`);

      // Issue COMMIT: proxy forwards it to PostgreSQL, then severs client connection
      const commitPromise = client.query("COMMIT");

      // Await deterministic client-side cut
      await proxy.commitCut;

      // The client MUST experience a connection drop
      await expect(commitPromise).rejects.toThrow();

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
    await freshClient.connect();

    const res = await freshClient.query(`SELECT * FROM ${FIXTURE_TABLE} WHERE id = 'tx-1'`);
    expect(res.rows.length).toBe(1);
    expect(res.rows[0].amount).toBe(100);

    await freshClient.end();
  });

  it("before_forward: severs connection before COMMIT is delivered, row is NOT visible from fresh connection", async () => {
    const proxy = await CutProxy.start({
      mode: "before_forward",
      upstreamPort: "port" in PG_CONFIG ? PG_CONFIG.port : 5432,
      upstreamHost: "host" in PG_CONFIG ? PG_CONFIG.host : "127.0.0.1",
    });

    try {
      const client = new Client({
        ...PG_CONFIG,
        port: proxy.port,
        host: "127.0.0.1",
      });
      client.on("error", () => {}); // Handle expected connection drop error

      await client.connect();
      await client.query("BEGIN");
      await client.query(`INSERT INTO ${FIXTURE_TABLE} (id, amount) VALUES ('tx-2', 200)`);

      // Issue COMMIT: proxy severs connection BEFORE forwarding COMMIT
      const commitPromise = client.query("COMMIT");

      await proxy.commitCut;
      await expect(commitPromise).rejects.toThrow();

      const outcome = await proxy.serverSettled;
      expect(outcome.status).toBe("rolled_back");
    } finally {
      await proxy.stop();
    }

    // Verify through a fresh independent connection: row must NOT exist (rolled back)
    const freshClient = new Client(PG_CONFIG);
    await freshClient.connect();

    const res = await freshClient.query(`SELECT * FROM ${FIXTURE_TABLE} WHERE id = 'tx-2'`);
    expect(res.rows.length).toBe(0);

    await freshClient.end();
  });
});
