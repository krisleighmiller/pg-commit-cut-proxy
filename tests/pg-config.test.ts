import { afterEach, describe, expect, it } from "vitest";
import net from "node:net";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import pg from "pg";
import { CutProxy } from "../src/cut-proxy.js";
import { livePgConfig, proxiedPgConfig, setupFailure } from "./support/pg-config.js";

describe("live PostgreSQL configuration", () => {
  it("parses URL credentials, host, port, and driver options without retaining a connectionString", () => {
    const config = livePgConfig({ TEST_PG_URL: "postgres://a%40b:p%3Ass@localhost:61234/example?application_name=cut-test&sslmode=disable" });
    expect(config).toMatchObject({ user: "a@b", password: "p:ss", host: "localhost", port: 61234, database: "example", application_name: "cut-test", ssl: false });
    expect(config).not.toHaveProperty("connectionString");
    expect(proxiedPgConfig(config, 62345)).toMatchObject({ host: "127.0.0.1", port: 62345 });
  });

  it("uses explicit defaults for URL and environment configurations", () => {
    expect(livePgConfig({ TEST_PG_URL: "postgres://user:pass@localhost/db" })).toMatchObject({ host: "localhost", port: 5432 });
    expect(livePgConfig({})).toMatchObject({ host: "127.0.0.1", port: 55432, ssl: false });
    expect(livePgConfig({ TEST_PG_PORT: "61234", TEST_PG_HOST: "localhost" })).toMatchObject({ host: "localhost", port: 61234 });
  });

  it("fails safely for invalid URLs, invalid ports, encrypted or Unix-socket upstreams", () => {
    for (const env of [
      { TEST_PG_URL: "postgres://user:secret@[invalid/db" },
      { TEST_PG_PORT: "invalid-secret" },
      { TEST_PG_PORT: "5432junk" },
      { TEST_PG_PORT: "0" },
      { TEST_PG_URL: "postgres://user:secret@localhost/db?sslmode=verify-full" },
      { TEST_PG_HOST: "/var/run/postgresql" },
    ]) {
      expect(() => livePgConfig(env)).toThrow();
      try { livePgConfig(env); } catch (error) {
        expect(String(error)).not.toContain("secret");
        expect(error).not.toHaveProperty("cause");
      }
    }
  });

  it("does not include connection configuration or underlying error text in setup errors", () => {
    const error = setupFailure();
    expect(error.message).toContain("TEST_PG_URL");
    expect(error.message).not.toContain("postgres://");
    expect(error).not.toHaveProperty("cause");
  });

  it("keeps credentials and raw constructor errors out of actual failing live-suite output", async () => {
    const secret = "synthetic-credential-leak-sentinel";
    // Invalid negotiation fails in the pg constructor before any network access.
    const run = promisify(execFile);
    let output = "";
    try {
      await run(process.execPath, ["node_modules/vitest/vitest.mjs", "run", "tests/postgres-live.test.ts"], {
        env: { ...process.env, TEST_PG_URL: `postgres://user:${secret}@127.0.0.1:1/db?sslnegotiation=${secret}` },
        timeout: 8000,
      });
      throw new Error("The intentionally invalid live suite unexpectedly passed");
    } catch (error) {
      const failure = error as Error & { stdout?: string; stderr?: string; code?: number };
      expect(failure.code).toBe(1);
      output = (failure.stdout ?? "") + (failure.stderr ?? "");
    }
    expect(output).toContain("Connection details and driver error text are omitted");
    expect(output).not.toContain(secret);
    expect(output).not.toContain("postgres://");
    expect(output).not.toContain("Invalid sslnegotiation value");
  });

  let proxy: CutProxy | undefined;
  let server: net.Server | undefined;
  const sockets = new Set<net.Socket>();
  afterEach(async () => {
    await proxy?.stop(0);
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => server ? server.close(() => resolve()) : resolve());
  });

  it("connects a URL-configured pg client through proxy.port, not the URL's upstream port", async () => {
    let startupCount = 0;
    let queryCount = 0;
    server = net.createServer((socket) => {
      sockets.add(socket);
      socket.on("error", () => {});
      let started = false;
      socket.on("data", () => {
        if (!started) {
          started = true;
          startupCount++;
          socket.write(Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0, 0x5a, 0, 0, 0, 5, 0x49]));
        } else {
          queryCount++;
        }
      });
    });
    await new Promise<void>((resolve) => server!.listen(0, "127.0.0.1", resolve));
    const upstreamPort = (server.address() as net.AddressInfo).port;
    const config = livePgConfig({ TEST_PG_URL: `postgres://user:password@127.0.0.1:${upstreamPort}/db?sslmode=disable` });
    proxy = await CutProxy.start({ mode: "before_forward", upstreamHost: config.host, upstreamPort: config.port });
    const client = new pg.Client(proxiedPgConfig(config, proxy.port));
    client.on("error", () => {});
    try {
      await client.connect();
      const commit = expect(client.query("COMMIT")).rejects.toThrow();
      await proxy.commitCut;
      await commit;
      expect(startupCount).toBe(1);
      expect(queryCount).toBe(0);
      expect(proxy.cutCount).toBe(1);
      expect((await proxy.serverSettled).status).toBe("rolled_back");
    } finally {
      await client.end();
    }
  });
});
