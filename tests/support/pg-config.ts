import type { ClientConfig } from "pg";
import { parse, toClientConfig } from "pg-connection-string";

export type LivePgConfig = ClientConfig & { host: string; port: number };

/** Normalize once, before overriding the endpoint for a proxied client. */
export function livePgConfig(env: NodeJS.ProcessEnv): LivePgConfig {
  try {
    const parsed = env.TEST_PG_URL ? parse(env.TEST_PG_URL) : undefined;
    const port = Number(parsed ? parsed.port || env.PGPORT || 5432 : env.TEST_PG_PORT ?? 55432);
    if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error();
    const config = parsed ? toClientConfig(parsed) : {
      host: env.TEST_PG_HOST ?? "127.0.0.1",
      user: env.TEST_PG_USER ?? "postgres",
      password: env.TEST_PG_PASSWORD ?? "postgres",
      database: env.TEST_PG_DATABASE ?? "postgres",
    };
    // Query parameters are preserved by pg-connection-string. Never let a URL
    // reintroduce connectionString and override the proxy endpoint later.
    delete config.connectionString;
    const host = config.host || env.PGHOST || "localhost";
    if (host.startsWith("/")) throw new Error(); // CutProxy has a TCP upstream.
    if (config.ssl) throw new Error(); // Do not silently downgrade required TLS.
    return { ...config, host, port, ssl: false, connectionTimeoutMillis: 3000 };
  } catch {
    // Parser errors may contain credentials, URL query values, or file paths.
    throw new Error("Invalid PostgreSQL test configuration. Use a TCP endpoint, a port from 1 to 65535, and plaintext (sslmode=disable). Configure TEST_PG_URL or TEST_PG_HOST / TEST_PG_PORT.");
  }
}

export function proxiedPgConfig(config: ClientConfig, port: number): ClientConfig {
  const { connectionString: _connectionString, ...explicit } = config;
  return { ...explicit, host: "127.0.0.1", port };
}

export function setupFailure(): Error {
  // Deliberately omit raw driver errors and config: either can contain secrets.
  return new Error(
    "PostgreSQL integration setup failed. A reachable plaintext PostgreSQL instance with CREATE TABLE permission is required. " +
    "Start a test container (e.g. docker run -p 55432:5432 -e POSTGRES_PASSWORD=postgres postgres:16) " +
    "or configure TEST_PG_URL / TEST_PG_PORT. Connection details and driver error text are omitted to protect credentials."
  );
}
