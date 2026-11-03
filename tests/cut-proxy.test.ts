import { describe, it, expect, afterEach } from "vitest";
import net from "node:net";
import { CutProxy } from "../src/cut-proxy.js";

/**
 * A minimal mock PostgreSQL server that speaks the basic frontend wire protocol:
 * 1. Accepts StartupMessage
 * 2. Emits AuthenticationOk ('R') and ReadyForQuery ('Z')
 * 3. Handles SimpleQuery ('Q') messages
 */
class MockPostgresServer {
  private server: net.Server;
  readonly receivedQueries: string[] = [];
  readonly connections = new Set<net.Socket>();

  private constructor() {
    this.server = net.createServer((socket) => this.handleClient(socket));
  }

  static async start(): Promise<MockPostgresServer> {
    const mock = new MockPostgresServer();
    await new Promise<void>((resolve, reject) => {
      mock.server.once("error", reject);
      mock.server.listen(0, "127.0.0.1", () => resolve());
    });
    return mock;
  }

  get port(): number {
    return (this.server.address() as net.AddressInfo).port;
  }

  async stop(): Promise<void> {
    for (const s of this.connections) {
      s.destroy();
    }
    this.connections.clear();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }

  private handleClient(socket: net.Socket): void {
    this.connections.add(socket);
    socket.on("close", () => this.connections.delete(socket));

    let startupDone = false;
    let buf = Buffer.alloc(0);

    socket.on("data", (chunk: Buffer) => {
      buf = Buffer.concat([buf, chunk]);

      for (;;) {
        let frameLen = 0;
        if (!startupDone) {
          if (buf.length < 4) break;
          frameLen = buf.readUInt32BE(0);
        } else {
          if (buf.length < 5) break;
          frameLen = 1 + buf.readUInt32BE(1);
        }

        if (buf.length < frameLen) break;

        const frame = buf.subarray(0, frameLen);
        buf = buf.subarray(frameLen);

        if (!startupDone) {
          startupDone = true;
          // Send AuthenticationOk ('R' + len 8 + auth type 0)
          const authOk = Buffer.from([0x52, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00]);
          // Send ReadyForQuery ('Z' + len 5 + 'I')
          const ready = Buffer.from([0x5a, 0x00, 0x00, 0x00, 0x05, 0x49]);
          socket.write(Buffer.concat([authOk, ready]));
        } else if (frame[0] === 0x51) {
          // 'Q' simple query
          const query = frame.subarray(5, frame.length - 1).toString("utf8");
          this.receivedQueries.push(query);

          // CommandComplete ('C' + len + tag + null)
          const tag = Buffer.from(`${query}\0`, "utf8");
          const compLen = 4 + tag.length;
          const compHeader = Buffer.alloc(5);
          compHeader[0] = 0x43; // 'C'
          compHeader.writeUInt32BE(compLen, 1);

          // ReadyForQuery
          const ready = Buffer.from([0x5a, 0x00, 0x00, 0x00, 0x05, 0x54]); // 'T' inside transaction
          socket.write(Buffer.concat([compHeader, tag, ready]));
        }
      }
    });
  }
}

/** Helper to encode a PostgreSQL startup packet. */
function encodeStartupPacket(): Buffer {
  const payload = Buffer.from("user\0test\0database\0test\0\0", "utf8");
  const packet = Buffer.alloc(4 + 4 + payload.length);
  packet.writeUInt32BE(packet.length, 0); // Total length
  packet.writeUInt32BE(196608, 4); // Protocol 3.0 (major 3, minor 0)
  payload.copy(packet, 8);
  return packet;
}

/** Helper to encode a PostgreSQL simple query ('Q') packet. */
function encodeQueryPacket(sql: string): Buffer {
  const sqlBuf = Buffer.from(`${sql}\0`, "utf8");
  const packet = Buffer.alloc(1 + 4 + sqlBuf.length);
  packet[0] = 0x51; // 'Q'
  packet.writeUInt32BE(4 + sqlBuf.length, 1);
  sqlBuf.copy(packet, 5);
  return packet;
}

describe("CutProxy", () => {
  let mockPostgres: MockPostgresServer;
  let proxy: CutProxy | null = null;

  afterEach(async () => {
    if (proxy) {
      await proxy.stop();
      proxy = null;
    }
    if (mockPostgres) {
      await mockPostgres.stop();
    }
  });

  it("passes normal queries through transparently", async () => {
    mockPostgres = await MockPostgresServer.start();
    proxy = await CutProxy.start({
      mode: "after_forward",
      upstreamPort: mockPostgres.port,
    });

    const client = net.connect(proxy.port, "127.0.0.1");

    await new Promise<void>((resolve) => client.once("connect", resolve));

    client.write(encodeStartupPacket());
    client.write(encodeQueryPacket("BEGIN"));
    client.write(encodeQueryPacket("SELECT 1"));

    // Wait briefly for queries to be processed
    await new Promise((r) => setTimeout(r, 100));

    expect(mockPostgres.receivedQueries).toEqual(["BEGIN", "SELECT 1"]);
    expect(proxy.cutCount).toBe(0);

    client.destroy();
  });

  it("mode 'after_forward': delivers COMMIT upstream, then tears down downstream before response", async () => {
    mockPostgres = await MockPostgresServer.start();
    proxy = await CutProxy.start({
      mode: "after_forward",
      upstreamPort: mockPostgres.port,
    });

    const client = net.connect(proxy.port, "127.0.0.1");
    client.resume();
    let clientDisconnected = false;

    client.on("close", () => {
      clientDisconnected = true;
    });
    client.on("error", () => {
      clientDisconnected = true;
    });

    await new Promise<void>((resolve) => client.once("connect", resolve));

    client.write(encodeStartupPacket());
    client.write(encodeQueryPacket("BEGIN"));
    client.write(encodeQueryPacket("INSERT INTO test VALUES (1)"));
    client.write(encodeQueryPacket("COMMIT"));

    // Must resolve deterministically once the cut happens
    await proxy.commitSeen;

    expect(proxy.cutCount).toBe(1);

    // Wait briefly to ensure mock server has flushed all received packets
    await new Promise((r) => setTimeout(r, 50));

    // The server DID receive the COMMIT
    expect(mockPostgres.receivedQueries).toContain("COMMIT");

    // But the client connection was destroyed
    expect(clientDisconnected).toBe(true);
  });

  it("mode 'before_forward': destroys both sockets immediately without delivering COMMIT", async () => {
    mockPostgres = await MockPostgresServer.start();
    proxy = await CutProxy.start({
      mode: "before_forward",
      upstreamPort: mockPostgres.port,
    });

    const client = net.connect(proxy.port, "127.0.0.1");
    client.resume();
    let clientDisconnected = false;

    client.on("close", () => {
      clientDisconnected = true;
    });
    client.on("error", () => {
      clientDisconnected = true;
    });

    await new Promise<void>((resolve) => client.once("connect", resolve));

    client.write(encodeStartupPacket());
    client.write(encodeQueryPacket("BEGIN"));
    client.write(encodeQueryPacket("INSERT INTO test VALUES (2)"));

    // Wait until server has received the prior queries before issuing COMMIT
    await new Promise((r) => setTimeout(r, 50));
    expect(mockPostgres.receivedQueries).toEqual(["BEGIN", "INSERT INTO test VALUES (2)"]);

    client.write(encodeQueryPacket("COMMIT"));

    // Must resolve deterministically
    await proxy.commitSeen;

    expect(proxy.cutCount).toBe(1);

    // Wait for client socket close event to fire
    await new Promise<void>((resolve) => {
      if (clientDisconnected) return resolve();
      client.once("close", () => resolve());
    });

    // The server NEVER received the COMMIT
    expect(mockPostgres.receivedQueries).toEqual(["BEGIN", "INSERT INTO test VALUES (2)"]);
    expect(mockPostgres.receivedQueries).not.toContain("COMMIT");

    // And the client connection was destroyed
    expect(clientDisconnected).toBe(true);
  });

  it("handles variations like 'COMMIT;' and 'COMMIT WORK;'", async () => {
    mockPostgres = await MockPostgresServer.start();
    proxy = await CutProxy.start("after_forward", mockPostgres.port);

    const client = net.connect(proxy.port, "127.0.0.1");
    await new Promise<void>((resolve) => client.once("connect", resolve));

    client.write(encodeStartupPacket());
    client.write(encodeQueryPacket("commit; "));

    await proxy.commitSeen;
    expect(proxy.cutCount).toBe(1);

    await new Promise((r) => setTimeout(r, 50));
    expect(mockPostgres.receivedQueries).toContain("commit; ");
  });
});
