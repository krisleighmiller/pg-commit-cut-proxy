import { describe, it, expect, afterEach } from "vitest";
import net from "node:net";
import { CutProxy } from "../src/cut-proxy.js";

/**
 * A mock PostgreSQL server that speaks the basic frontend wire protocol:
 * 1. Accepts SSLRequest (code 80877103) and replies 'N' (refused)
 * 2. Accepts StartupMessage
 * 3. Emits AuthenticationOk ('R') and ReadyForQuery ('Z')
 * 4. Handles SimpleQuery ('Q') messages
 */
class MockPostgresServer {
  private server: net.Server;
  readonly receivedQueries: string[] = [];
  readonly connections = new Set<net.Socket>();
  sslRequestsHandled = 0;

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
    socket.on("error", () => {});

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
          // Check for SSLRequest (len 8, code 80877103)
          if (frameLen === 8 && frame.readUInt32BE(4) === 80877103) {
            this.sslRequestsHandled += 1;
            // Reply with 'N' (SSL not supported)
            socket.write(Buffer.from([0x4e]));
            continue;
          }

          // Real StartupMessage
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
          // PostgreSQL returns canonical command tags, not the original SQL.
          const isCommit = /^\s*commit(?:\s+(?:work|transaction))?\s*;?\s*$/i.test(query);
          const tag = Buffer.from(`${isCommit ? "COMMIT" : query}\0`, "utf8");
          const compLen = 4 + tag.length;
          const compHeader = Buffer.alloc(5);
          compHeader[0] = 0x43; // 'C'
          compHeader.writeUInt32BE(compLen, 1);

          // ReadyForQuery ('Z' + len 5 + status byte)
          // If query is COMMIT, transaction status is 'I' (idle); otherwise 'T'
          const txStatus = isCommit ? 0x49 : 0x54;
          const ready = Buffer.from([0x5a, 0x00, 0x00, 0x00, 0x05, txStatus]);
          socket.write(Buffer.concat([compHeader, tag, ready]));
        }
      }
    });
  }
}

/** Helper to encode an SSLRequest packet (8 bytes, code 80877103). */
function encodeSSLRequest(): Buffer {
  const packet = Buffer.alloc(8);
  packet.writeUInt32BE(8, 0);
  packet.writeUInt32BE(80877103, 4);
  return packet;
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

describe("CutProxy Wire Protocol Tests", () => {
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
    client.resume();

    await new Promise<void>((resolve) => client.once("connect", resolve));

    client.write(encodeStartupPacket());
    client.write(encodeQueryPacket("BEGIN"));
    client.write(encodeQueryPacket("SELECT 1"));

    await new Promise((r) => setTimeout(r, 100));

    expect(mockPostgres.receivedQueries).toEqual(["BEGIN", "SELECT 1"]);
    expect(proxy.cutCount).toBe(0);

    client.destroy();
  });

  it("handles SSL negotiation: forwards SSLRequest, relays 'N' refusal, and transparently handles subsequent StartupMessage", async () => {
    mockPostgres = await MockPostgresServer.start();
    proxy = await CutProxy.start({
      mode: "after_forward",
      upstreamPort: mockPostgres.port,
    });

    const client = net.connect(proxy.port, "127.0.0.1");
    let sslRefusalReceived = false;

    client.on("data", (chunk: Buffer) => {
      if (chunk[0] === 0x4e) {
        sslRefusalReceived = true;
      }
    });

    await new Promise<void>((resolve) => client.once("connect", resolve));

    client.write(encodeSSLRequest());

    await new Promise((r) => setTimeout(r, 50));
    expect(sslRefusalReceived).toBe(true);
    expect(mockPostgres.sslRequestsHandled).toBe(1);

    client.write(encodeStartupPacket());
    client.write(encodeQueryPacket("BEGIN"));
    client.write(encodeQueryPacket("SELECT 2"));

    await new Promise((r) => setTimeout(r, 100));
    expect(mockPostgres.receivedQueries).toEqual(["BEGIN", "SELECT 2"]);

    client.destroy();
  });

  it("mode 'after_forward': delivers COMMIT upstream, severs client, and confirms full response cycle settlement", async () => {
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

    await proxy.commitCut;
    expect(proxy.cutCount).toBe(1);

    const outcome = await proxy.serverSettled;
    expect(outcome.status).toBe("committed");
    expect(outcome.tag).toBe("COMMIT");
    expect(outcome.completedCycle).toBe(true);

    expect(clientDisconnected).toBe(true);
    expect(mockPostgres.receivedQueries).toContain("COMMIT");
  });

  it("mode 'before_forward': severs both sockets immediately without delivering COMMIT", async () => {
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

    await new Promise((r) => setTimeout(r, 50));
    expect(mockPostgres.receivedQueries).toEqual(["BEGIN", "INSERT INTO test VALUES (2)"]);

    client.write(encodeQueryPacket("COMMIT"));

    await proxy.commitCut;
    expect(proxy.cutCount).toBe(1);

    const outcome = await proxy.serverSettled;
    expect(outcome.status).toBe("rolled_back");

    await new Promise<void>((resolve) => {
      if (clientDisconnected) return resolve();
      client.once("close", () => resolve());
    });
    expect(clientDisconnected).toBe(true);

    expect(mockPostgres.receivedQueries).not.toContain("COMMIT");
  });

  it("reliably accounts for multiple cuts on a single proxy instance", async () => {
    mockPostgres = await MockPostgresServer.start();
    proxy = await CutProxy.start({
      mode: "after_forward",
      upstreamPort: mockPostgres.port,
    });

    // Client 1 connects and commits
    const client1 = net.connect(proxy.port, "127.0.0.1");
    client1.resume();
    client1.on("error", () => {});
    await new Promise<void>((r) => client1.once("connect", r));

    client1.write(encodeStartupPacket());
    client1.write(encodeQueryPacket("BEGIN"));
    client1.write(encodeQueryPacket("INSERT INTO test VALUES (10)"));
    client1.write(encodeQueryPacket("COMMIT"));

    // Wait for first cut to be processed
    await proxy.commitCut;
    expect(proxy.cutCount).toBe(1);
    expect(proxy.cuts.length).toBe(1);

    const outcome1 = await proxy.cuts[0].serverSettled;
    expect(outcome1.status).toBe("committed");

    // Client 2 connects and commits through the same proxy instance
    const client2 = net.connect(proxy.port, "127.0.0.1");
    client2.resume();
    client2.on("error", () => {});
    await new Promise<void>((r) => client2.once("connect", r));

    client2.write(encodeStartupPacket());
    client2.write(encodeQueryPacket("BEGIN"));
    client2.write(encodeQueryPacket("INSERT INTO test VALUES (20)"));
    client2.write(encodeQueryPacket("COMMIT"));

    // Await second cut
    const cut2 = await proxy.waitForCut(1);
    await cut2.commitCut;
    expect(proxy.cutCount).toBe(2);
    expect(proxy.cuts.length).toBe(2);

    const outcome2 = await cut2.serverSettled;
    expect(outcome2.status).toBe("committed");

    // stop() must gracefully wait for all cuts
    await proxy.stop(1000);
  });

  it("handles refused upstream honestly: does not increment cutCount on refused connection", async () => {
    proxy = await CutProxy.start({
      mode: "after_forward",
      upstreamPort: 1, // Port 1 is unreachable
    });

    const client = net.connect(proxy.port, "127.0.0.1");
    client.resume();

    await new Promise<void>((resolve) => client.once("connect", resolve));

    client.write(encodeStartupPacket());
    client.write(encodeQueryPacket("COMMIT"));

    const outcome = await proxy.serverSettled;
    expect(outcome.status).toBe("refused");
    expect(proxy.cutCount).toBe(0);

    client.destroy();
  });

  it("supports multiple valid frames packaged in a single TCP write without false rejection", async () => {
    mockPostgres = await MockPostgresServer.start();
    // Set maxFrameSize small (200 bytes), but buffer large (1024 bytes)
    proxy = await CutProxy.start({
      mode: "after_forward",
      upstreamPort: mockPostgres.port,
      maxFrameSize: 200,
      maxBufferSize: 2048,
    });

    const client = net.connect(proxy.port, "127.0.0.1");
    client.resume();
    await new Promise<void>((resolve) => client.once("connect", resolve));

    // Three valid frames whose COMBINED length is ~250 bytes (> maxFrameSize 200),
    // but each INDIVIDUAL frame is ~80 bytes (<= maxFrameSize 200).
    const frame1 = encodeQueryPacket("SELECT 1000");
    const frame2 = encodeQueryPacket("SELECT 2000");
    const frame3 = encodeQueryPacket("SELECT 3000");
    const combined = Buffer.concat([encodeStartupPacket(), frame1, frame2, frame3]);

    client.write(combined);

    await new Promise((r) => setTimeout(r, 100));

    expect(mockPostgres.receivedQueries).toEqual(["SELECT 1000", "SELECT 2000", "SELECT 3000"]);
    client.destroy();
  });

  it("rejects an individual frame that exceeds maxFrameSize", async () => {
    mockPostgres = await MockPostgresServer.start();
    proxy = await CutProxy.start({
      mode: "after_forward",
      upstreamPort: mockPostgres.port,
      maxFrameSize: 100, // 100-byte frame limit
    });

    const client = net.connect(proxy.port, "127.0.0.1");
    client.resume();
    let clientError = false;

    client.on("close", () => {
      clientError = true;
    });
    client.on("error", () => {
      clientError = true;
    });

    await new Promise<void>((resolve) => client.once("connect", resolve));

    client.write(encodeStartupPacket());
    await new Promise((r) => setTimeout(r, 50));

    // Send a single frame claiming to be 500 bytes (> 100 bytes limit)
    const bigHeader = Buffer.alloc(5);
    bigHeader[0] = 0x51; // 'Q'
    bigHeader.writeUInt32BE(500, 1);
    client.write(bigHeader);

    await new Promise<void>((resolve) => {
      if (clientError) return resolve();
      client.once("close", () => resolve());
    });
    expect(clientError).toBe(true);
  });

  it("handles variations like 'COMMIT;' and 'COMMIT WORK;'", async () => {
    mockPostgres = await MockPostgresServer.start();
    proxy = await CutProxy.start("after_forward", mockPostgres.port);

    const client = net.connect(proxy.port, "127.0.0.1");
    client.resume();
    client.on("error", () => {});
    client.on("close", () => {});
    await new Promise<void>((resolve) => client.once("connect", resolve));

    client.write(encodeStartupPacket());
    client.write(encodeQueryPacket("commit work; "));

    await proxy.commitCut;
    expect(proxy.cutCount).toBe(1);

    const outcome = await proxy.serverSettled;
    expect(outcome.status).toBe("committed");

    expect(mockPostgres.receivedQueries).toContain("commit work; ");
  });
});
