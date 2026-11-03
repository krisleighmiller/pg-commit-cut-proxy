import net from "node:net";

export type CutMode = "after_forward" | "before_forward";

export interface CutProxyOptions {
  /**
   * The injection mode when a COMMIT simple query frame arrives:
   * - "after_forward": forwards COMMIT to PostgreSQL, then immediately destroys
   *   the client socket with RST before the server's response can return.
   *   PostgreSQL WILL commit, but the client cannot learn the outcome.
   * - "before_forward": destroys both sockets immediately without forwarding.
   *   PostgreSQL never sees COMMIT and rolls back when the connection drops.
   */
  mode: CutMode;

  /** Port of the real PostgreSQL server. Defaults to 5432. */
  upstreamPort?: number;

  /** Host of the real PostgreSQL server. Defaults to "127.0.0.1". */
  upstreamHost?: string;

  /** Host interface to bind the proxy to. Defaults to "127.0.0.1". */
  listenHost?: string;

  /** Port to listen on. Defaults to 0 (assigns an ephemeral port). */
  listenPort?: number;
}

/**
 * Deterministic uncertain-COMMIT injector for PostgreSQL.
 *
 * A lightweight TCP proxy that parses the PostgreSQL frontend wire protocol frame-by-frame.
 * When a simple-Query frame ('Q', 0x51) whose SQL is COMMIT arrives:
 *   - mode "after_forward": the frame is delivered (the server WILL commit),
 *     the client connection is destroyed before the response can return, and the
 *     upstream connection is shut down gracefully. The caller can never learn the
 *     outcome: the classic ambiguous commit.
 *   - mode "before_forward": both sockets are destroyed without forwarding;
 *     the server never sees COMMIT and rolls back when the connection drops.
 *
 * No sleeps, no timing probabilities: the cut is synchronized on the wire protocol
 * COMMIT frame itself. `commitSeen` resolves once the frame has been intercepted and cut.
 */
export class CutProxy {
  private readonly server: net.Server;
  private readonly mode: CutMode;
  private readonly upstreamPort: number;
  private readonly upstreamHost: string;
  private readonly activeSockets = new Set<net.Socket>();

  readonly commitSeen: Promise<void>;
  private resolveSeen!: () => void;
  private _cutCount = 0;

  private constructor(options: CutProxyOptions) {
    this.mode = options.mode;
    this.upstreamPort = options.upstreamPort ?? 5432;
    this.upstreamHost = options.upstreamHost ?? "127.0.0.1";

    this.commitSeen = new Promise<void>((resolve) => {
      this.resolveSeen = resolve;
    });

    this.server = net.createServer((downstream) => this.handleConnection(downstream));
  }

  /**
   * Start a new CutProxy instance.
   *
   * @param modeOrOptions Either a CutMode string ("after_forward" | "before_forward") or full CutProxyOptions.
   * @param upstreamPort Port of the target PostgreSQL server (if first argument is a CutMode string). Defaults to 5432.
   * @param upstreamHost Host of the target PostgreSQL server (if first argument is a CutMode string). Defaults to "127.0.0.1".
   */
  static async start(
    modeOrOptions: CutMode | CutProxyOptions,
    upstreamPort?: number,
    upstreamHost?: string
  ): Promise<CutProxy> {
    const options: CutProxyOptions =
      typeof modeOrOptions === "string"
        ? { mode: modeOrOptions, upstreamPort: upstreamPort ?? 5432, upstreamHost: upstreamHost ?? "127.0.0.1" }
        : modeOrOptions;

    const proxy = new CutProxy(options);
    const listenHost = options.listenHost ?? "127.0.0.1";
    const listenPort = options.listenPort ?? 0;

    await new Promise<void>((resolve, reject) => {
      proxy.server.once("error", reject);
      proxy.server.listen(listenPort, listenHost, () => {
        proxy.server.removeListener("error", reject);
        resolve();
      });
    });

    return proxy;
  }

  /** The local port the proxy is listening on. */
  get port(): number {
    return (this.server.address() as net.AddressInfo).port;
  }

  /** Number of times a COMMIT frame was intercepted and cut. */
  get cutCount(): number {
    return this._cutCount;
  }

  /** Stop the proxy server and terminate any open client or upstream sockets. */
  async stop(): Promise<void> {
    for (const socket of this.activeSockets) {
      socket.destroy();
    }
    this.activeSockets.clear();

    await new Promise<void>((resolve) => {
      this.server.close(() => resolve());
    });
  }

  private handleConnection(downstream: net.Socket): void {
    const upstream = net.connect(this.upstreamPort, this.upstreamHost);

    this.activeSockets.add(downstream);
    this.activeSockets.add(upstream);

    const cleanup = () => {
      this.activeSockets.delete(downstream);
      this.activeSockets.delete(upstream);
    };

    let buf: Buffer = Buffer.alloc(0);
    let startupDone = false;
    let cut = false;

    const destroyBoth = () => {
      cut = true;
      cleanup();
      downstream.destroy();
      upstream.destroy();
    };

    downstream.on("data", (chunk: Buffer) => {
      if (cut) return;
      buf = Buffer.concat([buf, chunk]);

      // Forward complete frames as they become available.
      for (;;) {
        let frameLen = 0;
        if (!startupDone) {
          if (buf.length < 4) break;
          frameLen = buf.readUInt32BE(0); // Startup packet: length including length word itself
        } else {
          if (buf.length < 5) break;
          frameLen = 1 + buf.readUInt32BE(1); // Type byte + length including length word itself
        }

        if (buf.length < frameLen) break;

        const frame = buf.subarray(0, frameLen);
        buf = buf.subarray(frameLen);

        let isCommit = false;
        if (startupDone && frame[0] === 0x51) {
          // 'Q' simple query: message format is 'Q' (1 byte) + length (4 bytes) + null-terminated string
          // Slice out the null-terminated SQL payload
          const queryStr = frame
            .subarray(5, frame.length - 1)
            .toString("utf8")
            .trim()
            .replace(/;+$/, "")
            .trim()
            .toUpperCase();

          isCommit =
            queryStr === "COMMIT" ||
            queryStr === "COMMIT WORK" ||
            queryStr === "COMMIT TRANSACTION";
        }

        startupDone = true;

        if (isCommit) {
          if (this.mode === "after_forward") {
            // Deliver COMMIT (server will commit), then cut the client before
            // the response packet can arrive. Gracefully FIN upstream so the server
            // processes and flushes the frame; RST on the client downstream side.
            upstream.write(frame, () => upstream.end());
            cut = true;
            this.activeSockets.delete(downstream);
            downstream.destroy();
            this._cutCount += 1;
            this.resolveSeen();
          } else {
            // Never deliver COMMIT: server rolls back when connection drops.
            destroyBoth();
            this._cutCount += 1;
            this.resolveSeen();
          }
          return;
        }

        upstream.write(frame);
      }
    });

    upstream.on("data", (chunk: Buffer) => {
      if (!cut) downstream.write(chunk);
    });

    upstream.on("error", () => {
      if (!cut) downstream.destroy();
      cleanup();
    });

    downstream.on("error", () => {
      if (!cut) upstream.destroy();
      cleanup();
    });

    upstream.on("close", () => {
      if (!cut) downstream.destroy();
      cleanup();
    });

    downstream.on("close", () => {
      if (!cut) upstream.destroy();
      cleanup();
    });
  }
}
