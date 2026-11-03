import net from "node:net";

export type CutMode = "after_forward" | "before_forward";

export type ServerCommitStatus = "committed" | "rolled_back" | "error" | "refused";

export interface ServerCommitOutcome {
  /**
   * The authoritative outcome on the PostgreSQL server side:
   * - "committed": PostgreSQL completed the full query response cycle for COMMIT
   *   (received CommandComplete 'COMMIT' followed by ReadyForQuery 'I').
   * - "rolled_back": COMMIT was never delivered to PostgreSQL (before_forward mode),
   *   inducing an automatic rollback when the connection drops.
   * - "error": PostgreSQL returned an ErrorResponse or the connection dropped before completion.
   * - "refused": Upstream PostgreSQL was unreachable or refused connection.
   */
  status: ServerCommitStatus;

  /** Command tag returned by PostgreSQL (e.g. "COMMIT"). */
  tag?: string;

  /** Error message if an error occurred. */
  error?: string;

  /** True if the full PostgreSQL response cycle (CommandComplete + ReadyForQuery) completed. */
  completedCycle?: boolean;
}

export interface CutRecord {
  /** Sequential index of this cut (0-indexed). */
  readonly index: number;

  /** Resolves when the client socket cut is executed. */
  readonly commitCut: Promise<void>;

  /** Resolves when upstream PostgreSQL finishes processing the commit or fails. */
  readonly serverSettled: Promise<ServerCommitOutcome>;

  /** Whether serverSettled has resolved for this cut. */
  readonly isSettled: boolean;
}

export interface CutProxyOptions {
  /**
   * The injection mode when a COMMIT simple-query frame arrives:
   * - "after_forward": forwards COMMIT to PostgreSQL, then abruptly severs
   *   the client connection. Upstream PostgreSQL continues running to process the
   *   commit; the server outcome is monitored and reported via `serverSettled`.
   * - "before_forward": destroys both sockets immediately without forwarding COMMIT.
   *   PostgreSQL never receives COMMIT and rolls back on connection drop.
   */
  mode: CutMode;

  /** Port of the target PostgreSQL server. Defaults to 5432. */
  upstreamPort?: number;

  /** Host of the target PostgreSQL server. Defaults to "127.0.0.1". */
  upstreamHost?: string;

  /** Host interface to bind the proxy listener to. Defaults to "127.0.0.1". */
  listenHost?: string;

  /** Port to listen on. Defaults to 0 (assigns an ephemeral free port). */
  listenPort?: number;

  /** Maximum allowed size in bytes for a single frame. Defaults to 16MB (16,777,216 bytes). */
  maxFrameSize?: number;

  /** Maximum allowed size in bytes for unparsed buffer accumulation. Defaults to 64MB (67,108,864 bytes). */
  maxBufferSize?: number;

  /**
   * Handling of client SSLRequest (code 80877103) / GSSENCRequest (code 80877104):
   * - "forward": forwards SSLRequest to upstream. If upstream refuses ('N'), relays 'N'
   *   and awaits plaintext StartupMessage. If upstream accepts ('S' / 'G'), fails closed
   *   because encrypted traffic cannot be inspected as plaintext.
   * - "refuse": proxy immediately responds 'N' directly to client without forwarding,
   *   prompting standard PostgreSQL drivers (pg, libpq) to fall back to plaintext.
   *
   * Defaults to "forward".
   */
  sslMode?: "forward" | "refuse";
}

interface InternalCutRecord extends CutRecord {
  _resolveCut: () => void;
  _resolveSettled: (outcome: ServerCommitOutcome) => void;
}

/**
 * Deterministic wire-protocol TCP proxy for testing PostgreSQL ambiguous commits.
 *
 * Sits between a PostgreSQL client and server, parsing frontend Simple Query ('Q') frames.
 * When an exact standalone COMMIT query arrives:
 *   - mode "after_forward": forwards COMMIT upstream, severs the client connection,
 *     and monitors PostgreSQL's CommandComplete ('C') and ReadyForQuery ('Z') response
 *     cycle to verify that the transaction actually committed.
 *   - mode "before_forward": severs both connections immediately without forwarding COMMIT,
 *     forcing PostgreSQL to roll back when the connection drops.
 *
 * Scope & Limitations:
 * - Matches standalone Simple Query ('Q') frames containing COMMIT, COMMIT WORK,
 *   or COMMIT TRANSACTION (case-insensitive, optional trailing semicolon).
 * - Does NOT parse Extended Query Protocol frames ('P' Parse, 'B' Bind, 'E' Execute).
 * - Does NOT split multi-statement query frames (e.g. "COMMIT; SELECT 1").
 * - Requires unencrypted connections to inspect SQL frames; SSL/GSS requests are
 *   refused or must be refused by upstream.
 */
export class CutProxy {
  private readonly server: net.Server;
  private readonly mode: CutMode;
  private readonly upstreamPort: number;
  private readonly upstreamHost: string;
  private readonly maxFrameSize: number;
  private readonly maxBufferSize: number;
  private readonly sslMode: "forward" | "refuse";
  private readonly activeSockets = new Set<net.Socket>();

  private readonly _cuts: InternalCutRecord[] = [];
  private readonly firstCutRecord: InternalCutRecord;
  private readonly cutWaiters = new Map<number, (record: CutRecord) => void>();

  private _cutCount = 0;

  /** All cuts executed by this proxy instance so far. */
  get cuts(): readonly CutRecord[] {
    return this._cuts;
  }

  /**
   * Get a cut record by index, or wait for it to occur if not yet triggered.
   */
  waitForCut(index: number): Promise<CutRecord> {
    if (this._cuts[index]) {
      return Promise.resolve(this._cuts[index]);
    }
    return new Promise<CutRecord>((resolve) => {
      this.cutWaiters.set(index, resolve);
    });
  }

  /** Get an already-executed cut record by index. */
  cut(index: number): CutRecord | undefined {
    return this._cuts[index];
  }

  /** Resolves when the first client socket cut occurs. */
  get commitCut(): Promise<void> {
    return this.firstCutRecord.commitCut;
  }

  /** Alias for `commitCut` for backward compatibility. */
  get commitSeen(): Promise<void> {
    return this.commitCut;
  }

  /** Resolves when upstream PostgreSQL settles the first cut. */
  get serverSettled(): Promise<ServerCommitOutcome> {
    return this.firstCutRecord.serverSettled;
  }

  private constructor(options: CutProxyOptions) {
    this.mode = options.mode;
    this.upstreamPort = options.upstreamPort ?? 5432;
    this.upstreamHost = options.upstreamHost ?? "127.0.0.1";
    this.maxFrameSize = options.maxFrameSize ?? 16 * 1024 * 1024;
    this.maxBufferSize = options.maxBufferSize ?? 64 * 1024 * 1024;
    this.sslMode = options.sslMode ?? "forward";

    this.firstCutRecord = this.createCutRecord(0);

    this.server = net.createServer((downstream) => this.handleConnection(downstream));
  }

  private createCutRecord(index: number): InternalCutRecord {
    let resolveCut!: () => void;
    const commitCut = new Promise<void>((r) => {
      resolveCut = r;
    });

    let resolveSettled!: (outcome: ServerCommitOutcome) => void;
    let settled = false;
    const serverSettled = new Promise<ServerCommitOutcome>((r) => {
      resolveSettled = (outcome) => {
        if (!settled) {
          settled = true;
          r(outcome);
        }
      };
    });

    return {
      index,
      commitCut,
      serverSettled,
      get isSettled() {
        return settled;
      },
      _resolveCut: resolveCut,
      _resolveSettled: resolveSettled,
    };
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

  /** Total number of cuts executed. */
  get cutCount(): number {
    return this._cutCount;
  }

  /**
   * Stop the proxy server and terminate open sockets.
   *
   * If any after_forward commits are currently in flight, `stop()` waits up to
   * `gracePeriodMs` for all pending cuts to settle before closing upstream sockets.
   */
  async stop(gracePeriodMs = 2000): Promise<void> {
    const pending = this._cuts.filter((c) => !c.isSettled).map((c) => c.serverSettled);
    if (pending.length > 0 && this.mode === "after_forward") {
      try {
        await Promise.race([
          Promise.allSettled(pending),
          new Promise((r) => setTimeout(r, Math.max(10, gracePeriodMs))),
        ]);
      } catch {
        // Continue shutdown regardless
      }
    }

    for (const socket of this.activeSockets) {
      socket.destroy();
    }
    this.activeSockets.clear();

    await new Promise<void>((resolve) => {
      this.server.close(() => resolve());
    });
  }

  private severClient(client: net.Socket, upstream?: net.Socket): void {
    this.activeSockets.delete(client);
    if (upstream) {
      upstream.resume();
    }
    if (typeof (client as any).resetAndDestroy === "function") {
      (client as any).resetAndDestroy();
    } else {
      client.destroy(new Error("CutProxy: connection terminated on COMMIT"));
    }
  }

  private handleConnection(downstream: net.Socket): void {
    const upstream = net.connect(this.upstreamPort, this.upstreamHost);

    this.activeSockets.add(downstream);
    this.activeSockets.add(upstream);

    let upstreamConnected = false;
    let upstreamError: Error | null = null;

    upstream.once("connect", () => {
      upstreamConnected = true;
    });

    upstream.on("error", (err) => {
      upstreamError = err;
      if (!cut) {
        downstream.destroy();
      }
    });

    downstream.on("error", () => {
      if (!cut) {
        upstream.destroy();
      }
    });

    downstream.on("close", () => {
      this.activeSockets.delete(downstream);
      if (!cut) {
        upstream.destroy();
      }
    });

    upstream.on("close", () => {
      this.activeSockets.delete(upstream);
      if (!cut) {
        downstream.destroy();
      }
      if (currentCutRecord && !currentCutRecord.isSettled) {
        currentCutRecord._resolveSettled({
          status: "error",
          error: "Upstream socket closed before COMMIT cycle completed",
        });
      }
    });

    // Wire backpressure
    upstream.on("drain", () => downstream.resume());
    downstream.on("drain", () => upstream.resume());

    let startupState: "awaiting_startup" | "awaiting_ssl_response" | "ready" = "awaiting_startup";
    let downstreamBuf: Buffer = Buffer.alloc(0);
    let upstreamBuf: Buffer = Buffer.alloc(0);
    let cut = false;
    let currentCutRecord: InternalCutRecord | null = null;
    let sawCommandComplete = false;
    let commitTag = "";

    downstream.on("data", async (chunk: Buffer) => {
      if (cut) return;

      downstreamBuf = Buffer.concat([downstreamBuf, chunk]);

      // Bounding check on overall unparsed buffer accumulation
      if (downstreamBuf.length > this.maxBufferSize) {
        downstream.destroy(new Error("CutProxy: unparsed buffer exceeded maxBufferSize"));
        upstream.destroy();
        return;
      }

      for (;;) {
        if (startupState === "awaiting_startup") {
          if (downstreamBuf.length < 4) break;
          const frameLen = downstreamBuf.readUInt32BE(0);

          if (frameLen < 8 || frameLen > 10240) {
            downstream.destroy(new Error("CutProxy: malformed PostgreSQL startup packet length"));
            upstream.destroy();
            return;
          }

          if (downstreamBuf.length < frameLen) break;

          const frame = downstreamBuf.subarray(0, frameLen);
          downstreamBuf = downstreamBuf.subarray(frameLen);

          const requestCode = frame.readUInt32BE(4);

          // SSLRequest (80877103) or GSSENCRequest (80877104)
          if (frameLen === 8 && (requestCode === 80877103 || requestCode === 80877104)) {
            if (this.sslMode === "refuse") {
              downstream.write(Buffer.from([0x4e]));
              continue;
            }

            startupState = "awaiting_ssl_response";
            if (!upstream.write(frame)) downstream.pause();
            continue;
          }

          // CancelRequest (80877102, length 16)
          if (frameLen === 16 && requestCode === 80877102) {
            if (!upstream.write(frame)) downstream.pause();
            continue;
          }

          // Regular StartupMessage
          startupState = "ready";
          if (!upstream.write(frame)) downstream.pause();
          continue;
        }

        if (startupState === "awaiting_ssl_response") {
          break;
        }

        if (startupState === "ready") {
          if (downstreamBuf.length < 5) break;

          const msgType = downstreamBuf[0];
          const msgLen = downstreamBuf.readUInt32BE(1);
          const frameLen = 1 + msgLen;

          // Individual frame length validation
          if (msgLen < 4 || frameLen > this.maxFrameSize) {
            downstream.destroy(
              new Error(`CutProxy: frame length (${frameLen}) exceeds maxFrameSize (${this.maxFrameSize})`)
            );
            upstream.destroy();
            return;
          }

          if (downstreamBuf.length < frameLen) break;

          const frame = downstreamBuf.subarray(0, frameLen);
          downstreamBuf = downstreamBuf.subarray(frameLen);

          let isCommit = false;
          if (msgType === 0x51) {
            // 'Q' simple query
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

          if (isCommit) {
            // Allocate cut record for this cut event
            const cutIdx = this._cuts.length;
            const record = cutIdx === 0 ? this.firstCutRecord : this.createCutRecord(cutIdx);
            if (cutIdx === 0 && this._cuts.length === 0) {
              this._cuts.push(record);
            } else if (cutIdx > 0) {
              this._cuts.push(record);
            }
            currentCutRecord = record;
            const waiter = this.cutWaiters.get(cutIdx);
            if (waiter) {
              this.cutWaiters.delete(cutIdx);
              waiter(record);
            }

            if (this.mode === "after_forward") {
              if (upstream.connecting) {
                await new Promise<void>((resolve) => {
                  upstream.once("connect", () => resolve());
                  upstream.once("error", () => resolve());
                });
              }

              if (upstreamError || !upstreamConnected || upstream.destroyed) {
                record._resolveSettled({
                  status: "refused",
                  error: upstreamError?.message ?? "Upstream PostgreSQL is not connected or refused",
                });
                this.severClient(downstream, upstream);
                record._resolveCut();
                return;
              }

              // Deliver COMMIT upstream
              cut = true;
              upstream.write(frame);

              // Abruptly sever the client
              this.severClient(downstream, upstream);

              this._cutCount += 1;
              record._resolveCut();

              // Upstream remains open to monitor server response cycle
              return;
            } else {
              // before_forward: destroy both sockets without forwarding COMMIT
              cut = true;
              this.severClient(downstream, upstream);
              upstream.destroy();

              this._cutCount += 1;
              record._resolveCut();
              record._resolveSettled({ status: "rolled_back" });
              return;
            }
          }

          // Forward non-commit frames upstream
          if (!upstream.write(frame)) downstream.pause();
        }
      }
    });

    upstream.on("data", (chunk: Buffer) => {
      if (startupState === "awaiting_ssl_response") {
        const replyByte = chunk[0];
        if (replyByte === 0x4e) {
          downstream.write(Buffer.from([0x4e]));
          startupState = "awaiting_startup";
          return;
        }

        if (replyByte === 0x53 || replyByte === 0x47) {
          const err = new Error(
            "CutProxy: upstream accepted SSL/GSS encryption. Unencrypted connections are required to inspect SQL frames."
          );
          downstream.destroy(err);
          upstream.destroy(err);
          return;
        }
      }

      if (cut && currentCutRecord) {
        // Monitor server response cycle for forwarded COMMIT
        upstreamBuf = Buffer.concat([upstreamBuf, chunk]);

        for (;;) {
          if (upstreamBuf.length < 5) break;
          const uType = upstreamBuf[0];
          const uLen = upstreamBuf.readUInt32BE(1);

          if (uLen < 4) break;
          const uTotalLen = 1 + uLen;
          if (upstreamBuf.length < uTotalLen) break;

          const uFrame = upstreamBuf.subarray(0, uTotalLen);
          upstreamBuf = upstreamBuf.subarray(uTotalLen);

          if (uType === 0x43) {
            // CommandComplete ('C')
            const tag = uFrame.subarray(5, uFrame.length - 1).toString("utf8").trim();
            if (tag.toUpperCase().startsWith("COMMIT")) {
              sawCommandComplete = true;
              commitTag = tag;
            }
          } else if (uType === 0x45) {
            // ErrorResponse ('E')
            currentCutRecord._resolveSettled({
              status: "error",
              error: "PostgreSQL returned ErrorResponse for COMMIT",
            });
          } else if (uType === 0x5a) {
            // ReadyForQuery ('Z'): full backend query cycle completed
            if (sawCommandComplete) {
              currentCutRecord._resolveSettled({
                status: "committed",
                tag: commitTag,
                completedCycle: true,
              });
            }
            upstream.end();
          }
        }
        return;
      }

      if (!cut && !downstream.destroyed) {
        if (!downstream.write(chunk)) upstream.pause();
      }
    });
  }
}
