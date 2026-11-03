import net from "node:net";

export type CutMode = "after_forward" | "before_forward";

export type ServerCommitStatus = "committed" | "rolled_back" | "error" | "refused";

export interface ServerCommitOutcome {
  /**
   * The authoritative outcome on the PostgreSQL server side:
   * - "committed": PostgreSQL completed the full query response cycle for COMMIT
   *   (received CommandComplete 'COMMIT' followed by ReadyForQuery 'I').
   * - "rolled_back": COMMIT was never delivered to PostgreSQL (before_forward mode),
   *   and PostgreSQL subsequently closed the upstream connection after observing
   *   EOF and completing transaction cleanup.
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
   * - "before_forward": severs the client without forwarding COMMIT, half-closes
   *   upstream, and waits for PostgreSQL's reciprocal EOF after transaction cleanup.
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

  /** Maximum size of a typed frontend/backend frame, including its header. Defaults to 16MB (16,777,216 bytes). Startup packets have a separate 10KB limit. */
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
  _rejectCut: (error: Error) => void;
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
 *   - mode "before_forward": severs the client without forwarding COMMIT,
 *     half-closes upstream, and verifies rollback completion from PostgreSQL's EOF.
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
  private readonly cutWaiters = new Map<number, Set<{ resolve: (record: CutRecord) => void; reject: (error: Error) => void }>>();
  private stopped = false;
  private stopPromise?: Promise<void>;

  private _cutCount = 0;

  /** All cuts executed by this proxy instance so far. */
  get cuts(): readonly CutRecord[] {
    return this._cuts;
  }

  /**
   * Get a cut record by index, or wait for it to occur if not yet triggered.
   */
  waitForCut(index: number): Promise<CutRecord> {
    if (!Number.isSafeInteger(index) || index < 0) {
      return Promise.reject(new Error("CutProxy: cut index must be a nonnegative safe integer"));
    }
    if (this._cuts[index]) {
      return Promise.resolve(this._cuts[index]);
    }
    if (this.stopped) return Promise.reject(new Error("CutProxy: proxy stopped before cut"));
    return new Promise<CutRecord>((resolve, reject) => {
      const waiters = this.cutWaiters.get(index) ?? new Set();
      waiters.add({ resolve, reject });
      this.cutWaiters.set(index, waiters);
    });
  }

  /** Get an already-executed cut record by index. */
  cut(index: number): CutRecord | undefined {
    if (!Number.isSafeInteger(index) || index < 0) return undefined;
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

    // Reject bad runtime configuration before opening a listener. TypeScript
    // types do not protect JavaScript callers, and net.connect can otherwise
    // throw later from the server's connection callback.
    if (this.mode !== "after_forward" && this.mode !== "before_forward") {
      throw new Error('CutProxy: mode must be "after_forward" or "before_forward"');
    }
    if (this.sslMode !== "forward" && this.sslMode !== "refuse") {
      throw new Error('CutProxy: sslMode must be "forward" or "refuse"');
    }
    for (const [name, value] of [["maxFrameSize", this.maxFrameSize], ["maxBufferSize", this.maxBufferSize]] as const) {
      if (!Number.isSafeInteger(value) || value <= 0) {
        throw new Error(`CutProxy: ${name} must be a positive safe integer`);
      }
    }
    if (!Number.isInteger(this.upstreamPort) || this.upstreamPort < 1 || this.upstreamPort > 65535) {
      throw new Error("CutProxy: upstreamPort must be an integer from 1 to 65535");
    }
    if (typeof this.upstreamHost !== "string" || this.upstreamHost.length === 0) {
      throw new Error("CutProxy: upstreamHost must be a non-empty string");
    }

    this.firstCutRecord = this.createCutRecord(0);

    this.server = net.createServer((downstream) => this.handleConnection(downstream));
  }

  private createCutRecord(index: number): InternalCutRecord {
    let resolveCut!: () => void;
    let rejectCut!: (error: Error) => void;
    const commitCut = new Promise<void>((resolve, reject) => {
      resolveCut = resolve;
      rejectCut = reject;
    });
    // A proxy may be stopped without callers observing commitCut. Registering an
    // internal rejection handler prevents a terminal no-cut shutdown from
    // becoming an unhandled rejection; awaiting the original promise still
    // rejects normally.
    void commitCut.catch(() => {});

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
      _rejectCut: rejectCut,
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
    if (typeof listenHost !== "string" || listenHost.length === 0) {
      throw new Error("CutProxy: listenHost must be a non-empty string");
    }
    if (!Number.isInteger(listenPort) || listenPort < 0 || listenPort > 65535) {
      throw new Error("CutProxy: listenPort must be an integer from 0 to 65535");
    }

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
   * If any cuts are currently in flight, `stop()` waits up to
   * `gracePeriodMs` for all pending cuts to settle before closing upstream sockets.
   */
  stop(gracePeriodMs = 2000): Promise<void> {
    this.stopPromise ??= this.stopInternal(gracePeriodMs);
    return this.stopPromise;
  }

  private async stopInternal(gracePeriodMs: number): Promise<void> {
    this.stopped = true;
    // Stop accepting before the grace period; otherwise newly accepted cuts
    // are absent from the snapshot being drained.
    const closed = new Promise<void>((resolve) => this.server.close(() => resolve()));
    for (const waiters of this.cutWaiters.values()) {
      for (const waiter of waiters) waiter.reject(new Error("CutProxy: proxy stopped before cut"));
    }
    this.cutWaiters.clear();
    if (this._cuts.length === 0) {
      const error = new Error("CutProxy: proxy stopped before first cut");
      this.firstCutRecord._rejectCut(error);
      this.firstCutRecord._resolveSettled({ status: "error", error: error.message });
    }
    const pending = this._cuts.filter((c) => !c.isSettled).map((c) => c.serverSettled);
    if (pending.length > 0) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          Promise.allSettled(pending),
          new Promise<void>((resolve) => { timer = setTimeout(resolve, Math.max(0, gracePeriodMs)); }),
        ]);
      } finally {
        clearTimeout(timer);
      }
    }
    for (const socket of this.activeSockets) socket.destroy();
    this.activeSockets.clear();
    for (const record of this._cuts) {
      record._resolveSettled({ status: "error", error: "CutProxy stopped before COMMIT cycle completed" });
    }
    await closed;
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
    if (this.stopped) { downstream.destroy(); return; }
    const upstream = net.connect(this.upstreamPort, this.upstreamHost);

    this.activeSockets.add(downstream);
    this.activeSockets.add(upstream);

    let upstreamConnected = false;
    let upstreamError: Error | null = null;
    let awaitingRollbackClosure = false;

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

    upstream.on("end", () => {
      // In before_forward mode we half-close our write side instead of locally
      // destroying the socket. PostgreSQL's reciprocal FIN arrives only after
      // it observes EOF, aborts the session transaction, and closes the backend
      // connection, so this is the rollback-completion barrier.
      if (awaitingRollbackClosure && currentCutRecord && !currentCutRecord.isSettled) {
        currentCutRecord._resolveSettled({ status: "rolled_back" });
      }
    });

    upstream.on("close", () => {
      this.activeSockets.delete(upstream);
      if (!cut) {
        downstream.destroy();
      }
      if (currentCutRecord && !currentCutRecord.isSettled) {
        currentCutRecord._resolveSettled({
          status: upstreamConnected ? "error" : "refused",
          error: upstreamError?.message ?? "Upstream socket closed before COMMIT cycle completed",
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
    type PendingAction =
      | { kind: "ready" | "query" | "function" | "sync"; segment?: number }
      | { kind: "parse" | "bind" | "describe" | "execute" | "close"; segment: number };
    const pendingActions: PendingAction[] = [];
    let openExtendedSegment: number | undefined;
    let nextExtendedSegment = 0;
    let recoveringExtendedSegment: number | undefined;

    const handleDownstreamData = async (chunk: Buffer) => {
      if (cut) return;
      // Shutdown drains only cuts already admitted to its pending snapshot.
      // An established, uncut connection must not start another transaction
      // or COMMIT during that grace period.
      if (this.stopped) {
        downstream.destroy();
        upstream.destroy();
        return;
      }

      downstreamBuf = Buffer.concat([downstreamBuf, chunk]);

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
          pendingActions.push({ kind: "ready" });
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
            if (frame.length < 6 || frame.indexOf(0, 5) !== frame.length - 1) {
              downstream.destroy(new Error("CutProxy: malformed Simple Query string"));
              upstream.destroy();
              return;
            }
            // Match PostgreSQL's ASCII keyword casing and scanner whitespace,
            // not JavaScript's Unicode-wide trim/case normalization. In
            // particular, dotless i, long s, NBSP, BOM, and vertical tab must
            // remain ordinary (potentially invalid) SQL sent to the server.
            // https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/parser/scan.l
            const queryStr = frame.subarray(5, frame.length - 1).toString("utf8");
            const match = /^[ \t\n\r\f]*[cC][oO][mM][mM][iI][tT](?:[ \t\n\r\f]+(?:[wW][oO][rR][kK]|[tT][rR][aA][nN][sS][aA][cC][tT][iI][oO][nN]))?[ \t\n\r\f]*(?:;+[ \t\n\r\f]*)?$/.exec(queryStr);
            // JS '$' can match before a final line terminator. Require that
            // the match consumed the entire payload, including its last byte.
            isCommit = match !== null && match[0] === queryStr;
          }

          if (isCommit) {
            // Reserve before awaiting connection: another data event must not
            // allocate a second cut or forward traffic beyond this COMMIT.
            cut = true;
            downstreamBuf = Buffer.alloc(0);
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
              for (const waiting of waiter) waiting.resolve(record);
            }

            if (this.mode === "after_forward") {
              if (upstream.connecting) {
                await new Promise<void>((resolve) => {
                  const done = () => {
                    upstream.removeListener("connect", done);
                    upstream.removeListener("error", done);
                    upstream.removeListener("close", done);
                    resolve();
                  };
                  upstream.once("connect", done);
                  upstream.once("error", done);
                  upstream.once("close", done);
                });
              }

              if (upstreamError || !upstreamConnected || upstream.destroyed) {
                record._resolveSettled({
                  status: upstreamConnected ? "error" : "refused",
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
              // before_forward: withhold COMMIT and half-close upstream. A local
              // destroy only proves the cut happened; waiting for PostgreSQL's
              // reciprocal EOF proves the backend observed disconnect and
              // completed transaction cleanup.
              cut = true;
              this.severClient(downstream, upstream);
              awaitingRollbackClosure = true;
              upstream.end();

              this._cutCount += 1;
              record._resolveCut();
              return;
            }
          }

          // Keep an ordered model of frontend actions whose backend responses
          // precede the intercepted COMMIT. This is deliberately smaller than a
          // full protocol implementation, but it preserves Sync boundaries so
          // one extended-protocol error cannot erase work from a later batch.
          const extendedKinds = new Map<number, PendingAction["kind"]>([
            [0x50, "parse"], [0x42, "bind"], [0x44, "describe"],
            [0x45, "execute"], [0x43, "close"],
          ]);
          const extendedKind = extendedKinds.get(msgType);
          if (extendedKind || msgType === 0x48) {
            openExtendedSegment ??= nextExtendedSegment++;
            if (recoveringExtendedSegment !== openExtendedSegment && extendedKind) {
              pendingActions.push({ kind: extendedKind as "parse" | "bind" | "describe" | "execute" | "close", segment: openExtendedSegment });
            }
          } else if (msgType === 0x51 || msgType === 0x46) {
            if (recoveringExtendedSegment === undefined || recoveringExtendedSegment !== openExtendedSegment) {
              pendingActions.push({
                kind: msgType === 0x51 ? "query" : "function",
                ...(openExtendedSegment === undefined ? {} : { segment: openExtendedSegment }),
              });
            }
          } else if (msgType === 0x53) {
            pendingActions.push({ kind: "sync", ...(openExtendedSegment === undefined ? {} : { segment: openExtendedSegment }) });
            if (recoveringExtendedSegment === openExtendedSegment) recoveringExtendedSegment = undefined;
            openExtendedSegment = undefined;
          }
          if (!upstream.write(frame)) downstream.pause();
        }
      }
      if (downstreamBuf.length > this.maxBufferSize) {
        downstream.destroy(new Error("CutProxy: unparsed buffer exceeded maxBufferSize"));
        upstream.destroy();
      }
    };
    downstream.on("data", handleDownstreamData);

    upstream.on("data", (chunk: Buffer) => {
      if (startupState === "awaiting_ssl_response") {
        const replyByte = chunk[0];
        if (replyByte === 0x4e) {
          downstream.write(Buffer.from([0x4e]));
          startupState = "awaiting_startup";
          // A pipelined plaintext startup may already be buffered. No further
          // client data event is guaranteed after negotiation completes.
          void handleDownstreamData(Buffer.alloc(0));
          // Consume only the negotiation byte, not a coalesced response frame.
          chunk = chunk.subarray(1);
          if (chunk.length === 0) return;
        } else {
          // libpq also fails negotiation immediately for errors/unknown bytes;
          // do not retain a single invalid byte as an incomplete typed frame.
          const err = new Error(
            replyByte === 0x53 || replyByte === 0x47
              ? "CutProxy: upstream accepted SSL/GSS encryption. Unencrypted connections are required to inspect SQL frames."
              : "CutProxy: invalid upstream SSL/GSS negotiation response"
          );
          downstream.destroy(err);
          upstream.destroy(err);
          return;
        }
      }

      // Keep response framing from the first byte, including partial frames
      // received before the cut. PostgreSQL protocol message formats:
      // https://www.postgresql.org/docs/current/protocol-message-formats.html
      upstreamBuf = Buffer.concat([upstreamBuf, chunk]);
      const failResponse = (error: string) => {
        currentCutRecord?._resolveSettled({ status: "error", error });
        downstream.destroy();
        upstream.destroy();
      };
      for (;;) {
        if (upstreamBuf.length < 5) break;
        const uType = upstreamBuf[0];
        const uLen = upstreamBuf.readUInt32BE(1);
        const uTotalLen = 1 + uLen;
        if (uLen < 4 || uTotalLen > this.maxFrameSize) {
          failResponse("CutProxy: malformed or oversized PostgreSQL response frame");
          return;
        }
        if (upstreamBuf.length < uTotalLen) break;
        const uFrame = upstreamBuf.subarray(0, uTotalLen);
        upstreamBuf = upstreamBuf.subarray(uTotalLen);

        if (uType === 0x5a && (uTotalLen !== 6 || ![0x49, 0x54, 0x45].includes(uFrame[5]))) {
          failResponse("CutProxy: malformed ReadyForQuery response");
          return;
        }
        const pending = pendingActions[0];
        if (pending) {
          if (["parse", "bind", "describe", "execute", "close"].includes(pending.kind)) {
            if (uType === 0x45) {
              const segment = pending.segment!;
              const syncIndex = pendingActions.findIndex((action) => action.kind === "sync" && action.segment === segment);
              if (syncIndex >= 0) {
                // PostgreSQL discards this segment through Sync. Retain that
                // Sync because it still produces the segment's ReadyForQuery.
                pendingActions.splice(0, syncIndex);
              } else {
                while (pendingActions[0]?.segment === segment) pendingActions.shift();
                recoveringExtendedSegment = segment;
              }
            } else {
              const completed =
                (pending.kind === "parse" && uType === 0x31) ||
                (pending.kind === "bind" && uType === 0x32) ||
                (pending.kind === "describe" && (uType === 0x54 || uType === 0x6e)) ||
                (pending.kind === "execute" && (uType === 0x43 || uType === 0x73 || uType === 0x49)) ||
                (pending.kind === "close" && uType === 0x33);
              if (completed) pendingActions.shift();
            }
          } else if (uType === 0x5a) {
            pendingActions.shift();
          }
          continue;
        }
        if (awaitingRollbackClosure) continue;
        if (!cut || !currentCutRecord) continue;

        if (uType === 0x43) {
          // CommandComplete is a NUL-terminated, exact command tag. A prefix
          // such as COMMIT PREPARED is not evidence of a standalone COMMIT.
          sawCommandComplete = uFrame.subarray(5).equals(Buffer.from("COMMIT\0"));
          commitTag = sawCommandComplete ? "COMMIT" : "";
        } else if (uType === 0x45) {
          currentCutRecord._resolveSettled({
            status: "error",
            error: "PostgreSQL returned ErrorResponse for COMMIT",
          });
        } else if (uType === 0x5a) {
          currentCutRecord._resolveSettled(sawCommandComplete && uFrame[5] === 0x49
            ? { status: "committed", tag: commitTag, completedCycle: true }
            : { status: "error", error: "COMMIT cycle ended without exact COMMIT and idle ReadyForQuery" });
          // No further commands are sent on this connection. Do not wait for a
          // peer FIN (or remain paused behind the severed client's backpressure).
          upstream.destroy();
          return;
        }
      }
      if (upstreamBuf.length > this.maxBufferSize) {
        failResponse("CutProxy: unparsed response buffer exceeded maxBufferSize");
        return;
      }
      if (!cut && !downstream.destroyed) {
        if (!downstream.write(chunk)) upstream.pause();
      }
    });
  }
}
