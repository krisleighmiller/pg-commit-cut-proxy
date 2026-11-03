import { afterEach, describe, expect, it } from 'vitest';
import net from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { CutProxy, type CutProxyOptions } from '../src/cut-proxy.js';

const message = (type: string, payload: Buffer | string) => {
  const body = typeof payload === 'string' ? Buffer.from(payload) : payload;
  const frame = Buffer.alloc(body.length + 5);
  frame[0] = type.charCodeAt(0);
  frame.writeInt32BE(body.length + 4, 1);
  body.copy(frame, 5);
  return frame;
};
const query = (sql: string) => message('Q', `${sql}\0`);
const ready = (status = 'I') => message('Z', status);
const startup = Buffer.from([0, 0, 0, 8, 0, 3, 0, 0]);
const until = async (predicate: () => boolean) => {
  for (let i = 0; i < 200 && !predicate(); i++) await delay(5);
  expect(predicate()).toBe(true);
};

const cleanup: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
});

async function setup(options: Partial<CutProxyOptions> = {}) {
  let backend: net.Socket | undefined;
  let incoming = Buffer.alloc(0);
  const server = net.createServer(socket => {
    backend = socket;
    socket.on('error', () => {});
    socket.on('data', chunk => { incoming = Buffer.concat([incoming, chunk]); });
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  cleanup.push(() => new Promise<void>(resolve => {
    backend?.destroy();
    server.close(() => resolve());
  }));
  const proxy = await CutProxy.start({ mode: 'after_forward', upstreamPort: (server.address() as net.AddressInfo).port, ...options });
  cleanup.push(() => proxy.stop(0));
  const client = net.connect(proxy.port, '127.0.0.1');
  client.on('error', () => {});
  cleanup.push(async () => { client.destroy(); });
  let received = Buffer.alloc(0);
  client.on('data', chunk => { received = Buffer.concat([received, chunk]); });
  await new Promise<void>(resolve => client.once('connect', resolve));
  await until(() => backend !== undefined);
  return { proxy, client, backend: backend!, incoming: () => incoming, received: () => received };
}

async function handshake(f: Awaited<ReturnType<typeof setup>>) {
  f.client.write(startup);
  await until(() => f.incoming().length === startup.length);
  f.backend.write(ready());
  await until(() => f.received().length === ready().length);
}

async function cut(f: Awaited<ReturnType<typeof setup>>) {
  f.client.write(query('COMMIT'));
  await until(() => f.proxy.cuts.length === 1);
}

describe('core regression coverage', () => {
  for (const mode of ['before_forward', 'after_forward'] as const) {
    for (const sql of ['COMM\u0131T', '\u00a0COMMIT', 'COMMIT\u00a0', '\u000bCOMMIT', '\ufeffCOMMIT', 'COMMIT TRAN\u017fACTION', 'COMMIT\u2028', 'COMMIT\u2029']) {
      it(`${mode}: forwards non-ASCII keyword/whitespace lookalike ${JSON.stringify(sql)} without cutting`, async () => {
        const f = await setup({ mode });
        await handshake(f);
        const expected = Buffer.concat([startup, query(sql)]);
        f.client.write(query(sql));
        await until(() => f.incoming().length === expected.length || f.client.destroyed);
        expect(f.proxy.cutCount).toBe(0);
        expect(f.proxy.cuts).toHaveLength(0);
        expect(f.client.destroyed).toBe(false);
        expect(f.incoming()).toEqual(expected);
      });
    }
    for (const sql of ['commit', ' \t\r\n\fCoMmIt WoRk;; \n', 'COMMIT\tTRANSACTION;', 'COMMIT\nWORK']) {
      it(`${mode}: cuts ASCII COMMIT spelling ${JSON.stringify(sql)}`, async () => {
        const f = await setup({ mode });
        await handshake(f);
        f.client.write(query(sql));
        await f.proxy.commitCut;
        if (mode === 'after_forward') f.backend.write(Buffer.concat([message('C', 'COMMIT\0'), ready()]));
        expect((await f.proxy.serverSettled).status).toBe(mode === 'after_forward' ? 'committed' : 'rolled_back');
        expect(f.proxy.cutCount).toBe(1);
      });
    }
  }
  for (const invalid of [NaN, Infinity, -1, 0, 1.5]) {
    for (const key of ['maxFrameSize', 'maxBufferSize', 'upstreamPort'] as const) {
      it(`rejects invalid ${key}=${String(invalid)} before listening`, async () => {
        const started = CutProxy.start({ mode: 'after_forward', [key]: invalid });
        void started.then(proxy => { cleanup.push(() => proxy.stop(0)); }, () => {});
        await expect(started).rejects.toThrow(key);
      });
    }
  }
  for (const index of [NaN, Infinity, -1, 0.5, 'map', 'length', '__proto__']) {
    it(`rejects invalid cut waiter index ${String(index)}`, async () => {
      const f = await setup();
      await expect(f.proxy.waitForCut(index as number)).rejects.toThrow('cut index');
      expect(f.proxy.cut(index as number)).toBeUndefined();
    });
  }
  it('resolves all concurrent cut waiters to the same cut', async () => {
    const f = await setup();
    await handshake(f);
    const first = Promise.race([f.proxy.waitForCut(0), delay(250).then(() => { throw new Error('waiter timed out'); })]);
    const second = Promise.race([f.proxy.waitForCut(0), delay(250).then(() => { throw new Error('waiter timed out'); })]);
    f.client.write(query('COMMIT'));
    const results = await Promise.allSettled([first, second]);
    expect(results.map(result => result.status)).toEqual(['fulfilled', 'fulfilled']);
    if (results[0].status === 'fulfilled' && results[1].status === 'fulfilled') {
      expect(results[0].value).toBe(results[1].value);
    }
    f.backend.write(Buffer.concat([message('C', 'COMMIT\0'), ready()]));
    await f.proxy.serverSettled;
  });

  it('allows coalesced frontend traffic larger than maxBufferSize', async () => {
    const f = await setup({ maxBufferSize: 32 });
    const data = Buffer.concat([startup, ...Array.from({ length: 20 }, () => query('SELECT 1'))]);
    f.client.write(data);
    await until(() => f.incoming().length === data.length || f.client.destroyed);
    expect(f.incoming()).toEqual(data);
  });

  it('bounds partial backend frames before a cut', async () => {
    const f = await setup({ maxBufferSize: 32 });
    await handshake(f);
    const oversized = Buffer.alloc(40);
    oversized[0] = 'D'.charCodeAt(0);
    oversized.writeInt32BE(100, 1);
    f.backend.write(oversized);
    await until(() => f.client.destroyed);
  });

  it('bounds partial backend frames after a cut', async () => {
    const f = await setup({ maxBufferSize: 32 });
    await handshake(f);
    await cut(f);
    const oversized = Buffer.alloc(40);
    oversized[0] = 'D'.charCodeAt(0);
    oversized.writeInt32BE(100, 1);
    f.backend.write(oversized);
    expect((await f.proxy.serverSettled).status).toBe('error');
    await until(() => f.backend.destroyed);
  });

  it('allows coalesced backend traffic larger than maxBufferSize', async () => {
    const f = await setup({ maxBufferSize: 32 });
    await handshake(f);
    const responses = Buffer.concat(Array.from({ length: 20 }, () => message('C', 'SELECT 1\0')));
    f.backend.write(responses);
    await until(() => f.received().length === ready().length + responses.length);
    expect(f.client.destroyed).toBe(false);
  });

  for (const [tag, status] of [['COMMIT PREPARED', 'I'], ['COMMIT', 'T'], ['COMMIT', 'E'], ['COMMIT', 'X']]) {
    it(`does not report committed for tag=${tag}, status=${status}`, async () => {
      const f = await setup();
      await handshake(f);
      await cut(f);
      f.backend.write(Buffer.concat([message('C', `${tag}\0`), ready(status)]));
      expect((await f.proxy.serverSettled).status).toBe('error');
    });
  }

  it('preserves backend framing across the cut boundary', async () => {
    const f = await setup();
    await handshake(f);
    const notice = message('N', 'Mnotice\0\0');
    f.backend.write(notice.subarray(0, 3));
    await until(() => f.received().length === ready().length + 3);
    await cut(f);
    f.backend.write(Buffer.concat([notice.subarray(3), message('C', 'COMMIT\0'), ready()]));
    expect((await f.proxy.serverSettled).status).toBe('committed');
  });

  it('does not attribute a prior pipelined query response to the cut', async () => {
    const f = await setup();
    await handshake(f);
    f.client.write(Buffer.concat([query('BEGIN'), query('COMMIT')]));
    await until(() => f.proxy.cuts.length === 1);
    f.backend.write(Buffer.concat([message('C', 'BEGIN\0'), ready('T'), message('C', 'COMMIT\0'), ready()]));
    expect((await f.proxy.serverSettled).status).toBe('committed');
  });

  it('does not attribute a prior query error to a successful cut', async () => {
    const f = await setup();
    await handshake(f);
    f.client.write(query('SELECT 1'));
    await until(() => f.incoming().length > startup.length);
    f.client.write(query('COMMIT'));
    await until(() => f.proxy.cuts.length === 1);
    f.backend.write(Buffer.concat([message('E', 'SERROR\0\0'), ready(), message('C', 'COMMIT\0'), ready()]));
    expect((await f.proxy.serverSettled).status).toBe('committed');
  });

  for (const [name, length] of [['oversized', 100], ['malformed', 3]] as const) {
    for (const afterCut of [false, true]) {
      it(`rejects ${name} backend frame headers ${afterCut ? 'after' : 'before'} the cut`, async () => {
        const f = await setup({ maxFrameSize: 32 });
        await handshake(f);
        if (afterCut) await cut(f);
        const header = Buffer.alloc(5);
        header[0] = 'D'.charCodeAt(0);
        header.writeUInt32BE(length, 1);
        f.backend.write(header);
        await until(() => f.backend.destroyed);
        if (afterCut) expect((await f.proxy.serverSettled).status).toBe('error');
        else await until(() => f.client.destroyed);
      });
    }
  }

  it('does not cut malformed Simple Query messages without a terminating NUL', async () => {
    const f = await setup();
    await handshake(f);
    f.client.write(message('Q', 'COMMITx'));
    await until(() => f.client.destroyed);
    expect(f.proxy.cuts).toHaveLength(0);
  });

  it('resumes buffered startup/query frames after an SSL refusal', async () => {
    const f = await setup();
    const ssl = Buffer.alloc(8);
    ssl.writeUInt32BE(8, 0);
    ssl.writeUInt32BE(80877103, 4);
    f.client.write(Buffer.concat([ssl, startup, query('COMMIT')]));
    await until(() => f.incoming().length === ssl.length);
    f.backend.write('N');
    await until(() => f.proxy.cuts.length === 1);
    f.backend.write(Buffer.concat([ready(), message('C', 'COMMIT\0'), ready()]));
    expect((await f.proxy.serverSettled).status).toBe('committed');
  });

  it('preserves backend frames coalesced with SSL refusal', async () => {
    const f = await setup();
    const ssl = Buffer.alloc(8);
    ssl.writeUInt32BE(8, 0);
    ssl.writeUInt32BE(80877103, 4);
    f.client.write(Buffer.concat([ssl, startup, query('COMMIT')]));
    await until(() => f.incoming().length === ssl.length);
    f.backend.write(Buffer.concat([Buffer.from('N'), ready()]));
    await until(() => f.proxy.cuts.length === 1);
    f.backend.write(Buffer.concat([message('C', 'COMMIT\0'), ready()]));
    await until(() => f.proxy.cuts[0].isSettled);
    expect((await f.proxy.serverSettled).status).toBe('committed');
  });

  it('fails closed on an invalid SSL negotiation reply without waiting for more data', async () => {
    const f = await setup();
    const ssl = Buffer.alloc(8);
    ssl.writeUInt32BE(8, 0);
    ssl.writeUInt32BE(80877103, 4);
    f.client.write(ssl);
    await until(() => f.incoming().length === ssl.length);
    f.backend.write('?');
    await until(() => f.client.destroyed);
    await until(() => f.backend.destroyed);
    expect(f.proxy.cuts).toHaveLength(0);
  });

  it('tracks a prior extended-protocol Sync response before COMMIT', async () => {
    const f = await setup();
    await handshake(f);
    f.client.write(Buffer.concat([message('S', ''), query('COMMIT')]));
    await until(() => f.proxy.cuts.length === 1);
    f.backend.write(Buffer.concat([ready(), message('C', 'COMMIT\0'), ready()]));
    expect((await f.proxy.serverSettled).status).toBe('committed');
  });

  it('does not settle committed before a complete idle ReadyForQuery', async () => {
    const f = await setup();
    await handshake(f);
    await cut(f);
    f.backend.write(Buffer.concat([message('C', 'COMMIT\0'), ready().subarray(0, 5)]));
    await delay(25);
    expect(f.proxy.cuts[0].isSettled).toBe(false);
    f.backend.write(ready().subarray(5));
    expect((await f.proxy.serverSettled).status).toBe('committed');
  });

  it('closes the listener at the beginning of the shutdown grace period', async () => {
    const f = await setup();
    await handshake(f);
    await cut(f);
    const port = f.proxy.port;
    const stopping = f.proxy.stop(100);
    const other = net.connect(port, '127.0.0.1');
    cleanup.push(async () => { other.destroy(); });
    const connected = await new Promise<boolean>(resolve => {
      other.once('connect', () => resolve(true));
      other.once('error', () => resolve(false));
    });
    await stopping;
    expect(connected).toBe(false);
    expect(f.proxy.cuts[0].isSettled).toBe(true);
  });

  it('does not admit a new cut from an existing connection during shutdown grace', async () => {
    const f = await setup();
    await handshake(f);
    const other = net.connect(f.proxy.port, '127.0.0.1');
    other.on('error', () => {});
    cleanup.push(async () => { other.destroy(); });
    await new Promise<void>(resolve => other.once('connect', resolve));
    // The first cut keeps shutdown in its drain phase.
    await cut(f);
    const stopping = f.proxy.stop(100);
    other.write(Buffer.concat([startup, query('COMMIT')]));
    await stopping;
    expect(f.proxy.cuts).toHaveLength(1);
  });

  it('rejects waiters promptly on stop', async () => {
    const f = await setup();
    const waiting = f.proxy.waitForCut(0);
    const result = waiting.catch(error => error as Error);
    await f.proxy.stop();
    expect(await Promise.race([result, delay(100).then(() => 'pending')])).toBeInstanceOf(Error);
  });

  it.each([
    [{ upstreamPort: 5432 }, 'mode'],
    [{ mode: 'after-forward', upstreamPort: 5432 }, 'mode'],
    [{ mode: 'after_forward', sslMode: 'passthrough', upstreamPort: 5432 }, 'sslMode'],
    [{ mode: 'after_forward', upstreamHost: 42, upstreamPort: 5432 }, 'upstreamHost'],
    [{ mode: 'after_forward', listenHost: 42, upstreamPort: 5432 }, 'listenHost'],
    [{ mode: 'after_forward', listenPort: 1.5, upstreamPort: 5432 }, 'listenPort'],
  ])('rejects invalid runtime configuration before listening: %j', async (options, field) => {
    await expect(CutProxy.start(options as never)).rejects.toThrow(field);
  });

  it('terminates the original first-cut promises when stopped before any cut', async () => {
    const f = await setup();
    const cutResult = f.proxy.commitCut.catch(error => error as Error);
    const settledResult = f.proxy.serverSettled;
    await f.proxy.stop(0);
    expect(await Promise.race([cutResult, delay(100).then(() => 'pending')])).toBeInstanceOf(Error);
    expect(await Promise.race([settledResult, delay(100).then(() => ({ status: 'pending' as const }))]))
      .toMatchObject({ status: 'error' });
  });
});
